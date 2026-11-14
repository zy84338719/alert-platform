package edge

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// memWAL mirrors the real WAL contract after P0-02: Ack confirms ONE record;
// the watermark advances only across the longest contiguous confirmed prefix.
// Methods are mutex-guarded: Run() drains in a goroutine (TestUploaderRateCap).
type memWAL struct {
	mu    sync.Mutex
	recs  []Record
	ack   int64
	acked map[uint64]struct{}
}

func (m *memWAL) Pending() ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Record
	for _, r := range m.recs {
		if int64(r.Seq) > m.ack {
			if _, done := m.acked[r.Seq]; done {
				continue // confirmed above a hole: no retransmit
			}
			out = append(out, r)
		}
	}
	return out, nil
}
func (m *memWAL) Ack(seq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.acked == nil {
		m.acked = map[uint64]struct{}{}
	}
	if int64(seq) <= m.ack {
		return
	}
	m.acked[seq] = struct{}{}
	for {
		next := uint64(m.ack + 1)
		if _, ok := m.acked[next]; !ok {
			break
		}
		delete(m.acked, next)
		m.ack = int64(next)
	}
}
func (m *memWAL) Compact() error      { return nil }
func (m *memWAL) Stats() (int64, int) { return 0, len(m.recs) }

func batch(id string, seq uint64) Record {
	return Record{ID: id, Body: []byte(`{"batch_id":"` + id + `","events":[]}`), Seq: seq}
}

// EDGE-002: 202 acks and advances the watermark.
func TestUploaderAcksOn202(t *testing.T) {
	var accepted int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&accepted, 1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	w := &memWAL{recs: []Record{batch("b1", 0)}, ack: -1}
	u, err := NewUploader(w, UploaderConfig{Endpoint: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	sent, err := u.DrainOnce(context.Background())
	if err != nil || sent != 1 {
		t.Fatalf("sent=%d err=%v", sent, err)
	}
	if p, _ := w.Pending(); len(p) != 0 {
		t.Fatalf("pending after ack: %d", len(p))
	}
}

// 503 keeps the record pending; the retry posts the SAME bytes (stable
// transport id — the center dedups via receipts).
func TestUploaderRetriesOn503(t *testing.T) {
	var calls int32
	var lastBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 512)
		n, _ := r.Body.Read(buf)
		lastBody = buf[:n]
		if atomic.AddInt32(&calls, 1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	w := &memWAL{recs: []Record{batch("same-batch", 7)}, ack: -1}
	u, _ := NewUploader(w, UploaderConfig{Endpoint: srv.URL})
	if _, err := u.DrainOnce(context.Background()); err == nil {
		t.Fatal("first drain must fail on 503")
	}
	if p, _ := w.Pending(); len(p) != 1 {
		t.Fatal("record lost on 503")
	}
	sent, err := u.DrainOnce(context.Background())
	if err != nil || sent != 1 {
		t.Fatalf("retry: sent=%d err=%v", sent, err)
	}
	if string(lastBody) != string(w.recs[0].Body) {
		t.Fatal("retransmission mutated the batch body")
	}
}

// --- mTLS helpers: one private CA, server + client certs signed by it. ---

type testCA struct {
	cert *x509.Certificate
	key  *rsa.PrivateKey
	pem  []byte
}

func newCA(t *testing.T) *testCA {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "edge-test-ca"},
		IsCA: true, BasicConstraintsValid: true,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

type keyPair struct {
	certPEM, keyPEM []byte
	pair            tls.Certificate
}

func (ca *testCA) mint(t *testing.T, cn string, serial int64, client bool) keyPair {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	eku := x509.ExtKeyUsageServerAuth
	if client {
		eku = x509.ExtKeyUsageClientAuth
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{eku},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return keyPair{certPEM: certPEM, keyPEM: keyPEM, pair: pair}
}

func writeTmp(t *testing.T, name string, b []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// EDGE-002 mTLS: a real mutual-TLS handshake against a server that REQUIRES
// the client certificate, validated by the shared CA.
func TestUploaderMTLSHandshake(t *testing.T) {
	ca := newCA(t)
	server := ca.mint(t, "center", 11, false)
	client := ca.mint(t, "edge-1", 21, true)

	var sawClientCert bool
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawClientCert = len(r.TLS.PeerCertificates) > 0
		w.WriteHeader(http.StatusAccepted)
	}))
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{server.pair},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS12,
	}
	srv.StartTLS()
	defer srv.Close()

	w := &memWAL{recs: []Record{batch("mtls-batch", 1)}}
	u, err := NewUploader(w, UploaderConfig{
		Endpoint:       srv.URL,
		ClientCertFile: writeTmp(t, "client.crt", client.certPEM),
		ClientKeyFile:  writeTmp(t, "client.key", client.keyPEM),
		CAFile:         writeTmp(t, "ca.crt", ca.pem),
	})
	if err != nil {
		t.Fatal(err)
	}
	sent, err := u.DrainOnce(context.Background())
	if err != nil || sent != 1 {
		t.Fatalf("mTLS drain: sent=%d err=%v", sent, err)
	}
	if !sawClientCert {
		t.Fatal("server never saw the client certificate")
	}
}

// A foreign CA fails the handshake — mutual auth is verified, not decorative.
func TestUploaderMTLSWrongCAFails(t *testing.T) {
	other := newCA(t)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	w := &memWAL{recs: []Record{batch("x", 1)}}
	u, err := NewUploader(w, UploaderConfig{Endpoint: srv.URL, CAFile: writeTmp(t, "other-ca.crt", other.pem)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := u.DrainOnce(context.Background()); err == nil {
		t.Fatal("handshake with a foreign CA must fail")
	}
}

// EDGE-002 限速上限: the spacing between successful drains never beats the
// configured rate cap.
func TestUploaderRateCap(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	w := &memWAL{recs: []Record{batch("r1", 1), batch("r2", 2)}}
	u, err := NewUploader(w, UploaderConfig{Endpoint: srv.URL, Batch: 1, RatePerSec: 5})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	go u.Run(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p, _ := w.Pending(); len(p) == 0 {
			elapsed := time.Since(start)
			// two capped uploads must take at least one spacing interval
			// (1/5s = 200ms); unthrottled would be near-instant.
			if elapsed < 150*time.Millisecond {
				t.Fatalf("rate cap not enforced: 2 uploads in %s", elapsed)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("uploads never drained")
}
