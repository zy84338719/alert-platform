package v1

import (
	"net/http"
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/example/alert-platform/internal/application/authn"
	"github.com/example/alert-platform/internal/application/policymgmt"
)

// AUD-016: listing channels is a read operation (channels:read). Before the
// split it required channels:write, so auditor/oncall could not open the
// console's channel picker — despite both permissions existing in AUTH-002.
func TestListChannelsReadPermission(t *testing.T) {
	SetAuthn(authn.NewService(&fakeAuthnStore{byID: map[string]*authn.Identity{
		"tok-admin":   {TenantID: "tenant-a", ActorID: "alice", Role: authn.RolePlatformAdmin},
		"tok-oncall":  {TenantID: "tenant-a", ActorID: "bob", Role: authn.RoleOncall},
		"tok-auditor": {TenantID: "tenant-b", ActorID: "carol", Role: authn.RoleAuditor},
	}}))
	SetPolicySvc(policymgmt.NewService(&fakePolicyStore{}))
	t.Cleanup(func() { SetAuthn(nil); SetPolicySvc(nil) })

	h := server.Default(server.WithHostPorts(":0"))
	h.GET("/channels/v1", ListChannels)
	h.POST("/channels/v1", CreateChannel)

	for _, tok := range []string{"tok-admin", "tok-oncall", "tok-auditor"} {
		w := ut.PerformRequest(h.Engine, http.MethodGet, "/channels/v1", nil,
			ut.Header{Key: "Authorization", Value: "Bearer " + tok})
		if w.Code != http.StatusOK {
			t.Errorf("%s list channels: %d body=%s", tok, w.Code, w.Body.String())
		}
	}
	// Write still requires channels:write: oncall is rejected before body
	// validation reaches the service.
	body := `{}`
	w := ut.PerformRequest(h.Engine, http.MethodPost, "/channels/v1",
		&ut.Body{Body: strings.NewReader(body), Len: len(body)},
		ut.Header{Key: "Content-Type", Value: "application/json"},
		ut.Header{Key: "Authorization", Value: "Bearer tok-oncall"})
	if w.Code != http.StatusForbidden {
		t.Fatalf("oncall create channel: %d, want 403", w.Code)
	}
}
