package channel

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/url"
)

func hmacSha256(msg, key string) string {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(msg))
	return url.QueryEscape(base64.StdEncoding.EncodeToString(mac.Sum(nil)))
}
