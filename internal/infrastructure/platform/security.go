package platform

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
)

// SecurityHeaders adds the baseline browser protections missing from the
// default stack (AUD-016). The console keeps its bearer token in
// sessionStorage, so clickjacking (XFO/frame-ancestors) and XSS amplification
// (CSP) are directly relevant.
//
// CSP is deliberately compatible with the embedded console: its only script
// and stylesheet are same-origin files (no inline handlers/styles), and all
// data calls go back to this origin.
func SecurityHeaders() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		c.Header("X-Frame-Options", "DENY")
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("Referrer-Policy", "no-referrer")
		c.Header("Content-Security-Policy",
			"default-src 'self'; script-src 'self'; style-src 'self'; "+
				"img-src 'self' data:; connect-src 'self'; font-src 'self'; "+
				"object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'")
		c.Header("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
		c.Next(ctx)
	}
}
