// Package console embeds the dependency-free static console (NOTI-004) into
// the alert-api binary: no node toolchain, no external asset fetch. The Vue3
// console replaces it when the frontend team onboards — the API surface it
// consumes is already IDL-versioned.
package console

import (
	"context"
	"embed"
	"io/fs"
	"net/http"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
)

//go:embed index.html app.js style.css
var files embed.FS

// Register mounts the console at /console (unauthenticated: static shell
// only — every data call carries the platform bearer token).
func Register(h *server.Hertz) {
	sub, err := fs.Sub(files, ".")
	if err != nil {
		panic("console embed: " + err.Error())
	}
	h.GET("/console", func(ctx context.Context, c *app.RequestContext) {
		serve(sub, c, "index.html")
	})
	h.GET("/console/:file", func(ctx context.Context, c *app.RequestContext) {
		serve(sub, c, c.Param("file"))
	})
}

func serve(root fs.FS, c *app.RequestContext, name string) {
	// No path traversal: embed.FS has no parent, but keep the guard for
	// clarity — names come from the route parameter.
	if name == "" || name != sanitize(name) {
		c.String(http.StatusBadRequest, "bad file")
		return
	}
	data, err := fs.ReadFile(root, name)
	if err != nil {
		c.String(http.StatusNotFound, "not found")
		return
	}
	// The console is embedded in the binary: always serve the bytes that
	// match the running version, never a stale cached copy.
	c.Header("Cache-Control", "no-store")
	switch {
	case hasSuffix(name, ".html"):
		c.Data(http.StatusOK, "text/html; charset=utf-8", data)
	case hasSuffix(name, ".css"):
		c.Data(http.StatusOK, "text/css; charset=utf-8", data)
	case hasSuffix(name, ".js"):
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", data)
	default:
		c.Data(http.StatusOK, "application/octet-stream", data)
	}
}

func sanitize(name string) string {
	for _, r := range name {
		if r == '/' || r == '\\' {
			return ""
		}
	}
	return name
}

func hasSuffix(s, suf string) bool {
	return len(s) >= len(suf) && s[len(s)-len(suf):] == suf
}
