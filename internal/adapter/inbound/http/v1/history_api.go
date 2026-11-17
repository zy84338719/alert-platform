// P9-CH-01: historical transition query with PG -> ClickHouse routing. The
// window picker lives in history.Service; this handler only authenticates,
// bounds the parameters and surfaces which backend answered.
package v1

import (
	"context"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/oekit/logx"
	mw "github.com/oekit/middleware"
	ekiterrors "github.com/oekit/pkg/errors"

	"github.com/example/alert-platform/internal/application/authn"
	"github.com/example/alert-platform/internal/application/history"
)

// HistorySvc is bound by cmd wiring; nil returns 503 (fail closed).
var HistorySvc *history.Service

// SetHistorySvc binds the history router (PG window + optional CH archive).
func SetHistorySvc(svc *history.Service) { HistorySvc = svc }

// ListHistoryTransitions queries archived transitions in [from, to).
//
// @router /history/v1/transitions [GET]
func ListHistoryTransitions(ctx context.Context, c *app.RequestContext) {
	identity, ok := requirePerm(ctx, c, authn.PermAlertsRead)
	if !ok {
		return
	}
	if HistorySvc == nil {
		mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(50307, 503, "history service not wired"))
		return
	}
	now := time.Now()
	from, err := parseMetricTime(c.Query("from"), now.Add(-24*time.Hour))
	if err != nil {
		mw.Fail(ctx, c, ekiterrors.NewBadRequestError("invalid from: "+err.Error()))
		return
	}
	to, err := parseMetricTime(c.Query("to"), now)
	if err != nil {
		mw.Fail(ctx, c, ekiterrors.NewBadRequestError("invalid to: "+err.Error()))
		return
	}
	if !to.After(from) {
		mw.Fail(ctx, c, ekiterrors.NewBadRequestError("to must be after from"))
		return
	}
	limit := 500
	if raw := c.Query("limit"); raw != "" {
		n, perr := strconv.Atoi(raw)
		if perr != nil || n <= 0 {
			mw.Fail(ctx, c, ekiterrors.NewBadRequestError("limit must be a positive integer"))
			return
		}
		if n > 1000 {
			n = 1000
		}
		limit = n
	}
	res, err := HistorySvc.Query(ctx, identity.TenantID, from, to, limit)
	if err != nil {
		// AUD-014: log the underlying error (snippet included by the client)
		// before collapsing to 502 — a silent generic error is uninvestigable.
		logx.CtxError(ctx, "history query failed", "component", "alert-api",
			"tenant", identity.TenantID, "from", from.UTC().Format(time.RFC3339),
			"to", to.UTC().Format(time.RFC3339), logx.ErrKey, err)
		mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(50202, 502, "history backend error"))
		return
	}
	var nextFrom any
	if res.NextFrom != nil {
		nextFrom = res.NextFrom.UTC().Format(time.RFC3339)
	}
	c.JSON(200, map[string]any{
		"transitions": res.Transitions,
		"archived":    res.Archived,  // true = ClickHouse answered (any part)
		"truncated":   res.Truncated, // more rows exist; continue with next_from as the new "to"
		"next_from":   nextFrom,
		"coverage":    res.Coverage, // full | pg-window-only (no archive configured)
		"from":        from.UTC().Format("2006-01-02T15:04:05Z07:00"),
		"to":          to.UTC().Format("2006-01-02T15:04:05Z07:00"),
		"limit":       limit,
	})
}
