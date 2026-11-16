// P9-CH-01: historical transition query with PG -> ClickHouse routing. The
// window picker lives in history.Service; this handler only authenticates,
// bounds the parameters and surfaces which backend answered.
package v1

import (
	"context"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
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
	rows, archived, err := HistorySvc.Query(ctx, identity.TenantID, from, to, limit)
	if err != nil {
		mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(50202, 502, "history backend error"))
		return
	}
	c.JSON(200, map[string]any{
		"transitions": rows,
		"archived":    archived, // true = answered by ClickHouse
		"from":        from.UTC().Format("2006-01-02T15:04:05Z07:00"),
		"to":          to.UTC().Format("2006-01-02T15:04:05Z07:00"),
		"limit":       limit,
	})
}
