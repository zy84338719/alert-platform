// P9-MC-01: console metrics-center API. Two read-only endpoints backed by a
// server-side whitelist: the client names a catalog metric, the server picks
// the PromQL. Authentication/permissions reuse the platform bearer model.
package v1

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	mw "github.com/oekit/middleware"
	ekiterrors "github.com/oekit/pkg/errors"

	"github.com/example/alert-platform/internal/application/authn"
	"github.com/example/alert-platform/internal/application/metricsquery"
)

// MetricsQuery is bound by cmd wiring; nil returns 503 (fail closed, never a
// half-working proxy).
var MetricsQuery *metricsquery.Service

// SetMetricsQuery binds the metrics-center service.
func SetMetricsQuery(svc *metricsquery.Service) { MetricsQuery = svc }

// MetricsCatalog lists the whitelisted metrics.
//
// @router /metrics/v1/catalog [GET]
func MetricsCatalog(ctx context.Context, c *app.RequestContext) {
	if _, ok := requirePerm(ctx, c, authn.PermAlertsRead); !ok {
		return
	}
	if MetricsQuery == nil {
		mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(50306, 503, "metrics backend not configured"))
		return
	}
	c.JSON(200, map[string]any{"metrics": metricsquery.Catalog()})
}

// MetricsQueryRange proxies one whitelisted metric over a bounded window.
//
// @router /metrics/v1/query [GET]
func MetricsQueryRange(ctx context.Context, c *app.RequestContext) {
	if _, ok := requirePerm(ctx, c, authn.PermAlertsRead); !ok {
		return
	}
	if MetricsQuery == nil {
		mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(50306, 503, "metrics backend not configured"))
		return
	}
	name := c.Query("metric")
	start, err := parseMetricTime(c.Query("start"), time.Now().Add(-time.Hour))
	if err != nil {
		mw.Fail(ctx, c, ekiterrors.NewBadRequestError("invalid start: "+err.Error()))
		return
	}
	end, err := parseMetricTime(c.Query("end"), time.Now())
	if err != nil {
		mw.Fail(ctx, c, ekiterrors.NewBadRequestError("invalid end: "+err.Error()))
		return
	}
	step, err := parseMetricStep(c.Query("step"))
	if err != nil {
		mw.Fail(ctx, c, ekiterrors.NewBadRequestError("invalid step: "+err.Error()))
		return
	}
	res, err := MetricsQuery.Query(ctx, name, start, end, step)
	switch {
	case errors.Is(err, metricsquery.ErrUnknownMetric):
		mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(40402, 404, err.Error()))
		return
	case errors.Is(err, metricsquery.ErrInvalidRange):
		mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(40002, 400, err.Error()))
		return
	case err != nil:
		mw.Fail(ctx, c, ekiterrors.NewBizErrorWithStatus(50201, 502, "metrics backend error"))
		return
	}
	c.JSON(200, res)
}

// parseMetricTime accepts RFC3339 or unix seconds; empty uses the default.
func parseMetricTime(raw string, fallback time.Time) (time.Time, error) {
	if raw == "" {
		return fallback, nil
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t, nil
	}
	secs, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Time{}, errors.New("expected RFC3339 or unix seconds")
	}
	return time.Unix(secs, 0), nil
}

// parseMetricStep accepts "60s"/"1m" or plain seconds; empty uses 60s.
func parseMetricStep(raw string) (time.Duration, error) {
	if raw == "" {
		return time.Minute, nil
	}
	if secs, err := strconv.Atoi(raw); err == nil {
		return time.Duration(secs) * time.Second, nil
	}
	return time.ParseDuration(raw)
}
