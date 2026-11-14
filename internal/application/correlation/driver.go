package correlation

import "context"

// ScanStore is the auto-merge persistence port.
type ScanStore interface {
	UnlinkedFiringAlerts(ctx context.Context, tenantID string, within interface{ String() string }) ([]Alert, error)
}

// Driver glues the pure rules to a tenant scan; the concrete store signature
// lives in infrastructure (time.Duration cannot appear here without pulling
// the port into infrastructure shape — see postgres.Store.CorrelateOnce).
type Driver struct {
	CorrelateOnce func(ctx context.Context) (int, error)
}

// NewDriver wraps a correlate function.
func NewDriver(fn func(ctx context.Context) (int, error)) *Driver { return &Driver{CorrelateOnce: fn} }
