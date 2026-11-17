// Package ops defines the operational read/report ports (MC-002 / UI-003 /
// RETN-002): edge waterlines, delivery records and retention cleanups.
package ops

import (
	"context"
	"errors"
	"time"
)

// ErrSourceNotOwned rejects a waterline write whose (tenant, source) pair is
// not a registered source — the heartbeat must not trust body identity
// (AUD-009).
var ErrSourceNotOwned = errors.New("source not registered for tenant")

// EdgeStatus is one edge's reported waterline.
type EdgeStatus struct {
	SourceID   string
	WalBytes   int64
	WalPending int64
	UpdatedAt  time.Time
}

// NotificationTask is one delivery record row.
type NotificationTask struct {
	ID            string
	ChannelID     string
	Receiver      string
	Status        string
	Attempts      int
	Type          string
	CreatedAt     time.Time
	NextAttemptAt time.Time
	Message       string
}

// Store is the operational persistence port.
type Store interface {
	ReportEdgeStatus(ctx context.Context, tenantID, sourceID string, walBytes, walPending int64) error
	ListEdgeStatus(ctx context.Context, tenantID string) ([]EdgeStatus, error)
	ListNotificationTasks(ctx context.Context, tenantID, status string, limit int) ([]NotificationTask, error)
	// ReplayNotifyTask resets a dead-lettered (failed) task for one more
	// delivery attempt; tenant-scoped, audited by the handler (AUD-002).
	ReplayNotifyTask(ctx context.Context, tenantID, taskID string) (bool, error)
}

// RetentionStore adds the retention cleanups (RETN-002).
type RetentionStore interface {
	CleanupNotifications(ctx context.Context, olderThan time.Time) (int64, error)
	CleanupAudit(ctx context.Context, olderThan time.Time) (int64, error)
}
