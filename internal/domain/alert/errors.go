// Package alert errors shared across layers: the transport (Kafka consumer)
// must distinguish CONTRACT violations (poison: quarantine into the DLQ) from
// infrastructure failures (retry/replay).
package alert

import "errors"

// ErrInvalidEvent marks a canonical event that violates the ingest contract
// (missing transport identity, tenant/source, signal, or firing severity).
// Retrying cannot fix it: the processor classifies it as Permanent so the
// consumer quarantines the message into the DLQ and commits the offset instead
// of crash-looping the critical processor task (R20).
var ErrInvalidEvent = errors.New("invalid canonical event")
