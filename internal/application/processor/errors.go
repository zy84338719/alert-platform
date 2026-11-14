package processor

import "errors"

// PermanentError marks an event that can NEVER succeed on retry (malformed
// payload, contract violation). The transport quarantines such messages into
// the DLQ and commits the offset — a poison message must not wedge the
// consumer group (docs/04 §1 alert.dlq.v1).
//
// Transient errors (dependency outage) are NOT wrapped: they fail the
// consumer so the offset stays uncommitted and at-least-once redelivery
// handles recovery.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return "permanent: " + e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent classifies an error as unretryable.
func Permanent(err error) error { return &PermanentError{Err: err} }

// IsPermanent reports whether err (or its chain) is a PermanentError.
func IsPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}
