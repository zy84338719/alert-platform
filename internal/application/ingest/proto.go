package ingest

import (
	"google.golang.org/protobuf/proto"

	eventv1 "github.com/example/alert-platform/gen/go/alert/event/v1"
)

// protoMarshal keeps the wire encoding decision (protobuf, docs/07 §2) in one
// place inside the application layer.
func protoMarshal(ev *eventv1.AlertEvent) ([]byte, error) {
	return proto.Marshal(ev)
}
