// deps.go holds the wiring point between cmd/ and the generated handlers.
// hz update never touches this file.
package v1

import "github.com/example/alert-platform/internal/application/ingest"

// SetIngest binds the ingest application service used by PushBatch.
func SetIngest(s *ingest.Service) { Ingest = s }
