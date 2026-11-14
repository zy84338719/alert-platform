#!/usr/bin/env bash
# ING-004 drill: prove the contract tests actually bite. Corrupt one golden
# fixture (drop a required field), expect `go test ./internal/adapter/translate`
# to fail; restore, expect green. Mirrors scripts/breaking_drill.sh.
set -euo pipefail
cd "$(dirname "$0")/.."

TARGET="contracts/external/deepflow/v7.2.3/fixtures/SYNTHETIC-bridge-event.json"
BACKUP=".dev/contract-drill-backup.json"
mkdir -p .dev

PKG="./internal/adapter/translate/"
run_tests() { GOWORK=off go test -count=1 "$PKG" >/dev/null 2>&1; }

restore() { [ -f "$BACKUP" ] && mv "$BACKUP" "$TARGET" || true; }
trap restore EXIT
cp "$TARGET" "$BACKUP"

# Corrupt: remove the required target_key (python keeps the JSON valid).
python3 - "$TARGET" <<'EOF'
import json, sys
p = sys.argv[1]
data = json.load(open(p))
data.pop("target_key", None)
json.dump(data, open(p, "w"), indent=2)
EOF

if run_tests; then
  echo "FAIL: corrupted fixture was ACCEPTED by the contract tests (they do not bite)"
  exit 1
fi
echo "OK: corrupted fixture rejected (red)"

restore
if ! run_tests; then
  echo "FAIL: tests stayed red after restore"
  exit 1
fi
echo "OK: contract tests green after restore"
