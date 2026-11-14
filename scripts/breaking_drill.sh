#!/usr/bin/env bash
# Breaking-change drill (BASE-003 / docs/13): prove `buf breaking` blocks a
# backward-incompatible proto change and passes when reverted.
#
# A directory baseline is used instead of `.git#branch=` because the repo has
# no baseline commit yet; CI exercises the git form (contract-ci.yml).
#
# Exit 0 = drill passed (red blocked, then green). Any other exit = drill broken.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BASELINE=".dev/breaking-baseline"
VICTIM="proto/alert/event/v1/event.proto"
FIELD_LINE='  string episode_key = 17;'
DRILLED_LINE='  string drill_removed_field = 99;'

cleanup() {
  rm -rf "$BASELINE"
  rm -f "$VICTIM.bak" # portable `sed -i.bak` leaves this behind
}
trap cleanup EXIT

# 1) Snapshot the current (known-good) proto tree as the baseline.
rm -rf "$BASELINE"
mkdir -p "$BASELINE"
cp -r proto "$BASELINE/proto"
cp buf.yaml "$BASELINE/buf.yaml"

echo "[drill] baseline snapshot: $BASELINE"
echo "[drill] step 1/3: RED — remove field 'episode_key' from $VICTIM"
grep -qF "$FIELD_LINE" "$VICTIM" || { echo "[drill] FAIL: expected field line not found"; exit 1; }
sed -i.bak "s|$FIELD_LINE|$DRILLED_LINE|" "$VICTIM"

set +e
OUT="$(GOWORK=off go tool buf breaking --against "$BASELINE" 2>&1)"
RC=$?
set -e
if [ "$RC" -eq 0 ]; then
  echo "[drill] FAIL: breaking change was NOT blocked (exit 0)"
  echo "$OUT"
  # restore before exiting so the working tree is clean
  sed -i.bak "s|$DRILLED_LINE|$FIELD_LINE|" "$VICTIM"
  exit 1
fi
echo "[drill] blocked as expected (exit $RC):"
echo "$OUT" | head -5

echo "[drill] step 2/3: restore the field"
sed -i.bak "s|$DRILLED_LINE|$FIELD_LINE|" "$VICTIM"

echo "[drill] step 3/3: GREEN — same comparison after restore"
if ! GOWORK=off go tool buf breaking --against "$BASELINE"; then
  echo "[drill] FAIL: restored protos should pass breaking"
  exit 1
fi

echo "[drill] PASS: red blocked (exit $RC), green passes (exit 0)"
