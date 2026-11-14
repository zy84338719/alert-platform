#!/usr/bin/env python3
"""Shadow-mode reconciliation (MVP-002): compare UPSTREAM notification records
against the platform's notification_outbox over a time window and emit an
explainable difference report.

Inputs (JSONL, one record per line):
  --upstream  records like {"fingerprint": "<upstream dedup key>",
                             "at": "RFC3339", "status": "firing|resolved",
                             "receiver": "...", "delivered": true}
  --platform  records like {"idempotency_key": "...", "created_at": "RFC3339",
                             "status": "sent|suppressed|failed|...",
                             "notification_type": "firing|resolved",
                             "receiver_id": "..."}   (notification_outbox export)

Both sides are matched by (alert identity bucket, notification type, time
bucket); identities that only exist on one side are reported as missing.
Exit code: 0 = report produced (differences are DATA, not tool failure).

Runbook: deploy/shadow/README.md (MVP-002 — real cluster run is BLOCKED until
the first production cluster is attached; this tool + runbook are the
deliverable).
"""
import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime

def load(path):
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out

def parse_ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--upstream", required=True)
    ap.add_argument("--platform", required=True)
    ap.add_argument("--window-seconds", type=int, default=120,
                    help="match window between an upstream and platform record")
    ap.add_argument("--out", default="-")
    args = ap.parse_args()

    upstream = load(args.upstream)
    platform = load(args.platform)

    def bucket_u(r):
        return (r["fingerprint"], r["status"])

    def bucket_p(r):
        return (r.get("alert_fingerprint", ""), r["notification_type"])

    # index platform records by (fingerprint, type) -> sorted times
    pidx = defaultdict(list)
    for r in platform:
        pidx[bucket_p(r)].append(parse_ts(r["created_at"]))
    for v in pidx.values():
        v.sort()

    import bisect
    matched, missing_on_platform = 0, []
    for r in upstream:
        times = pidx.get(bucket_u(r), [])
        t = parse_ts(r["at"])
        i = bisect.bisect_left(times, t)
        hit = any(abs((times[j] - t).total_seconds()) <= args.window_seconds
                  for j in (i - 1, i) if 0 <= j < len(times))
        if hit:
            matched += 1
        else:
            missing_on_platform.append(r)

    ukeys = {bucket_u(r) for r in upstream}
    extra_on_platform = []
    seen = set()
    for r in platform:
        k = bucket_p(r)
        if k not in ukeys and k not in seen:
            seen.add(k)
            extra_on_platform.append(r)

    report = {
        "upstream_records": len(upstream),
        "platform_records": len(platform),
        "matched_within_window": matched,
        "missing_on_platform": [
            {"fingerprint": r.get("fingerprint"), "status": r.get("status"), "at": r.get("at")}
            for r in missing_on_platform],
        "platform_only_buckets": [
            {"fingerprint": r.get("alert_fingerprint"), "notification_type": r.get("notification_type")}
            for r in extra_on_platform],
        "note": "missing_on_platform MUST be explainable (silences/pending retries) "
                "before any cutover decision (docs/10 MVP-002).",
    }
    text = json.dumps(report, indent=2, ensure_ascii=False)
    if args.out == "-":
        print(text)
    else:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(text + "\n")
    print(f"shadow compare: {matched}/{len(upstream)} upstream records matched; "
          f"{len(missing_on_platform)} missing on platform", file=sys.stderr)

if __name__ == "__main__":
    main()
