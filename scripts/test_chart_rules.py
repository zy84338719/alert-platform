#!/usr/bin/env python3
"""P9-OBS-07: PrometheusRule 渲染矩阵。

断言：
  ① 默认 values 不渲染 PrometheusRule（可选能力，默认关闭）；
  ② enabled=true 时恰好一条规则，包含 5 条自身告警（IngestReject/Lag/DLQ/Notify/Backlog）；
  ③ prometheusRule.labels 注入 metadata.labels（适配各集群的 rule selector）；
  ④ extraRules 追加生效；
  ⑤ 规则表达式引用的每个 alert_platform_ 指标都出现在 docs/08 §3（文档-实现-告警三方一致）。
"""
import re
import subprocess
import sys

CHART = "deploy/helm/alert-platform"
DOC = "docs/08-安全运维与部署.md"

ALERTS = [
    "AlertPlatformIngestRejectRateHigh",
    "AlertPlatformConsumerLagHigh",
    "AlertPlatformDLQGrowing",
    "AlertPlatformNotifyFailures",
    "AlertPlatformNotifyBacklog",
]


def render(extra_args=None):
    cmd = ["helm", "template", "ap-dev", CHART, "--set", "prometheusRule.enabled=true"]
    cmd += (extra_args or [])
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        raise SystemExit(f"helm template failed: {out.stderr}")
    return out.stdout


def rule_docs(rendered):
    return [b for b in rendered.split("\n---\n") if "kind: PrometheusRule" in b]


def main():
    failures = []

    # ① 默认关闭
    default = subprocess.run(
        ["helm", "template", "ap-dev", CHART], capture_output=True, text=True
    )
    if default.returncode != 0:
        raise SystemExit(f"helm template (default) failed: {default.stderr}")
    if rule_docs(default.stdout):
        failures.append("① default values must NOT render PrometheusRule")

    # ② 五个告警
    rendered = render()
    docs = rule_docs(rendered)
    if len(docs) != 1:
        failures.append(f"② expected exactly 1 PrometheusRule, got {len(docs)}")
    else:
        doc = docs[0]
        for alert in ALERTS:
            if alert not in doc:
                failures.append(f"② missing alert {alert}")
        if "alert_platform_ingest_rejected_total" not in doc or "alert_platform_notify_pending" not in doc:
            failures.append("② rule expressions missing platform metrics")

        # ③ selector labels
        rendered_labels = render(["--set", "prometheusRule.labels.release=kube-prometheus-stack"])
        if "release: kube-prometheus-stack" not in rule_docs(rendered_labels)[0]:
            failures.append("③ prometheusRule.labels not rendered into metadata.labels")

        # ④ extraRules
        extra = render([
            "--set", "prometheusRule.extraRules[0].alert=CustomDiskRule",
            "--set", "prometheusRule.extraRules[0].expr=up%7Bjob%3D%22edge%22%7D%3D%3D0",
        ])
        if "CustomDiskRule" not in rule_docs(extra)[0]:
            failures.append("④ extraRules not appended")

    # ⑤ 文档一致性：expr 引用的 alert_platform_* 指标必须在 docs/08 出现
    with open(DOC, encoding="utf-8") as f:
        doc_text = f.read()
    referenced = set(re.findall(r"alert_platform_[a-z_]+", rendered))
    undocumented = sorted(m for m in referenced if m not in doc_text)
    if undocumented:
        failures.append(f"⑤ metrics referenced by rules but missing from {DOC}: {undocumented}")

    if failures:
        print("P9-OBS-07 CHART RULES MATRIX: FAIL")
        for f in failures:
            print("  -", f)
        sys.exit(1)
    print("P9-OBS-07 CHART RULES MATRIX: PASS (5 rules, selector labels, extraRules, docs-consistent)")


if __name__ == "__main__":
    main()