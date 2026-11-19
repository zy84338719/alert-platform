#!/usr/bin/env python3
"""P10-008/P11: worker profile chart matrix.

断言：
  ① 默认不渲染独立 processor Deployment（单 Deployment 行为不变）；
  ② workerProcessor.enabled=true 时恰好一个 Deployment，WORKER_PROFILE=processor，
     副本数来自 workerProcessor.replicas；
  ③ 主 worker Deployment 的 profile 仍来自 worker.profile（默认 all），
     两个 Deployment 共用同一镜像。
"""
import subprocess
import sys

CHART = "deploy/helm/alert-platform"


def render(*args):
    cmd = ["helm", "template", "ap-dev", CHART, *args]
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        raise SystemExit(f"helm template failed: {out.stderr}")
    return out.stdout


def main():
    failures = []

    default = render()
    if "worker-processor" in default:
        failures.append("① default values must not render worker-processor")

    doc = render("--set", "workerProcessor.enabled=true",
                 "--set", "workerProcessor.replicas=3",
                 "--set", "worker.env.ALERT_KAFKA_BROKERS=kafka:9092")
    if doc.count("name: ap-dev-alert-platform-worker-processor") != 1:
        failures.append("② workerProcessor deployment not rendered exactly once")
    if 'value: "processor"' not in doc:
        failures.append("② processor profile env missing")
    if "replicas: 3" not in doc:
        failures.append("② replicas override not applied")
    if 'value: "all"' not in doc:
        failures.append("③ main worker profile default changed")
    # Both Deployments must use the same image.
    images = {line.split("image:")[1].strip().strip('"')
              for line in doc.splitlines() if line.strip().startswith("image:")}
    if len(images) != 1:
        failures.append(f"③ worker images diverged: {images}")

    if failures:
        print("P10-008 WORKER PROFILE CHART MATRIX: FAIL")
        for f in failures:
            print("  -", f)
        return 1
    print("P10-008 WORKER PROFILE CHART MATRIX: PASS (default off, processor profile, shared image)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
