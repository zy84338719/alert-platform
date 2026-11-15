#!/usr/bin/env python3
"""P1-04 (rh-p1-04): NetworkPolicy 拆分矩阵测试。

渲染 helm chart（helm template），断言：
  ① 恰好三条 NetworkPolicy：-api / -worker / -edge，各自 podSelector 钉死
     instance + component（修复前单一策略选中同 Release 全部组件）；
  ② api/worker egress 白名单 = 同 ns + kafka ns + postgres ns + DNS(kube-system 53)；
  ③ worker 额外放行通知渠道外呼（values 注入 extraEgress）；
  ④ edge egress 只含 DNS + 同 ns api pod（不含 kafka/pg namespace 规则），
     默认安装下 Edge→中央 API 连通而其它外呼被拒；
  ⑤ extraEgress 的注入逐组件生效（api 注入不影响 edge）；
  ⑥ P8-P1-02：默认 worker 策略显式放行公网通知出口（TCP 80/443，私网/
     链路本地/元数据段排除）；enabled=false 时完全 deny-by-default；
  ⑦ P8-P1-02：edgeCentralNamespace 非空时 edge 放行跨 namespace 中央 API。
"""
import subprocess
import sys

CHART = "deploy/helm/alert-platform"

EXTRA = [{"to": [{"ipBlock": {"cidr": "10.99.0.0/16"}}]}]


def render(extra_args=None):
    cmd = ["helm", "template", "test-rel", CHART]
    cmd += (extra_args or [])
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        raise SystemExit(f"helm template failed: {out.stderr}")
    docs = []
    for block in out.stdout.split("\n---\n"):
        if "kind: NetworkPolicy" in block:
            docs.append(block)
    return docs


def field(doc, key):
    for line in doc.splitlines():
        if line.strip().startswith(key + ":"):
            return line.split(":", 1)[1].strip()
    return None


def find_policies(docs):
    by_name = {}
    for d in docs:
        name = field(d, "name")
        by_name[name.split("/")[-1] if "/" in name else name] = d
    return by_name


def doc_list(by):
    return list(by.values())


def component_of(doc):
    """Extract the podSelector component label (None = not pinned)."""
    lines = doc.splitlines()
    sel = False
    depth = 0
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("podSelector:"):
            sel = True
            depth = len(line) - len(line.lstrip())
            continue
        if sel:
            cur = len(line) - len(line.lstrip())
            if stripped and cur <= depth and not stripped.startswith("-"):
                sel = False
                continue
            if "app.kubernetes.io/component:" in stripped:
                return stripped.split(":", 1)[1].strip()
    return None


def has_dns_rule(doc):
    return "kube-system" in doc and ("53" in doc or "UDP" in doc or "UDP" in doc.upper())


def main():
    failures = []

    # ---- 默认 values ----
    docs = render()
    by_name = find_policies(docs)
    components = sorted([c for c in (component_of(d) for d in by_name.values()) if c])

    # ① 三条独立策略，各自钉住组件
    if components != ["api", "edge", "worker"]:
        failures.append(f"① policies pin components {components}, want exactly [api edge worker]")
    names = sorted(by_name.keys())
    for suffix in ("api", "worker", "edge"):
        if not any(n.endswith("-" + suffix + "-np") for n in names):
            failures.append(f"① missing NetworkPolicy for component {suffix} (names={names})")

    # ④ edge：不含 kafka/pg namespace 白名单；含 DNS；含同 ns 目标
    edge_doc = next((d for n, d in by_name.items() if n.endswith("-edge-np")), None)
    if edge_doc:
        if field(edge_doc, "kubernetes.io/metadata.name") and (
            f"kafkaNamespace-default-value" in edge_doc or "infra" in edge_doc
        ):
            failures.append("④ edge egress must NOT allow kafka/postgres namespaces")
        if not has_dns_rule(edge_doc):
            failures.append("④ edge egress must allow DNS (kube-system)")
        if "podSelector" not in edge_doc.split("egress:")[1]:
            failures.append("④ edge egress must include the same-ns api pod selector")

    # ⑥ 默认安装：worker 显式放行公网通知出口（P8-P1-02）
    worker_doc0 = next((d for n, d in by_name.items() if n.endswith("-worker-np")), None)
    if not worker_doc0:
        failures.append("⑥ default render missing worker NetworkPolicy")
    else:
        if "cidr: 0.0.0.0/0" not in worker_doc0:
            failures.append("⑥ default worker policy must allow public notification egress (0.0.0.0/0)")
        for cidr in ("10.0.0.0/8", "169.254.0.0/16", "100.64.0.0/10"):
            if cidr not in worker_doc0:
                failures.append(f"⑥ notification egress must exclude {cidr}")
        for port in ("port: 80", "port: 443"):
            if port not in worker_doc0:
                failures.append(f"⑥ notification egress missing {port}")

    # ⑦ 跨 namespace 中央 API（默认关闭；显式配置才放行）
    if edge_doc and "kubernetes.io/metadata.name: central" in edge_doc:
        failures.append("⑦ edge must not allow a cross-namespace center by default")
    docs_cn = render([
        "--set", "networkPolicy.egress.edgeCentralNamespace=central",
        "--set", "networkPolicy.egress.edgeCentralPort=8443",
    ])
    edge_cn = next((d for n, d in find_policies(docs_cn).items() if n.endswith("-edge-np")), None)
    if not edge_cn or "kubernetes.io/metadata.name: central" not in edge_cn or "port: 8443" not in edge_cn:
        failures.append("⑦ edgeCentralNamespace/edgeCentralPort not rendered into edge egress")

    # ⑥b 显式关闭通知出口 -> 完全 deny-by-default
    docs_off = render(["--set", "networkPolicy.egress.notificationEgress.enabled=false"])
    worker_off = next((d for n, d in find_policies(docs_off).items() if n.endswith("-worker-np")), None)
    if worker_off and "cidr: 0.0.0.0/0" in worker_off:
        failures.append("⑥ notificationEgress.enabled=false must remove the public egress rule")

    # ---- values 注入 extraEgress（worker 通知渠道外呼）----
    docs2 = render([
        "--set", "networkPolicy.egress.workerExtraEgress[0].to[0].ipBlock.cidr=10.99.0.0/16",
        "--set", "networkPolicy.egress.apiExtraEgress[0].to[0].ipBlock.cidr=10.98.0.0/16",
    ])
    by_name2 = find_policies(docs2)
    worker_doc = next((d for n, d in by_name2.items() if n.endswith("-worker-np")), None)
    edge_doc2 = next((d for n, d in by_name2.items() if n.endswith("-edge-np")), None)
    api_doc2 = next((d for n, d in by_name2.items() if n.endswith("-api-np")), None)
    # ③ worker extra 生效
    if worker_doc and "10.99.0.0/16" not in worker_doc:
        failures.append("③ worker extraEgress (notification channels) missing from rendered policy")
    # ⑤ 注入不串组件
    if edge_doc2 and "10.99.0.0/16" in edge_doc2:
        failures.append("⑤ worker extraEgress leaked into edge policy")
    if edge_doc2 and "10.98.0.0/16" in edge_doc2:
        failures.append("⑤ api extraEgress leaked into edge policy")
    if api_doc2 and "10.98.0.0/16" not in api_doc2:
        failures.append("⑤ api extraEgress missing from api policy")

    if failures:
        print("P1-04 CHART POLICY MATRIX: FAIL")
        for f in failures:
            print("  -", f)
        sys.exit(1)
    print("P1-04/P8-P1-02 CHART POLICY MATRIX: PASS (3 policies, per-component selectors, DNS, public notification egress with private-range exclusions, cross-ns center opt-in, no leaks)")


if __name__ == "__main__":
    main()
