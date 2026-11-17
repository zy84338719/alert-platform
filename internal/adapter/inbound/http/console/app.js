// alert-platform console (NOTI-004): a dependency-free static SPA served from
// the alert-api binary (go:embed). No build toolchain by design — the Vue3
// toolchain joins when the frontend team onboards (docs/13 R10 replan).
"use strict";

const state = { token: sessionStorage.getItem("ap_token") || "", who: null };

async function api(method, path, body) {
  const res = await fetch(path, {
    method,
    headers: {
      "Content-Type": "application/json",
      ...(state.token ? { Authorization: "Bearer " + state.token } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (res.status === 401) { setWho(null); throw new Error("未认证（401）：请登录"); }
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.message || res.status + " " + res.statusText);
  // Responses use the unified envelope {code, data, message} (mw.Fail/Success).
  return json.data !== undefined ? json.data : json;
}

// wire format note (docs/07): Timestamps travel as {seconds, nanos} objects.
function ts(o) { return o && o.seconds ? new Date(o.seconds * 1000).toLocaleString() : "—"; }

function setWho(who) {
  state.who = who;
  const el = document.getElementById("whoami");
  if (who) { el.textContent = `${who.actor_id} · ${who.role}`; sessionStorage.setItem("ap_token", state.token); }
  else { el.textContent = ""; sessionStorage.removeItem("ap_token"); }
}

async function whoami() {
  if (!state.token) return setWho(null);
  try {
    // probes: any authed read both proves the token and yields nothing sensitive
    await api("GET", "/alerts/v1?page_size=1");
    setWho({ actor_id: "已登录", role: "ok" });
  } catch { setWho(null); }
}

function esc(s) { return String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c])); }

// ---------------------------------------------------------------- views
const views = {};

views.home = {
  async render(main) {
    const [alerts, critical, incidents, notifFailed, edges, sources] = await Promise.all([
      api("GET", "/alerts/v1?page_size=1"),
      api("GET", "/alerts/v1?severity=SEVERITY_CRITICAL&page_size=1"),
      api("GET", "/incidents/v1?status=open"),
      api("GET", "/notifications/v1?status=failed"),
      api("GET", "/edge/v1/status"),
      api("GET", "/sources/v1"),
    ]);
    const degraded = (sources.sources || []).filter((x) => x.health_status !== "ok").length;
    const edgeRows = (edges.edges || []).map((e) =>
      "<div>" + esc(e.source_id.slice(0, 8)) + "… 缓冲 " + fmtBytes(e.wal_bytes) +
      " · 待确认 " + e.wal_pending + " 条 · " + esc(e.updated_at) + "</div>").join("");
    main.innerHTML =
      '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px">' +
      card("当前告警", alerts.total_estimate ?? 0, "活跃实例（pending/firing/stale）") +
      card("严重告警", critical.total_estimate ?? 0, "CRITICAL 等级") +
      card("故障 Incident", (incidents.incidents || []).length, "处置状态 open") +
      card("通知失败", (notifFailed.tasks || []).length, "failed（含退避重试中）") +
      card("来源健康", degraded === 0 ? "全部正常" : (degraded + " 异常"), "health_status 非 ok 的来源数") +
      '</div><div class="card mt"><b>Edge 缓冲水位</b>' +
      (edgeRows || '<span class="muted">无 edge 上报（Edge 接入后每分钟心跳）。</span>') +
      "</div>";
  },
};

function card(title, value, hint) {
  return '<div class="card"><b>' + esc(title) + '</b>' +
    '<div style="font-size:28px;margin:6px 0">' + esc(String(value)) + "</div>" +
    '<span class="muted">' + esc(hint) + "</span></div>";
}

function fmtBytes(n) {
  if (n > 1048576) return (n / 1048576).toFixed(1) + " MiB";
  if (n > 1024) return (n / 1024).toFixed(1) + " KiB";
  return n + " B";
}

views.system = {
  async render(main) {
    // SYS-001: read-only consolidated view. No management operations here —
    // writes go through the audited APIs above.
    const [edges, sources] = await Promise.all([
      api("GET", "/edge/v1/status"),
      api("GET", "/sources/v1?include_disabled=true"),
    ]);
    const rows = (sources.sources || []).map((x) => {
      return "<tr><td>" + esc(x.name) + "</td><td>" + esc(x.source_id.slice(0, 8)) + "…</td><td>" +
        esc(x.source_type.toLowerCase().replace("source_type_", "")) + "</td><td>" +
        (x.enabled ? "启用" : "停用") + "</td><td>" + esc(x.health_status) + "</td></tr>";
    }).join("");
    main.innerHTML =
      '<p class="muted">只读视图：租户/来源/edge 状态。管理操作（来源增删、令牌签发）走审计 API 与流程，本页无入口。</p>' +
      "<table><tr><th>来源</th><th>ID</th><th>类型</th><th>状态</th><th>健康</th></tr>" +
      (rows || '<tr><td colspan="5" class="muted">无。</td></tr>') + "</table>" +
      '<div class="card mt"><b>Edge</b><div>' +
      ((edges.edges || []).map((e) => esc(e.source_id.slice(0, 8)) + "… " + fmtBytes(e.wal_bytes)).join("<br>") || "无上报") +
      "</div></div>";
  },
};

views.notifications = {
  async render(main) {
    const st = window.nstatusFilter || "";
    const d = await api("GET", "/notifications/v1?limit=100" + (st ? "&status=" + st : ""));
    const rows = (d.tasks || []).map((t) => {
      const cls = t.status === "sent" ? "resolved" : (t.status === "failed" ? "firing" : "pending");
      return "<tr><td>" + esc(t.created_at) + "</td><td>" + esc(t.notification_type) +
        '</td><td><span class="badge ' + cls + '">' + esc(t.status) + "</span></td><td>" +
        t.attempts + "</td><td>" + esc(t.receiver_id) + "</td><td>" + esc(t.message) + "</td></tr>";
    }).join("");
    main.innerHTML =
      '<form class="inline" id="nf"><label>状态 <select name="status">' +
      '<option value="">全部</option><option>pending</option><option>sent</option>' +
      '<option>retry_scheduled</option><option>failed</option><option>suppressed</option>' +
      '</select></label><button class="primary">过滤</button></form>' +
      "<table><tr><th>时间</th><th>类型</th><th>状态</th><th>尝试</th><th>接收人</th><th>消息</th></tr>" +
      (rows || '<tr><td colspan="6" class="muted">无投递记录。</td></tr>') + "</table>";
    document.getElementById("nf").onsubmit = (e) => {
      e.preventDefault();
      const v = new FormData(e.target).get("status");
      window.nstatusFilter = v;
      location.hash = "#/notifications";
      views.notifications.render(main).catch(() => {});
    };
  },
};

views.edges = {
  async render(main) {
    const [edges, sources] = await Promise.all([
      api("GET", "/edge/v1/status"),
      api("GET", "/sources/v1"),
    ]);
    const edgeBySrc = {};
    (edges.edges || []).forEach((e) => { edgeBySrc[e.source_id] = e; });
    const rows = (sources.sources || []).map((x) => {
      const e = edgeBySrc[x.source_id];
      const health = x.health_status === "ok"
        ? '<span class="ok">ok</span>'
        : '<span class="badge firing">' + esc(x.health_status) + "</span>";
      const seen = x.last_seen_at ? new Date(x.last_seen_at.seconds * 1000).toLocaleString() : "—";
      return "<tr><td>" + esc(x.name) + "</td><td>" +
        esc(x.source_type.toLowerCase().replace("source_type_", "")) + "</td><td>" + health +
        "</td><td>" + seen + "</td><td>" + (e ? fmtBytes(e.wal_bytes) : "—") +
        "</td><td>" + (e ? e.wal_pending : "—") + "</td></tr>";
    }).join("");
    main.innerHTML = "<table><tr><th>来源</th><th>类型</th><th>健康</th><th>LastSeen</th><th>缓冲水位</th><th>待确认</th></tr>" +
      (rows || '<tr><td colspan="6" class="muted">无来源。</td></tr>') +
      '</table><p class="muted">重连次数与消费 Lag 以 /metrics（alert_platform_edge_*）为准——SRE 视图。</p>';
  },
};

views.alerts = {
  async render(main) {
    main.innerHTML = `
      <form class="inline" id="f">
        <label>状态 <select name="state"><option value="">活跃(pending/firing/stale)</option>
          <option>pending</option><option>firing</option><option>stale</option><option>resolved</option></select></label>
        <label>等级 <select name="severity"><option value=""></option>
          <option>SEVERITY_CRITICAL</option><option>SEVERITY_ERROR</option><option>SEVERITY_WARNING</option><option>SEVERITY_INFO</option></select></label>
        <label>来源 <input name="source_id" size="30" placeholder="source uuid"></label>
        <button class="primary">查询</button>
      </form>
      <div id="list"></div>`;
    const load = async (q) => {
      const params = new URLSearchParams(Object.entries(q).filter(([, v]) => v));
      const d = await api("GET", "/alerts/v1?" + params);
      document.getElementById("list").innerHTML = d.alerts && d.alerts.length ? `
        <table><tr><th>告警</th><th>状态</th><th>等级</th><th>集群/对象</th><th>最近观察</th><th></th></tr>
        ${d.alerts.map((a) => `<tr>
          <td>${esc(a.name)}<div class="muted">${esc(a.episode_key.slice(0, 8))}…</div></td>
          <td><span class="badge ${a.monitor_state}">${a.monitor_state}</span></td>
          <td>${esc(a.severity.replace("SEVERITY_", ""))}</td>
          <td>${esc(a.cluster_id || "")} / ${esc(a.entity_type || "")}:${esc(a.entity_uid || "")}</td>
          <td>${ts(a.last_observed_at)}</td>
          <td><a href="#/alerts/${esc(a.instance_id)}">详情</a></td></tr>`).join("")}
        </table>` : '<p class="muted">无匹配告警。</p>';
    };
    document.getElementById("f").onsubmit = (e) => {
      e.preventDefault();
      load(Object.fromEntries(new FormData(e.target)));
    };
    await load({});
  },
};

views.alertDetail = {
  async render(main, id) {
    const [a, tl] = await Promise.all([
      api("GET", "/alerts/v1/" + id),
      api("GET", `/alerts/v1/${id}/transitions`),
    ]);
    main.innerHTML = `
      <div class="card"><b>${esc(a.alert.name)}</b>
        <span class="badge ${a.alert.monitor_state}">${a.alert.monitor_state}</span>
        <table class="mt">
          <tr><th>等级</th><td>${esc(a.alert.severity)}</td><th>版本</th><td>${a.alert.version}</td></tr>
          <tr><th>开始</th><td>${ts(a.alert.starts_at)}</td><th>结束</th><td>${ts(a.alert.ends_at)}</td></tr>
          <tr><th>fingerprint</th><td colspan="3"><code>${esc(a.alert.fingerprint)}</code></td></tr>
          <tr><th>labels</th><td colspan="3"><pre>${esc(JSON.stringify(a.alert.labels || {}, null, 2))}</pre></td></tr>
        </table></div>
      <div class="card"><b>状态时间线（原始事件引用）</b>
        <table><tr><th>时间</th><th>from → to</th><th>事件</th></tr>
        ${(tl.transitions || []).map((t) => `<tr><td>${ts(t.transition_at)}</td>
          <td>${esc(t.from_state || "∅")} → <b>${esc(t.to_state)}</b></td>
          <td><code>${esc(t.event_id)}</code></td></tr>`).join("") || '<tr><td colspan="3" class="muted">无迁移</td></tr>'}
        </table></div>`;
  },
};

// IncidentStatus arrives as a protobuf NUMBER over the wire (docs/07:
// 数字枚举); map it defensively regardless of the wire form.
function incidentStatusName(v) {
  if (typeof v === "number") {
    return ["", "open", "acknowledged", "mitigated", "closed"][v] || String(v);
  }
  return String(v).toLowerCase().replace("incident_status_", "");
}

views.incidents = {
  async render(main) {
    const d = await api("GET", "/incidents/v1");
    main.innerHTML = `
      <table><tr><th>Incident</th><th>标题</th><th>处置状态</th><th>版本</th><th>操作</th></tr>
      ${(d.incidents || []).map((i) => `<tr>
        <td><code>${esc(i.incident_id.slice(0, 8))}…</code></td>
        <td>${esc(i.correlation_key.slice(0, 24))}…</td>
        <td><span class="badge ${incidentStatusName(i.status)}">${incidentStatusName(i.status)}</span></td>
        <td>v${i.version}</td>
        <td>${incidentStatusName(i.status) === "open" ? `<button onclick="ack('${esc(i.incident_id)}', ${i.version})">ACK</button>` : ""}</td>
      </tr>`).join("") || '<tr><td colspan="5" class="muted">无 Incident。</td></tr>'}
      </table><p class="muted">ACK 只改处置状态，监控状态不变（FR-008）。</p>`;
  },
};

window.ack = async function (incidentId, version) {
  const key = "ack-" + incidentId + "-" + Date.now();
  try {
    await api("POST", `/incidents/v1/${incidentId}/acknowledge`, {
      incident_id: incidentId, idempotency_key: key, expected_version: version,
    });
    route();
  } catch (e) { alert("ACK 失败：" + e.message + "（版本冲突会返回 409）"); }
};

views.incidentDetail = {
  async render(main, id) {
    const d = await api("GET", "/incidents/v1/" + id + "/detail");
    const links = (d.linked_alerts || []).map((la) => {
      const started = la.started_at ? new Date(la.started_at.seconds * 1000).toLocaleString() : "—";
      return "<tr><td>" + esc(la.name) + "</td><td><code>" + esc(la.relation_type) + "</code></td><td>" +
        esc((la.severity || "").replace("SEVERITY_", "")) + ' </td><td><span class="badge ' + la.monitor_state + '">' +
        esc(la.monitor_state) + "</span></td><td>" + started +
        ' </td><td><button class="unlink-btn" data-incident="' + esc(d.incident.incident_id) + '" data-instance="' + esc(la.instance_id) + '">解除</button></td></tr>';
    }).join("");
    const escRows = (d.escalations || []).map((e) => "<tr><td>stage " + e.stage + "</td><td>v" + e.policy_version +
      "</td><td>" + new Date(e.notified_at.seconds * 1000).toLocaleString() + "</td></tr>").join("");
    main.innerHTML =
      '<div class="card"><b>' + esc(d.incident.correlation_key.slice(0, 24)) + '…</b> ' +
      '<span class="badge ' + incidentStatusName(d.incident.status) + '">' +
      incidentStatusName(d.incident.status) + "</span> v" + d.incident.version +
      '<p class="muted">关联 ≠ 已证实根因（US-01）：auto_correlated/manual 是证据关联，不是根因结论。</p></div>' +
      '<div class="card"><b>关联告警</b><table><tr><th>告警</th><th>关联类型</th><th>等级</th><th>状态</th><th>开始</th><th></th></tr>' +
      (links || '<tr><td colspan="6" class="muted">无关联。</td></tr>') + "</table></div>" +
      '<div class="card"><b>升级记录</b><table><tr><th>阶段</th><th>策略版本</th><th>时间</th></tr>' +
      (escRows || '<tr><td colspan="3" class="muted">未升级（5 分钟未 ACK 触发）。</td></tr>') + "</table></div>";
    main.querySelectorAll(".unlink-btn").forEach((btn) => {
      btn.onclick = async () => {
        try {
          await api("POST", "/incidents/v1/" + btn.dataset.incident + "/unlink/" + btn.dataset.instance, {});
          location.hash = "#/incidents/" + btn.dataset.incident;
          route();
        } catch (e) { alert("解除失败：" + e.message); }
      };
    });
  },
};

window.unlink = async function (incidentId, instanceId) {
  try {
    await api("POST", "/incidents/v1/" + incidentId + "/unlink/" + instanceId, {});
    location.hash = "#/incidents/" + incidentId;
    route();
  } catch (e) { alert("解除失败：" + e.message); }
};

views.silences = {
  async render(main) {
    const d = await api("GET", "/policies/v1/silences?include_expired=true");
    main.innerHTML = `
      <div class="card"><b>新建静默</b>
        <form class="inline" id="ns">
          <label>matcher（name op value 空格分隔，多行 AND）
            <textarea name="matchers" rows="3" placeholder="ns EQ demo
severity EQ critical"></textarea></label>
          <label>时长(分钟)<input name="minutes" type="number" value="60" min="1"></label>
          <label>原因<input name="reason" size="24" placeholder="变更窗口"></label>
          <button class="primary">创建</button>
        </form></div>
      <table><tr><th>ID</th><th>matchers</th><th>窗口</th><th>状态</th><th></th></tr>
      ${(d.silences || []).map((s) => `<tr>
        <td><code>${esc(s.silence_id.slice(0, 8))}…</code></td>
        <td>${s.matchers.map((m) => `<code>${esc(m.name)} ${esc(m.operation)} ${esc(m.value)}</code>`).join("<br>")}</td>
        <td>${ts(s.starts_at)} → ${ts(s.ends_at)}</td>
        <td>${new Date(s.ends_at.seconds * 1000) > new Date() ? '<span class="ok">生效中</span>' : '<span class="muted">已过期</span>'}</td>
        <td>${new Date(s.ends_at.seconds * 1000) > new Date() ? `<button onclick="revokeSil('${esc(s.silence_id)}')">撤销</button>` : ""}</td>
      </tr>`).join("") || '<tr><td colspan="5" class="muted">无静默。</td></tr>'}
      </table>
      <div class="card mt"><b>命中模拟器</b>
        <form class="inline" id="pv">
          <label>labels（name=value，每行一个）<textarea name="labels" rows="3" placeholder="ns=demo"></textarea></label>
          <button>模拟</button>
        </form><div id="pvr"></div></div>`;
    document.getElementById("ns").onsubmit = async (e) => {
      e.preventDefault();
      const f = new FormData(e.target);
      const matchers = f.get("matchers").split("\n").map((l) => l.trim()).filter(Boolean)
        .map((l) => { const [name, operation, ...rest] = l.split(/\s+/); return { name, operation, value: rest.join(" ") }; });
      const now = Math.floor(Date.now() / 1000);
      await api("POST", "/policies/v1/silences", {
        matchers,
        starts_at: { seconds: now },
        ends_at: { seconds: now + Number(f.get("minutes")) * 60 },
        reason: f.get("reason") || "console",
      });
      route();
    };
    document.getElementById("pv").onsubmit = async (e) => {
      e.preventDefault();
      const labels = Object.fromEntries(new FormData(e.target).get("labels")
        .split("\n").map((l) => l.trim()).filter(Boolean).map((l) => l.split("=")));
      const d = await api("POST", "/policies/v1/silences/preview", { labels });
      document.getElementById("pvr").innerHTML = (d.matches || []).map((m) => `
        <div class="card"><code>${esc(m.silence_id.slice(0, 8))}…</code>
          ${m.silenced ? '<b class="badge firing">会静默</b>' : '<span class="badge resolved">不静默</span>'}
          <div class="mt">命中：${m.matched.map(esc).join("，") || "—"}<br>未命中：${m.unmatched.map(esc).join("，") || "—"}</div>
        </div>`).join("") || '<p class="muted">无静默可评估。</p>';
    };
  },
};

window.revokeSil = async function (id) {
  await api("POST", `/policies/v1/silences/${id}/revoke`);
  route();
};

views.routes = {
  async render(main) {
    const [r, ch] = await Promise.all([api("GET", "/policies/v1/routes"), api("GET", "/channels/v1")]);
    const channelOpts = (ch.channels || []).map((c) => `<option value="${esc(c.channel_id)}">${esc(c.name)} (${esc(c.type)})</option>`).join("");
    main.innerHTML = `
      <div class="card"><b>新建路由（版本化，创建即生效）</b>
        <form class="inline" id="nr">
          <label>matcher（name op value 每行一条）
            <textarea name="matchers" rows="3" placeholder="ns EQ prod"></textarea></label>
          <label>优先级<input name="priority" type="number" value="10"></label>
          <label>渠道<select name="channel">${channelOpts}</select></label>
          <label>恢复也通知<input type="checkbox" name="send_resolved" checked></label>
          <button class="primary">创建</button>
        </form></div>
      <table><tr><th>策略</th><th>优先级</th><th>matchers</th><th>版本</th><th></th></tr>
      ${(r.routes || []).map((p) => `<tr>
        <td><code>${esc(p.policy_id.slice(0, 8))}…</code></td><td>${p.priority}</td>
        <td>${p.matchers.map((m) => `<code>${esc(m.name)} ${esc(m.operation)} ${esc(m.value)}</code>`).join("<br>")}</td>
        <td>v${p.version}</td>
        <td><button onclick="toggleRoute('${esc(p.policy_id)}', ${p.version})">停用</button></td>
      </tr>`).join("") || '<tr><td colspan="5" class="muted">无路由（将回退默认渠道）。</td></tr>'}
      </table>`;
    document.getElementById("nr").onsubmit = async (e) => {
      e.preventDefault();
      const f = new FormData(e.target);
      const matchers = f.get("matchers").split("\n").map((l) => l.trim()).filter(Boolean)
        .map((l) => { const [name, operation, ...rest] = l.split(/\s+/); return { name, operation, value: rest.join(" ") }; });
      await api("POST", "/policies/v1/routes", {
        priority: Number(f.get("priority")),
        matchers,
        receiver_ids: [f.get("channel")],
        send_resolved: f.get("send_resolved") === "on",
      });
      route();
    };
  },
};

window.toggleRoute = async function (id, version) {
  try {
    await api("POST", `/policies/v1/routes/${id}/set-enabled`, { policy_id: id, enabled: false, expected_version: version });
  } catch (e) { alert("停用失败：" + e.message + "（expected_version 冲突 → 409）"); }
  route();
};

views.channels = {
  async render(main) {
    const d = await api("GET", "/channels/v1");
    main.innerHTML = `
      <div class="card"><b>新建渠道</b>
        <form class="inline" id="nc">
          <label>名称<input name="name" required></label>
          <label>类型<select name="type"><option>webhook</option><option>feishu</option><option>dingtalk</option></select></label>
          <label>Webhook URL<input name="webhook_url" size="40" placeholder="https://open.feishu.cn/..." required></label>
          <label>密钥(钉钉签名)<input name="secret" type="password"></label>
          <label>设为默认<input type="checkbox" name="is_default"></label>
          <button class="primary">创建</button>
        </form>
        <p class="muted">私网/环回/metadata 地址会被拒绝（T-013 SSRF 双重校验）。</p></div>
      <table><tr><th>名称</th><th>类型</th><th>端点</th><th>默认</th><th>状态</th><th></th></tr>
      ${(d.channels || []).map((c) => `<tr>
        <td>${esc(c.name)}</td><td>${esc(c.type)}</td>
        <td><code>${esc(c.webhook_url.replace(/^([a-z]+:\/\/[^/]+).*/, "$1/…"))}</code></td>
        <td>${c.is_default ? "✓" : ""}</td>
        <td>${c.enabled ? '<span class="ok">启用</span>' : '<span class="muted">停用</span>'}</td>
        <td><button onclick="toggleChannel('${esc(c.channel_id)}', ${!c.enabled})">${c.enabled ? "停用" : "启用"}</button></td>
      </tr>`).join("") || '<tr><td colspan="6" class="muted">无渠道。</td></tr>'}
      </table>`;
    document.getElementById("nc").onsubmit = async (e) => {
      e.preventDefault();
      const f = new FormData(e.target);
      try {
        await api("POST", "/channels/v1", Object.fromEntries(f));
        route();
      } catch (err) { alert("创建失败：" + err.message); }
    };
  },
};

window.toggleChannel = async function (id, enabled) {
  await api("POST", `/channels/v1/${id}/set-enabled`, { channel_id: id, enabled });
  route();
};

views.audit = {
  async render(main) {
    // audit list joins the query service surface (auditor role has this perm)
    const d = await api("GET", "/audit/v1");
    main.innerHTML = `<table><tr><th>时间</th><th>操作者</th><th>操作</th><th>对象</th><th>详情</th></tr>
      ${(d.entries || []).map((e) => `<tr>
        <td>${esc(e.created_at)}</td><td>${esc(e.actor_id)}</td>
        <td><code>${esc(e.operation)}</code></td>
        <td>${esc(e.object_type)}:<code>${esc(String(e.object_id).slice(0, 8))}…</code></td>
        <td><code>${esc(JSON.stringify(e.details))}</code></td></tr>`).join("")
      || '<tr><td colspan="5" class="muted">暂无审计记录。</td></tr>'}</table>`;
  },
};

views.metrics = {
  async render(main) {
    let catalog;
    try {
      catalog = await api("GET", "/metrics/v1/catalog");
    } catch (e) {
      main.innerHTML = '<p class="error">指标中心不可用：' + esc(e.message) +
        '</p><p class="muted">需要服务端配置 ALERT_PROMETHEUS_URL（Prometheus HTTP API）。</p>';
      return;
    }
    const metrics = catalog.metrics || [];
    if (!metrics.length) { main.innerHTML = '<p class="muted">指标目录为空。</p>'; return; }
    main.innerHTML = `
      <div class="card">
        <form class="inline" id="mq">
          <label>指标<select id="mmetric">${metrics.map((m) =>
            `<option value="${esc(m.name)}">${esc(m.name)} — ${esc(m.description)}</option>`).join("")}</select></label>
          <label>时间窗<select id="mrange">
            <option value="900">15 分钟</option>
            <option value="3600" selected>1 小时</option>
            <option value="21600">6 小时</option>
            <option value="86400">24 小时</option>
          </select></label>
          <label>步长(秒)<input id="mstep" type="number" value="60" min="15" max="3600" style="width:80px"></label>
          <button class="primary">查询</button>
        </form>
        <p class="muted">白名单指标由服务端选定 PromQL；客户端无法注入任意查询。</p>
      </div>
      <div id="mchart"><p class="muted">选择指标后点击查询。</p></div>`;
    const run = async () => {
      const metric = document.getElementById("mmetric").value;
      const range = parseInt(document.getElementById("mrange").value, 10);
      const step = parseInt(document.getElementById("mstep").value, 10) || 60;
      const end = Math.floor(Date.now() / 1000);
      const start = end - range;
      const box = document.getElementById("mchart");
      box.innerHTML = '<p class="muted">查询中…</p>';
      try {
        const res = await api("GET", `/metrics/v1/query?metric=${encodeURIComponent(metric)}&start=${start}&end=${end}&step=${step}`);
        drawMetricChart(box, res);
      } catch (e) {
        box.innerHTML = '<p class="error">查询失败：' + esc(e.message) + '</p>';
      }
    };
    document.getElementById("mq").onsubmit = (e) => { e.preventDefault(); run(); };
    run();
  },
};

function drawMetricChart(box, res) {
  const series = (res.series || []).slice(0, 5);
  if (!series.length || series.every((s) => !(s.points || []).length)) {
    box.innerHTML = '<p class="muted">该时间窗暂无数据。</p>';
    return;
  }
  const W = 860, H = 260, P = 40;
  // IDL shape (P10-007): points are {timestamp, value} objects.
  const pts = series.flatMap((s) => s.points || []);
  const minX = Math.min(...pts.map((p) => p.timestamp));
  const maxX = Math.max(...pts.map((p) => p.timestamp));
  const values = pts.map((p) => p.value);
  let maxY = Math.max(...values);
  let minY = Math.min(0, ...values);
  if (maxY === minY) maxY = minY + 1;
  const x = (v) => P + ((v - minX) / (maxX - minX || 1)) * (W - 2 * P);
  const y = (v) => H - P - ((v - minY) / (maxY - minY)) * (H - 2 * P);
  const colors = ["#4f8cff", "#ff6b6b", "#2ecc71", "#f1c40f", "#9b59b6"];
  const paths = series.map((s, i) => {
    const line = (s.points || []).map((p) => `${x(p.timestamp).toFixed(1)},${y(p.value).toFixed(1)}`).join(" ");
    return `<polyline fill="none" stroke="${colors[i % colors.length]}" stroke-width="1.6" points="${line}"/>`;
  }).join("");
  const legend = series.map((s, i) => {
    const labels = Object.keys(s.labels || {}).length
      ? Object.entries(s.labels).map(([k, v]) => k + "=" + v).join(", ")
      : "(all)";
    const last = (s.points || []).slice(-1)[0];
    return `<div><span style="display:inline-block;width:10px;height:10px;background:${colors[i % colors.length]};margin-right:6px"></span>` +
      esc(labels) + ' · 最新 <b>' + (last ? esc(String(last.value.toFixed(3))) : "—") + "</b></div>";
  }).join("");
  const yTop = esc(String(maxY.toFixed(3)));
  const yBottom = esc(String(minY.toFixed(3)));
  const tFrom = new Date(minX * 1000).toLocaleTimeString();
  const tTo = new Date(maxX * 1000).toLocaleTimeString();
  box.innerHTML = `<div class="card"><b>${esc(res.metric)}</b> <span class="muted">${esc(res.unit || "")}</span>
    <svg viewBox="0 0 ${W} ${H}" style="width:100%;height:auto">
      <line x1="${P}" y1="${H - P}" x2="${W - P}" y2="${H - P}" stroke="#444" stroke-width="1"/>
      <line x1="${P}" y1="${P}" x2="${P}" y2="${H - P}" stroke="#444" stroke-width="1"/>
      <text x="${P - 4}" y="${P + 10}" fill="#888" font-size="11" text-anchor="end">${yTop}</text>
      <text x="${P - 4}" y="${H - P}" fill="#888" font-size="11" text-anchor="end">${yBottom}</text>
      ${paths}
    </svg>
    <div class="muted">${esc(tFrom)} → ${esc(tTo)} · step ${esc(String(res.step_seconds))}s</div>
    <div class="mt">${legend}</div></div>`;
}

// ---------------------------------------------------------------- router
function route() {
  const hash = location.hash || "#/";
  const parts = hash.slice(2).split("/");
  let viewName = parts[0] || "home";
  if (viewName === "incidents" && parts[1]) viewName = "incidentDetail";
  if (viewName === "alerts" && parts[1]) viewName = "alertDetail";
  const view = views[viewName] || views.home;
  document.querySelectorAll("#nav a").forEach((a) =>
    a.classList.toggle("active", a.dataset.view === (parts[0] || "home")));
  const main = document.getElementById("view");
  if (!state.token) {
    main.innerHTML = '<p class="error">请先登录（右上角输入平台令牌）。</p>';
    return;
  }
  view.render(main, parts[1]).catch((e) => {
    main.innerHTML = `<p class="error">加载失败：${esc(e.message)}</p>`;
  });
}

document.getElementById("login").onsubmit = async (e) => {
  e.preventDefault();
  state.token = document.getElementById("token").value.trim();
  try {
    await api("GET", "/alerts/v1?page_size=1");
    setWho({ actor_id: "已登录", role: "ok" });
    route();
  } catch (err) {
    setWho(null);
    alert("登录失败：" + err.message);
  }
};

window.addEventListener("hashchange", route);
whoami().then(route);
