"use strict";
/* Go SDK Demo 前端：与后端 WebSocket 桥通讯，所有数据来自真实 SDK。 */

// ---------------------------------------------------------------------------
// 状态

const S = {
  me: null,
  selfSession: "",
  convs: [],
  friends: [],
  requests: { received: [], sent: [], unread: 0 },
  groups: [],
  invitations: [],
  blacklist: [],
  presence: {},
  tab: "chat",
  openConv: null,
  convMeta: {},        // convId → {friend, group}
  messages: {},        // convId → Message[]
  typing: {},          // convId → {username, ts}
  conn: "disconnected",
  booted: false,       // 是否已收到过 bootstrap（区分「未登录」与「已登录后瞬断」）
  replyTo: null,
  kicked: false,
  detail: null,        // 通讯录/群组右侧详情 {kind, data}
  members: {},         // groupId → GroupMember[]
};
const $ = (sel, el = document) => el.querySelector(sel);
const $$ = (sel, el = document) => [...el.querySelectorAll(sel)];

// ---------------------------------------------------------------------------
// 工具

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[m]));
}
const AV_COLORS = ["#4f7ded", "#07c160", "#fa9d3b", "#f76260", "#8f6bf5", "#00b8a9", "#5b8cff", "#e0a03c"];
function avColor(name) {
  let h = 2166136261;
  for (const ch of String(name)) { h ^= ch.charCodeAt(0); h = Math.imul(h, 16777619) >>> 0; }
  return AV_COLORS[h % AV_COLORS.length];
}
function avHTML(name, cls = "", showPresence = false) {
  const initial = String(name || "?").slice(0, 1).toUpperCase();
  const dot = showPresence
    ? `<span class="presence-dot ${S.presence[name] === "online" ? "online" : ""}"></span>` : "";
  return `<div class="avatar ${cls}" style="--c:${avColor(name)}">${esc(initial)}${dot}</div>`;
}
function displayNameOf(username) {
  if (username === (S.me && S.me.username)) return (S.me.nickname || username);
  const f = S.friends.find((x) => x.username === username);
  return (f && (f.remark || f.nickname)) || username;
}
function fmtSize(n) {
  if (n >= 1 << 20) return (n / (1 << 20)).toFixed(1) + " MB";
  if (n >= 1024) return (n / 1024).toFixed(1) + " KB";
  return n + " B";
}
function fmtListTime(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (isNaN(d)) return "";
  const now = new Date();
  const sameDay = d.toDateString() === now.toDateString();
  const hh = String(d.getHours()).padStart(2, "0"), mm = String(d.getMinutes()).padStart(2, "0");
  if (sameDay) return `${hh}:${mm}`;
  const yest = new Date(now); yest.setDate(now.getDate() - 1);
  if (d.toDateString() === yest.toDateString()) return "昨天";
  if (d.getFullYear() === now.getFullYear()) return `${d.getMonth() + 1}月${d.getDate()}日`;
  return `${d.getFullYear()}/${d.getMonth() + 1}/${d.getDate()}`;
}
function fmtChatTime(iso) {
  const d = new Date(iso);
  if (isNaN(d)) return "";
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getMonth() + 1}月${d.getDate()}日 ${p(d.getHours())}:${p(d.getMinutes())}`;
}
function b64url(s) {
  const bytes = new TextEncoder().encode(s);
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
function mediaSrc(u, ct) {
  if (!u) return "";
  return `/api/media?u=${encodeURIComponent(b64url(u))}&ct=${encodeURIComponent(ct || "")}`;
}
function toast(msg, isErr = false) {
  const el = $("#toast");
  el.textContent = msg;
  el.classList.toggle("err", isErr);
  el.classList.remove("hidden");
  clearTimeout(toast._t);
  toast._t = setTimeout(() => el.classList.add("hidden"), 2400);
}

// ---------------------------------------------------------------------------
// WebSocket 桥

let ws = null, reqSeq = 1;
const pendingReplies = new Map();
function bridge(action, params = {}) {
  return new Promise((resolve, reject) => {
    if (!ws || ws.readyState !== 1) return reject(new Error("连接未就绪"));
    const id = reqSeq++;
    pendingReplies.set(id, { resolve, reject });
    ws.send(JSON.stringify({ id, action, params }));
    setTimeout(() => {
      if (pendingReplies.has(id)) {
        pendingReplies.delete(id);
        reject(new Error(`「${action}」请求超时`));
      }
    }, 15000);
  });
}
function connectBridge() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.onmessage = (ev) => {
    let f; try { f = JSON.parse(ev.data); } catch { return; }
    if (f.type === "reply") {
      const p = pendingReplies.get(f.id);
      if (!p) return;
      pendingReplies.delete(f.id);
      if (f.ok) p.resolve(f.data); else p.reject(new Error(f.error || "请求失败"));
    } else if (f.type === "push") {
      handlePush(f.event, f.data);
    }
  };
  ws.onclose = () => {
    if (S.kicked) return;
    if (!S.booted) {
      // 尚未登录（或会话已失效）：显示登录页，等用户登录成功后再 connectBridge
      showLoginView();
      return;
    }
    setConn("disconnected");
    setTimeout(connectBridge, 2000);
  };
}
function setConn(state) {
  S.conn = state;
  const dot = $("#conn-dot");
  dot.classList.remove("on", "off", "mid");
  dot.classList.add(state === "connected" ? "on" : state === "connecting" ? "mid" : "off");
  if (S.tab === "me") renderMain();
}

// typing 兜底清扫：typing case 里的 setTimeout 在后台标签页可能被浏览器节流甚至长期推迟，
// 导致「正在输入…」残留。这里每秒惰性检查过期项，保证任何场景下最多滞后 1 秒消失。
// 双保险：除「5 秒未活动」外，同一来源累计显示超过 15 秒也强制过期——防止后台标签页
// 恢复时事件重放反复刷新 ts 使指示器永不过期。
function sweepTyping(force) {
  const now = Date.now();
  let dirty = false;
  for (const cid in S.typing) {
    const t = S.typing[cid];
    if (now - t.ts >= 5000 || now - (t.firstSeen || t.ts) >= 15000) { delete S.typing[cid]; dirty = true; }
  }
  if (dirty || force) { renderConvList(); renderChatSub(); }
}
setInterval(() => sweepTyping(false), 1000);
// 后台标签页的定时器被浏览器强节流，切回前台时立即清扫一次，消除节流期间积累的残留画面
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") sweepTyping(true);
});

// ---------------------------------------------------------------------------
// 服务端推送

function handlePush(event, data) {
  switch (event) {
    case "bootstrap": {
      S.booted = true;
      $("#view-login").classList.add("hidden");
      $("#view-app").classList.remove("hidden");
      S.me = data.me; S.selfSession = data.self_session || "";
      S.convs = data.conversations || [];
      S.friends = data.friends || [];
      S.requests = data.requests || { received: [], sent: [], unread: 0 };
      S.groups = data.groups || [];
      S.invitations = data.invitations || [];
      S.blacklist = data.blacklist || [];
      S.presence = {};
      for (const p of data.presence || []) S.presence[p.username] = p.state;
      renderAll();
      break;
    }
    case "convs": {
      S.convs = data || [];
      renderConvList(); updateBadges();
      // convs 推送可能晚于 msgs 到达（unread 刚变 1），补一次已读上报，否则对方永远看不到"已读"。
      if (S.openConv && !String(S.openConv).startsWith("peer:")) markRead();
      break;
    }
    case "msgs": {
      const cid = data.conversation_id;
      const arr = S.messages[cid] || (S.messages[cid] = []);
      for (const m of data.messages || []) upsertMessage(arr, m);
      // 对端消息已到达即视为对方停止输入：立即清掉 typing 指示器，不等现象窗口过期
      const msgsArr = data.messages || [];
      const lastMsg = msgsArr.length ? msgsArr[msgsArr.length - 1] : null;
      if (lastMsg && lastMsg.sender !== (S.me && S.me.username) && S.typing[cid]) {
        delete S.typing[cid];
        renderConvList();
        if (cid === S.openConv) renderChatSub();
      }
      if (cid === S.openConv) { renderMessages(); markRead(); }
      break;
    }
    case "msgs_deleted": {
      const arr = S.messages[data.conversation_id];
      if (arr) {
        S.messages[data.conversation_id] = arr.filter((m) => !data.seqs.includes(m.seq));
        if (data.conversation_id === S.openConv) renderMessages();
      }
      break;
    }
    case "msgs_changed": {
      // message.changed 补丁：按 change.kind 把变化的字段合并进对应 seq 的消息。
      const arr = S.messages[data.conversation_id];
      const m = arr && arr.find((x) => x.seq === data.seq);
      if (m) {
        const kind = data.change && data.change.kind;
        if (data.recalled) m.recalled = data.recalled;
        if (data.edited) m.edited = data.edited;
        if (data.pinned !== undefined) m.pinned = data.pinned;
        if (data.reactions) m.reactions = data.reactions;
        if (data.erased) m.erased = true;
        if (data.body !== undefined && kind === "edited") { m.body = data.body; m.type = "text"; }
        m.message_version = data.message_version || m.message_version;
        if (data.conversation_id === S.openConv) renderMessages();
      }
      break;
    }
    case "typing": {
      const now = Date.now();
      // firstSeen 记录本轮输入的起点：连续输入只续 ts 不续 firstSeen，使 15 秒硬上限可生效
      const prevTyping = S.typing[data.conversation_id];
      const firstSeen = prevTyping && now - prevTyping.firstSeen < 15000 ? prevTyping.firstSeen : now;
      S.typing[data.conversation_id] = { username: data.username, ts: now, firstSeen };
      renderConvList();
      if (data.conversation_id === S.openConv) renderChatSub();
      setTimeout(() => {
        const t = S.typing[data.conversation_id];
        if (t && Date.now() - t.ts >= 4900) {
          delete S.typing[data.conversation_id];
          renderConvList();
          if (data.conversation_id === S.openConv) renderChatSub();
        }
      }, 5000);
      break;
    }
    case "peer_read": {
      const c = S.convs.find((x) => x.id === data.conversation_id);
      if (c) { c.peer_read_seq = data.read_seq; renderMessages(); }
      break;
    }
    case "friends": S.friends = data || []; renderListPane(); break;
    case "requests": S.requests = data || { received: [], sent: [], unread: 0 }; renderListPane(); updateBadges(); break;
    case "groups": S.groups = data || []; renderListPane(); break;
    case "invitations": S.invitations = data || []; renderListPane(); updateBadges(); break;
    case "blacklist": S.blacklist = data || []; if (S.tab === "contacts") renderListPane(); break;
    case "presence":
      for (const p of data || []) S.presence[p.username] = p.state;
      renderListPane(); break;
    case "profile": S.me = data; renderMain(); break;
    case "conn": setConn(data.state); break;
    case "kicked": showKicked(data.reason); break;
  }
}
function upsertMessage(arr, m) {
  const i = arr.findIndex((x) => x.seq === m.seq);
  if (i >= 0) arr[i] = m; else {
    arr.push(m);
    arr.sort((a, b) => a.seq - b.seq);
  }
}

// ---------------------------------------------------------------------------
// 登录 / 会话失效

async function doLogin() {
  const btn = $("#login-btn");
  const errEl = $("#login-error");
  errEl.classList.add("hidden");
  btn.disabled = true; btn.textContent = "登录中…";
  try {
    const resp = await fetch("/api/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        username: $("#login-username").value.trim(),
        password: $("#login-password").value,
      }),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || "登录失败");
    S.me = data.me;
    $("#view-login").classList.add("hidden");
    $("#view-app").classList.remove("hidden");
    renderListPane();
    connectBridge();
  } catch (err) {
    errEl.textContent = err.message;
    errEl.classList.remove("hidden");
  } finally {
    btn.disabled = false; btn.textContent = "登 录";
  }
}
function showLoginView() {
  $("#view-login").classList.remove("hidden");
  $("#view-app").classList.add("hidden");
  setConn("disconnected");
}
function showKicked(reason) {
  S.kicked = true;
  $("#kicked-reason").textContent = reason || "登录状态已失效";
  $("#kicked").classList.remove("hidden");
}

// ---------------------------------------------------------------------------
// 渲染：框架

function renderAll() {
  renderListPane();
  renderMain();
  updateBadges();
}
function updateBadges() {
  const unread = S.convs.reduce((n, c) => n + (c.unread > 0 ? c.unread : c.marked_unread ? 1 : 0), 0);
  setBadge("#badge-chat", unread);
  setBadge("#badge-contacts", S.requests.unread || 0);
  setBadge("#badge-groups", S.invitations.length);
}
function setBadge(sel, n) {
  const el = $(sel);
  el.textContent = n > 99 ? "99+" : n > 0 ? n : "";
  el.classList.toggle("hidden", !(n > 0));
}
function renderListPane() {
  const el = $("#pane-list");
  if (S.tab === "chat") el.innerHTML = chatListHTML();
  else if (S.tab === "contacts") el.innerHTML = contactsHTML();
  else if (S.tab === "groups") el.innerHTML = groupsHTML();
  else el.innerHTML = meHTML();
  $$(".nav-btn").forEach((b) => b.classList.toggle("active", b.dataset.tab === S.tab));
}
// 会话数据（列表/输入中状态）变化后的重渲入口：当前 tab 若是聊天页即重绘会话列表。
function renderConvList() { renderListPane(); }
function renderMain() {
  const el = $("#pane-main");
  if (S.tab === "me") { el.innerHTML = meDetailHTML(); return; }
  if (S.tab === "contacts" && S.detail && S.detail.kind === "friend") { el.innerHTML = friendDetailHTML(S.detail.data); return; }
  if (S.tab === "groups" && S.detail && S.detail.kind === "group") { renderGroupDetail(); return; }
  if (S.openConv) { renderChat(); return; }
  el.innerHTML = `<div class="main-empty">${esc(S.tab === "contacts" ? "选择一位好友查看资料" : S.tab === "groups" ? "选择一个群查看资料" : "选择一个会话开始聊天")}</div>`;
}

// ---------------------------------------------------------------------------
// 渲染：会话列表

function chatListHTML() {
  const rows = S.convs.map((c) => {
    const t = S.typing[c.id];
    const typing = t && Date.now() - t.ts < 5000 && Date.now() - (t.firstSeen || t.ts) < 15000 && t.username !== (S.me && S.me.username);
    let preview = c.last_preview || " ";
    if (typing) preview = `${esc(displayNameOf(t.username))} 正在输入…`;
    else if (c.last_sender === "self" && c.last_seq > 0) preview = "我: " + preview;
    const isGroup = c.type === "group";
    return `
    <div class="row conv-row ${c.id === S.openConv ? "active" : ""} ${c.pinned ? "pinned" : ""}" data-act="conv-open" data-id="${esc(c.id)}"
         style="${c.pinned ? "background:#f2f2f2" : ""}">
      ${isGroup ? avHTML(c.name || c.group_id, "", false) : avHTML(displayNameOf(c.peer), "", true)}
      <div class="row-main">
        <div class="row-top">
          <span class="row-title">${esc(c.name || c.peer)}</span>
          <span class="row-time">${esc(fmtListTime(c.last_at))}</span>
        </div>
        <div class="row-sub">
          <span class="preview ${typing ? "typing" : ""}">${esc(preview)}</span>
          ${c.unread > 0 ? `<span class="badge">${c.unread > 99 ? "99+" : c.unread}</span>` :
            c.marked_unread ? `<span class="badge" style="background:#c8c8c8;min-width:10px;padding:0"></span>` : ""}
        </div>
      </div>
      <button class="row-more" data-act="conv-menu" data-id="${esc(c.id)}">⋯</button>
    </div>`;
  });
  return `
    <div class="list-head"><h2>聊天</h2></div>
    <div class="list-scroll">
      ${rows.length ? rows.join("") : `<div class="empty-tip">暂无会话<br><br>去「通讯录」添加好友，或「群组」创建群聊</div>`}
    </div>`;
}

// ---------------------------------------------------------------------------
// 渲染：通讯录

function contactsHTML() {
  const reqs = (S.requests.received || []).filter((r) => r.status === "pending");
  const reqRows = reqs.map((r) => `
    <div class="row disabled">
      ${avHTML(r.nickname || r.username)}
      <div class="row-main">
        <div class="row-top"><span class="row-title">${esc(r.nickname || r.username)}</span><span class="row-time">${esc(fmtListTime(r.requested_at))}</span></div>
        <div class="row-sub"><span class="preview">${esc(r.message || "请求添加你为好友")}</span></div>
      </div>
      <div class="row-btns">
        <button class="btn-plain" data-act="req-accept" data-username="${esc(r.username)}">接受</button>
        <button class="btn-plain" data-act="req-decline" data-username="${esc(r.username)}">拒绝</button>
      </div>
    </div>`).join("");
  const friendRows = S.friends.map((f) => `
    <div class="row friend-row ${S.detail && S.detail.kind === "friend" && S.detail.data.username === f.username ? "active" : ""}"
         data-act="friend-open" data-username="${esc(f.username)}">
      ${avHTML(f.remark || f.nickname || f.username, "", true)}
      <div class="row-main">
        <div class="row-top"><span class="row-title">${esc(f.remark || f.nickname || f.username)}</span></div>
        <div class="row-sub"><span class="preview">微信号：${esc(f.username)}</span></div>
      </div>
    </div>`).join("");
  const blRows = S.blacklist.map((u) => `
    <div class="row disabled">
      ${avHTML(u)}
      <div class="row-main"><div class="row-top"><span class="row-title">${esc(displayNameOf(u))}</span></div></div>
      <button class="btn-plain" data-act="bl-remove" data-username="${esc(u)}">移出黑名单</button>
    </div>`).join("");
  return `
    <div class="list-head"><h2>通讯录</h2></div>
    <div class="search-bar">
      <input id="lookup-input" placeholder="输入用户名查找并添加">
      <button class="btn-plain" data-act="lookup-btn">查找</button>
    </div>
    <div id="lookup-result"></div>
    <div class="list-scroll">
      ${reqRows ? `<div class="list-section">新的朋友（${reqs.length}）</div>${reqRows}` : ""}
      <div class="list-section">好友（${S.friends.length}）</div>
      ${friendRows || `<div class="empty-tip">还没有好友</div>`}
      ${blRows ? `<div class="list-section">黑名单（${S.blacklist.length}）</div>${blRows}` : ""}
    </div>`;
}

function friendDetailHTML(f) {
  const shown = f.remark || f.nickname || f.username;
  return `
  <div class="detail">
    <div class="chat-head" style="margin:-28px -24px 20px">
      <button class="chat-back" data-act="back"><svg viewBox="0 0 24 24"><path d="M15.5 4.5 8 12l7.5 7.5-1.8 1.8L4.4 12l9.3-9.3z"/></svg></button>
      <div class="chat-title">用户资料</div>
    </div>
    <div class="detail-head">
      ${avHTML(shown, "lg")}
      <div class="info">
        <div class="detail-name">${esc(shown)}</div>
        <div class="detail-sub">微信号：${esc(f.username)}</div>
      </div>
    </div>
    ${f.nickname && f.remark ? `<div class="detail-field"><span class="k">昵称</span><span>${esc(f.nickname)}</span></div>` : ""}
    ${f.remark ? `<div class="detail-field"><span class="k">备注</span><span>${esc(f.remark)}</span></div>` : ""}
    <div class="detail-actions">
      <button class="btn-plain" data-act="friend-msg" data-username="${esc(f.username)}">发消息</button>
      <button class="btn-plain" data-act="friend-remark" data-username="${esc(f.username)}" data-remark="${esc(f.remark || "")}">设置备注</button>
      <button class="btn-plain" data-act="friend-block" data-username="${esc(f.username)}">加入黑名单</button>
      <button class="btn-plain btn-danger" data-act="friend-remove" data-username="${esc(f.username)}">删除好友</button>
    </div>
  </div>`;
}

// ---------------------------------------------------------------------------
// 渲染：群组

function groupsHTML() {
  const invRows = S.invitations.map((inv) => `
    <div class="row disabled">
      ${avHTML(inv.group_name || inv.group_id || "群")}
      <div class="row-main">
        <div class="row-top"><span class="row-title">${esc(inv.group_name || "群聊")}</span></div>
        <div class="row-sub"><span class="preview">${esc(inv.inviter || "")} 邀请你加入群聊</span></div>
      </div>
      <div class="row-btns">
        <button class="btn-plain" data-act="inv-accept" data-iid="${esc(inv.invitation_id)}">接受</button>
        <button class="btn-plain" data-act="inv-decline" data-iid="${esc(inv.invitation_id)}">拒绝</button>
      </div>
    </div>`).join("");
  const groupRows = S.groups.map((g) => `
    <div class="row group-row ${S.detail && S.detail.kind === "group" && S.detail.data.group_id === g.group_id ? "active" : ""}"
         data-act="group-open" data-gid="${esc(g.group_id)}">
      ${avHTML(g.name || g.group_id)}
      <div class="row-main">
        <div class="row-top"><span class="row-title">${esc(g.name || g.group_id)}</span></div>
        <div class="row-sub"><span class="preview">${g.member_count || 0} 人${g.my_role === "owner" ? " · 我是群主" : g.my_role === "admin" ? " · 我是管理员" : ""}</span></div>
      </div>
    </div>`).join("");
  return `
    <div class="list-head">
      <h2>群组</h2>
      <button class="btn-plain" data-act="group-create">＋ 创建群聊</button>
    </div>
    <div class="list-scroll">
      ${invRows ? `<div class="list-section">群邀请（${S.invitations.length}）</div>${invRows}` : ""}
      <div class="list-section">我的群聊（${S.groups.length}）</div>
      ${groupRows || `<div class="empty-tip">还没有加入任何群聊</div>`}
    </div>`;
}

async function renderGroupDetail() {
  const el = $("#pane-main");
  if (!S.detail || S.detail.kind !== "group") return;
  const g = S.groups.find((x) => x.group_id === S.detail.data.group_id) || S.detail.data;
  let members = S.members[g.group_id];
  if (!members) {
    el.innerHTML = `<div class="detail"><div class="empty-tip">加载中…</div></div>`;
    try { members = await bridge("group_members", { group_id: g.group_id }); S.members[g.group_id] = members; }
    catch (err) { el.innerHTML = `<div class="detail"><div class="empty-tip">${esc(err.message)}</div></div>`; return; }
    if (!S.detail || S.detail.kind !== "group" || S.detail.data.group_id !== g.group_id) return;
  }
  const isOwner = g.my_role === "owner" || g.my_role === "admin";
  const memberCells = members.map((m) => `
    <div class="member-cell">${avHTML(m.nickname || m.username, "sm")}
      <div class="member-name">${esc(m.nickname || m.username)}${m.role === "owner" ? "（群主）" : m.role === "admin" ? "（管理员）" : ""}</div>
    </div>`).join("");
  el.innerHTML = `
  <div class="detail">
    <div class="chat-head" style="margin:-28px -24px 20px">
      <button class="chat-back" data-act="back"><svg viewBox="0 0 24 24"><path d="M15.5 4.5 8 12l7.5 7.5-1.8 1.8L4.4 12l9.3-9.3z"/></svg></button>
      <div class="chat-title">群聊资料</div>
    </div>
    <div class="detail-head">
      ${avHTML(g.name || g.group_id, "lg")}
      <div class="info">
        <div class="detail-name">${esc(g.name || g.group_id)}</div>
        <div class="detail-sub">${g.member_count || members.length} 人 · ${esc(g.my_role === "owner" ? "我是群主" : g.my_role === "admin" ? "我是管理员" : "成员")}</div>
      </div>
    </div>
    <div class="detail-field"><span class="k">群 ID</span><span>${esc(g.group_id)}</span></div>
    ${g.description ? `<div class="detail-field"><span class="k">群公告</span><span>${esc(g.description)}</span></div>` : ""}
    <div style="margin-top:14px;color:var(--text-2);font-size:13px">群成员（${members.length}）</div>
    <div class="member-grid">${memberCells}</div>
    <div class="detail-actions">
      <button class="btn-plain" data-act="group-msg" data-gid="${esc(g.group_id)}">发消息</button>
      <button class="btn-plain" data-act="group-online" data-gid="${esc(g.group_id)}">查看在线人数</button>
      ${isOwner ? `<button class="btn-plain" data-act="group-rename" data-gid="${esc(g.group_id)}" data-name="${esc(g.name || "")}">修改群名</button>` : ""}
      <button class="btn-plain" data-act="group-invite" data-gid="${esc(g.group_id)}">邀请好友</button>
      <button class="btn-plain btn-danger" data-act="group-leave" data-gid="${esc(g.group_id)}">退出群聊</button>
    </div>
  </div>`;
}

// ---------------------------------------------------------------------------
// 渲染：聊天窗口

function currentConv() {
  if (S.openConv && String(S.openConv).startsWith("peer:")) {
    const peer = S.openConv.slice(5);
    const f = S.friends.find((x) => x.username === peer);
    return { id: S.openConv, type: "single", peer, name: (f && (f.remark || f.nickname)) || peer,
      unread: 0, marked_unread: false, pinned: false, max_seq: 0, last_seq: 0, peer_read_seq: 0 };
  }
  return S.convs.find((c) => c.id === S.openConv) || null;
}
function renderChat() {
  const c = currentConv();
  if (!c) return;
  const el = $("#pane-main");
  const isGroup = c.type === "group";
  const meta = S.convMeta[c.id] || {};
  const title = isGroup ? (c.name || (meta.group && meta.group.name) || c.group_id) : (c.name || c.peer);
  el.innerHTML = `
  <div class="chat">
    <div class="chat-head">
      <button class="chat-back" data-act="back"><svg viewBox="0 0 24 24"><path d="M15.5 4.5 8 12l7.5 7.5-1.8 1.8L4.4 12l9.3-9.3z"/></svg></button>
      <div class="chat-title"><span id="chat-title-text">${esc(title)}</span><span class="chat-sub" id="chat-sub"></span></div>
      ${isGroup ? `<button class="btn-plain" data-act="group-info" data-gid="${esc(c.group_id)}">群资料</button>` : ""}
    </div>
    <div class="chat-body" id="chat-body"></div>
    <div class="chat-foot">
      <div id="reply-bar-slot"></div>
      <div class="chat-tools">
        <button class="tool-btn" data-act="pick-image" title="发送图片">
          <svg viewBox="0 0 24 24"><path d="M5 3h14a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2zm0 12 4.5-5 3.5 4 2.5-3L19 15H5zm3.5-7a1.5 1.5 0 1 0 0-3 1.5 1.5 0 0 0 0 3z"/></svg>
        </button>
        <button class="tool-btn" data-act="pick-file" title="发送文件">
          <svg viewBox="0 0 24 24"><path d="M13 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V9l-6-6zm-1 7V4.5L17.5 10H12z"/></svg>
        </button>
      </div>
      <div class="chat-input-area">
        <textarea id="chat-input" class="chat-input" rows="1" placeholder="输入消息，Enter 发送，Shift+Enter 换行"></textarea>
        <button class="send-btn" id="send-btn" data-act="send">发送</button>
      </div>
    </div>
  </div>`;
  renderMessages();
  bindChatInput();
}
function renderChatSub() {
  const el = $("#chat-sub");
  if (!el) return;
  const c = currentConv();
  if (!c) return;
  const t = S.typing[c.id];
  if (t && Date.now() - t.ts < 5000 && Date.now() - (t.firstSeen || t.ts) < 15000 && t.username !== (S.me && S.me.username)) {
    el.textContent = `${displayNameOf(t.username)} 正在输入…`;
    return;
  }
  el.textContent = c.type === "group" ? (S.convMeta[c.id] && S.convMeta[c.id].group ? `${S.convMeta[c.id].group.member_count || ""} 人` : "") : "";
}
function msgDayKey(m) { return (m.created_at || "").slice(0, 10); }
function renderMessages() {
  const body = $("#chat-body");
  if (!body) return;
  const c = currentConv();
  if (!c) return;
  const msgs = S.messages[c.id] || [];
  const self = S.me && S.me.username;
  const lastSelfSeq = (() => {
    for (let i = msgs.length - 1; i >= 0; i--) {
      const m = msgs[i];
      if (m.sender === self && m.sender_type !== "system" && !m.recalled) return m.seq;
    }
    return 0;
  })();
  let html = "";
  let prevDay = "";
  for (const m of msgs) {
    const day = msgDayKey(m);
    if (day && day !== prevDay) { html += `<div class="day-chip"><span>${esc(fmtChatTime(m.created_at))}</span></div>`; prevDay = day; }
    html += msgHTML(m, c, m.seq === lastSelfSeq);
  }
  body.innerHTML = html || `<div class="empty-tip" style="margin-top:40px">还没有消息，打个招呼吧</div>`;
  body.scrollTop = body.scrollHeight;
}
function msgHTML(m, c, isLastSelf) {
  const self = S.me && S.me.username;
  if (m.sender_type === "system" || m.type === "tip") {
    return `<div class="msg-sys"><span>${esc(m.Text || m.text || tipText(m))}</span></div>`;
  }
  if (m.recalled) {
    const who = m.sender === self ? "你" : esc(displayNameOf(m.sender));
    return `<div class="msg-sys"><span>${who}撤回了一条消息</span></div>`;
  }
  const isSelf = m.sender === self;
  const isGroup = c.type === "group";
  const senderName = isSelf ? "" : esc(displayNameOf(m.sender));
  let contentHTML = "";
  if (m.type === "text") {
    contentHTML = esc(bodyText(m)).replace(/\n/g, "<br>");
  } else if (m.type === "image") {
    const b = m.body || {};
    contentHTML = `<img class="msg-image" loading="lazy" src="${esc(mediaSrc(b.url, b.content_type))}" alt="图片" onclick="window.open(this.src)">`;
  } else if (m.type === "file") {
    const b = m.body || {};
    contentHTML = `
      <div class="file-card" data-act="open-file" data-url="${esc(mediaSrc(b.url, b.content_type))}">
        <div class="file-ico"><svg viewBox="0 0 24 24"><path d="M13 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V9l-6-6zm-1 7V4.5L17.5 10H12z"/></svg></div>
        <div><div class="file-name">${esc(b.name || "文件")}</div><div class="file-size">${fmtSize(b.size || 0)}</div></div>
      </div>`;
  } else {
    contentHTML = `[${esc(m.type)}消息]`;
  }
  let quoteHTML = "";
  if (m.reply_to) {
    const orig = (S.messages[c.id] || []).find((x) => x.seq === m.reply_to.seq);
    const q = orig ? (orig.type === "text" ? bodyText(orig) : `[${orig.type}]`) : `第 ${m.reply_to.seq} 条消息`;
    const qn = orig && orig.sender !== self ? displayNameOf(orig.sender) : orig ? "我" : "";
    quoteHTML = `<div class="msg-quote">${qn ? esc(qn) + "：" : ""}${esc(q).slice(0, 60)}</div>`;
  }
  let reactionsHTML = "";
  if (m.reactions && m.reactions.length) {
    const KEY_EMOJI = { like: "👍", heart: "❤️" };
    reactionsHTML = `<div class="reactions">` + m.reactions.map((r) =>
      `<button class="react-chip ${r.reacted ? "mine" : ""}" data-act="toggle-react" data-seq="${m.seq}" data-key="${esc(r.key)}" data-reacted="${!!r.reacted}">${esc(KEY_EMOJI[r.key] || r.key)} ${r.count > 1 ? r.count : ""}</button>`
    ).join("") + `</div>`;
  }
  const readTag = !isGroup && isSelf && isLastSelf && c.peer_read_seq >= m.seq
    ? `<span class="read-tag">已读</span>` : "";
  const menuBtn = `<button class="msg-menu-btn" data-act="msg-menu" data-seq="${m.seq}">⋯</button>`;
  return `
  <div class="msg-row ${isSelf ? "self" : ""}" data-seq="${m.seq}">
    ${avHTML(isSelf ? (S.me.nickname || self) : displayNameOf(m.sender))}
    <div class="msg-main">
      ${isGroup && !isSelf ? `<div class="msg-sender">${senderName}</div>` : ""}
      ${quoteHTML}
      <div class="msg-bubble">${contentHTML}</div>
      ${reactionsHTML}
      <div class="msg-meta"><span>${esc(fmtChatTime(m.created_at))}</span>${readTag}</div>
    </div>
    ${menuBtn}
  </div>`;
}
function bodyText(m) {
  const b = m.body;
  if (b == null) return "";
  if (typeof b === "string") { try { return JSON.parse(b).text || ""; } catch { return ""; } }
  return b.text || "";
}
function tipText(m) {
  const b = m.body;
  if (b && typeof b === "object" && b.text) return b.text;
  if (typeof b === "string") { try { return JSON.parse(b).text || "[系统消息]"; } catch { return "[系统消息]"; } }
  return "[系统消息]";
}
function renderReplyBar() {
  const slot = $("#reply-bar-slot");
  if (!slot) return;
  if (!S.replyTo) { slot.innerHTML = ""; return; }
  const c = currentConv();
  const orig = (S.messages[c.id] || []).find((x) => x.seq === S.replyTo);
  const preview = orig ? (orig.type === "text" ? bodyText(orig) : `[${orig.type}]`) : "";
  const who = orig && orig.sender !== (S.me && S.me.username) ? displayNameOf(orig.sender) : "我";
  slot.innerHTML = `
    <div class="reply-bar">回复 ${esc(who)}：
      <span class="quote">${esc(preview).slice(0, 40)}</span>
      <button data-act="reply-cancel">✕</button>
    </div>`;
}
function bindChatInput() {
  const input = $("#chat-input");
  if (!input) return;
  let lastTyping = 0;
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); sendCurrentText(); }
  });
  input.addEventListener("input", () => {
    input.style.height = "auto";
    input.style.height = Math.min(input.scrollHeight, 120) + "px";
    const now = Date.now();
    if (now - lastTyping > 3000) {
      lastTyping = now;
      // 伪会话（peer:用户名）优先用会话列表里的真实 ID：对端才能把 typing 对到会话行上。
      // 列表里还没有会话时退回 toUser——此时对端也无会话行可显示，服务端 push 不带会话 ID 无影响。
      const cv = String(S.openConv || "");
      if (cv.startsWith("peer:")) {
        const peer = cv.slice(5);
        const known = S.convs.find((c) => c.type !== "group" && c.peer === peer);
        bridge("typing", known ? { id: known.id } : { toUser: peer }).catch(() => {});
      } else {
        bridge("typing", { id: cv }).catch(() => {});
      }
    }
  });
  setTimeout(() => input.focus(), 50);
}
async function sendCurrentText() {
  const input = $("#chat-input");
  if (!input) return;
  const text = input.value.trim();
  if (!text || !S.openConv) return;
  const params = { id: S.openConv, text };
  if (S.replyTo) params.reply_seq = S.replyTo;
  try {
    $("#send-btn").disabled = true;
    const m = await bridge("send_text", params);
    input.value = ""; input.style.height = "auto";
    S.replyTo = null; renderReplyBar();
    input.focus();
    // 伪会话首条消息已送达：切到真实会话（走 openConv 重建 convMeta 并拉取历史，
    // 不能用伪会话数组直接覆盖真实会话键——msgs 推送可能先于 reply 到达）
    if (m && m.conversation_id && m.conversation_id !== S.openConv) {
      S.replyTo = null; renderReplyBar();
      await openConv(m.conversation_id);
    }
  } catch (err) {
    toast(err.message, true);
  } finally {
    const b = $("#send-btn"); if (b) b.disabled = false;
  }
}
async function markRead() {
  const c = currentConv();
  if (!c || !document.hasFocus()) return;
  if (c.unread > 0 || c.marked_unread) {
    try { await bridge("read", { id: c.id, seq: c.max_seq }); } catch {}
  }
}
window.addEventListener("focus", () => { if (S.openConv) markRead(); });

// ---------------------------------------------------------------------------
// 渲染：我的

function meHTML() {
  return `
    <div class="list-head"><h2>我的</h2></div>
    <div class="list-scroll"><div class="empty-tip">加载中…</div></div>`;
}
function meDetailHTML() {
  const me = S.me || {};
  const connText = S.conn === "connected" ? "已连接" : S.conn === "connecting" ? "连接中…" : "已断开";
  return `
  <div class="detail" style="padding:0;overflow-y:auto;height:100%">
    <div class="chat-head">
      <button class="chat-back" data-act="back"><svg viewBox="0 0 24 24"><path d="M15.5 4.5 8 12l7.5 7.5-1.8 1.8L4.4 12l9.3-9.3z"/></svg></button>
      <div class="chat-title">我的</div>
    </div>
    <div class="me-card">
      ${avHTML(me.nickname || me.username || "?", "lg")}
      <div class="info">
        <div class="detail-name">${esc(me.nickname || me.username || "")}</div>
        <div class="detail-sub">微信号：${esc(me.username || "")} · <span style="color:${S.conn === "connected" ? "var(--green)" : "var(--red)"}">${connText}</span></div>
      </div>
      <button class="btn-plain" data-act="me-edit-nick">修改昵称</button>
    </div>
    <div class="me-section">
      <h3>账号安全</h3>
      <div class="passwd-form" id="passwd-form">
        <input type="password" id="pw-old" placeholder="当前密码">
        <input type="password" id="pw-new" placeholder="新密码（至少 6 位）">
        <div id="pw-msg" class="hidden"></div>
        <button class="btn-plain" data-act="pw-submit">修改密码</button>
      </div>
    </div>
    <div class="me-section">
      <h3>设备与登录态 <button class="btn-plain" style="font-size:12px" data-act="sessions-refresh">刷新</button></h3>
      <div id="sessions-box"><div class="empty-tip">加载中…</div></div>
    </div>
    <div class="logout-bar">
      <button class="btn-primary" style="background:#fff;color:var(--red);border:1px solid #f0d0d0" data-act="me-logout">退出登录</button>
    </div>
  </div>`;
}
async function loadSessions() {
  const box = $("#sessions-box");
  if (!box) return;
  try {
    const d = await bridge("sessions");
    box.innerHTML = (d.items || []).map((s) => {
      const cur = s.session_id === d.current;
      return `
      <div class="me-row">
        <div class="k">${esc(s.platform || "web")} · ${esc(s.device_name || s.device_id || "")}</div>
        <div class="v">${cur ? "本机" : esc((s.session_id || "").slice(0, 18))}</div>
        <button class="op ${cur ? "danger" : ""}" data-act="session-revoke" data-sid="${esc(s.session_id)}">${cur ? "下线本机" : "下线"}</button>
      </div>`;
    }).join("") || `<div class="empty-tip">暂无</div>`;
  } catch (err) {
    box.innerHTML = `<div class="empty-tip">${esc(err.message)}</div>`;
  }
}

// ---------------------------------------------------------------------------
// 弹窗 / 菜单

function showDialog(html) {
  const ov = $("#overlay");
  ov.innerHTML = `<div class="dialog">${html}</div>`;
  ov.classList.remove("hidden");
  ov.onclick = (e) => { if (e.target === ov) closeDialog(); };
  return ov;
}
function closeDialog() {
  const ov = $("#overlay");
  ov.classList.add("hidden");
  ov.innerHTML = "";
}
function promptDialog(title, placeholder, value = "") {
  return new Promise((resolve) => {
    showDialog(`
      <h3>${esc(title)}</h3>
      <div class="dialog-body">
        <input type="text" id="dlg-input" placeholder="${esc(placeholder)}" value="${esc(value)}">
      </div>
      <div class="dialog-foot">
        <button class="dlg-cancel" data-dlg="cancel">取消</button>
        <button class="dlg-ok" data-dlg="ok">确定</button>
      </div>`);
    const input = $("#dlg-input");
    input.focus(); input.select();
    input.onkeydown = (e) => { if (e.key === "Enter") { resolve(input.value.trim()); closeDialog(); } };
    $$("#overlay [data-dlg]").forEach((b) => b.onclick = () => {
      resolve(b.dataset.dlg === "ok" ? input.value.trim() : null);
      closeDialog();
    });
  });
}
function confirmDialog(title, text) {
  return new Promise((resolve) => {
    showDialog(`
      <h3>${esc(title)}</h3>
      <div class="dialog-body" style="text-align:center;color:var(--text-2);font-size:13px">${esc(text)}</div>
      <div class="dialog-foot">
        <button class="dlg-cancel" data-dlg="cancel">取消</button>
        <button class="dlg-ok" data-dlg="ok" style="background:var(--red)">确定</button>
      </div>`);
    $$("#overlay [data-dlg]").forEach((b) => b.onclick = () => {
      resolve(b.dataset.dlg === "ok"); closeDialog();
    });
  });
}
function friendCheckDialog(title, onOk) {
  const rows = S.friends.map((f) => `
    <label class="check-row">
      <input type="checkbox" value="${esc(f.username)}">
      ${avHTML(f.remark || f.nickname || f.username, "sm")}
      <span>${esc(f.remark || f.nickname || f.username)}</span>
    </label>`).join("");
  showDialog(`
    <h3>${esc(title)}</h3>
    <div class="dialog-body">
      ${rows || `<div class="empty-tip">还没有好友</div>`}
    </div>
    <div class="dialog-foot">
      <button class="dlg-cancel" data-dlg="cancel">取消</button>
      <button class="dlg-ok" data-dlg="ok">确定</button>
    </div>`);
  $$("#overlay [data-dlg]").forEach((b) => b.onclick = () => {
    // 先关弹窗再回调：onOk 里可能再弹新对话框（如创建群聊 → 输入群名），
    // 否则外层 closeDialog 会把新弹的对话框关掉。
    const ok = b.dataset.dlg === "ok";
    const picked = ok ? $$("#overlay input:checked").map((i) => i.value) : null;
    closeDialog();
    if (ok) onOk(picked);
  });
}
function showMenu(items, x, y) {
  const menu = $("#menu");
  menu.innerHTML = items.map((it, i) =>
    `<button class="menu-item ${it.danger ? "danger" : ""}" data-mi="${i}">${esc(it.label)}</button>`).join("");
  menu.classList.remove("hidden");
  const mw = 150, mh = items.length * 38 + 12;
  menu.style.left = Math.min(x, innerWidth - mw - 8) + "px";
  menu.style.top = Math.min(y, innerHeight - mh - 8) + "px";
  $$("#menu [data-mi]").forEach((b) => b.onclick = () => {
    hideMenu();
    items[+b.dataset.mi].onClick();
  });
}
function hideMenu() { $("#menu").classList.add("hidden"); }
document.addEventListener("click", (e) => {
  if (!e.target.closest("#menu")) hideMenu();
});

// ---------------------------------------------------------------------------
// 事件委托

document.addEventListener("click", async (e) => {
  const t = e.target.closest("[data-act]");
  if (!t) return;
  const act = t.dataset.act;
  const d = t.dataset;
  try {
    switch (act) {
      // 导航
      case "nav-tab": {
        S.tab = d.tab;
        S.detail = null;
        if (d.tab === "me") { renderListPane(); renderMain(); loadSessions(); }
        else { renderListPane(); renderMain(); }
        if (d.tab === "chat" && S.openConv) { renderChat(); }
        // 「我的」页内容渲染在主面板：窄视口主面板默认隐藏，需展开才能看到（返回箭头回列表）。
        if (d.tab === "me" && innerWidth < 720) document.body.classList.add("show-main");
        else document.body.classList.remove("show-main");
        break;
      }
      case "back": {
        document.body.classList.remove("show-main");
        S.openConv = null; S.detail = null;
        // 窄视口从「我的」返回：切回聊天列表，避免列表停留在「我的」的加载占位。
        if (S.tab === "me") { S.tab = "chat"; renderListPane(); }
        renderMain();
        break;
      }

      // 会话
      case "conv-open": openConv(d.id); break;
      case "conv-menu": {
        const c = S.convs.find((x) => x.id === d.id);
        showMenu([
          { label: c.pinned ? "取消置顶" : "置顶", onClick: () => bridge("pin_conv", { id: d.id, pinned: c.pinned ? "false" : "true" }).catch(showErr) },
          { label: "清空聊天记录", onClick: async () => { if (await confirmDialog("清空聊天记录", "本端不再展示历史消息")) bridge("clear_conv", { id: d.id }).catch(showErr); } },
          { label: "删除会话", danger: true, onClick: async () => { if (await confirmDialog("删除会话", "会话将从列表移除")) { if (S.openConv === d.id) { S.openConv = null; renderMain(); document.body.classList.remove("show-main"); } bridge("delete_conv", { id: d.id }).catch(showErr); } } },
        ], e.clientX, e.clientY);
        break;
      }
      case "group-info": {
        S.tab = "groups";
        S.detail = { kind: "group", data: { group_id: d.gid } };
        S.members = {};
        renderListPane(); renderGroupDetail();
        document.body.classList.add("show-main");
        break;
      }

      // 消息
      case "send": sendCurrentText(); break;
      case "pick-image": $("#file-image").value = ""; $("#file-image").click(); break;
      case "pick-file": $("#file-any").value = ""; $("#file-any").click(); break;
      case "open-file": window.open(d.url); break;
      case "msg-menu": {
        const c = currentConv();
        const m = (S.messages[c.id] || []).find((x) => x.seq === +d.seq);
        if (!m) break;
        const self = S.me && S.me.username;
        const items = [
          { label: "回复", onClick: () => { S.replyTo = m.seq; renderReplyBar(); $("#chat-input") && $("#chat-input").focus(); } },
          // 服务端 reaction key 只允许字母数字与 _-.:（emoji 会被 invalid_reaction_key 拒绝），用固定 key 传输、展示层映射 emoji。
          { label: "回应 👍", onClick: () => bridge("react", { id: c.id, seq: m.seq, key: "like" }).catch(showErr) },
          { label: "回应 ❤️", onClick: () => bridge("react", { id: c.id, seq: m.seq, key: "heart" }).catch(showErr) },
        ];
        if (m.sender === self) {
          items.push({ label: "撤回", danger: true, onClick: () => bridge("recall", { id: c.id, seq: m.seq }).catch(showErr) });
          items.push({ label: "删除", danger: true, onClick: () => bridge("delete_msgs", { id: c.id, seqs: [m.seq] }).catch(showErr) });
        }
        const r = t.getBoundingClientRect();
        showMenu(items, r.left, r.bottom);
        break;
      }
      case "toggle-react": {
        const c = currentConv();
        const params = { id: c.id, seq: +d.seq, key: d.key };
        if (d.reacted === "true") { params.remove = "true"; await bridge("react", params).catch(showErr); }
        else await bridge("react", params).catch(showErr);
        break;
      }
      case "reply-cancel": S.replyTo = null; renderReplyBar(); break;

      // 通讯录
      case "lookup-btn": {
        const name = $("#lookup-input").value.trim();
        const box = $("#lookup-result");
        if (!name) { box.innerHTML = ""; break; }
        try {
          const u = await bridge("lookup_user", { username: name });
          box.innerHTML = `
            <div class="row disabled">
              ${avHTML(u.nickname || u.username)}
              <div class="row-main">
                <div class="row-top"><span class="row-title">${esc(u.nickname || u.username)}</span></div>
                <div class="row-sub"><span class="preview">用户名：${esc(u.username)}</span></div>
              </div>
              <button class="btn-plain" data-act="add-friend" data-username="${esc(u.username)}">添加到通讯录</button>
            </div>`;
        } catch (err) {
          box.innerHTML = `<div class="empty-tip" style="padding:14px;color:var(--red)">${esc(err.message)}</div>`;
        }
        break;
      }
      case "add-friend": {
        const message = await promptDialog("发送好友申请", "填写验证消息（可选）", `我是 ${S.me.nickname || S.me.username}`);
        if (message === null) break;
        await bridge("add_friend", { username: d.username, message });
        toast("好友申请已发送");
        $("#lookup-result").innerHTML = "";
        break;
      }
      case "friend-open": {
        const f = S.friends.find((x) => x.username === d.username);
        if (!f) break;
        S.detail = { kind: "friend", data: f };
        renderListPane(); renderMain();
        document.body.classList.add("show-main");
        break;
      }
      case "friend-msg": {
        const res = await bridge("open_peer", { username: d.username });
        if (res.conversation && res.conversation.conversation_id) {
          S.convs = S.convs.some((x) => x.id === res.conversation.conversation_id)
            ? S.convs : [convFromState(res.conversation), ...S.convs];
          S.tab = "chat";
          renderListPane(); updateBadges();
          await openConv(res.conversation.conversation_id, res);
        } else {
          // 尚无往来：打开伪会话，首条消息发出后服务端建会话
          S.tab = "chat";
          renderListPane();
          await openPseudoConv(d.username);
        }
        break;
      }
      case "friend-remark": {
        const remark = await promptDialog("设置好友备注", "备注名", d.remark || "");
        if (remark === null) break;
        await bridge("remark", { username: d.username, remark });
        toast("备注已保存");
        break;
      }
      case "friend-remove": {
        if (!(await confirmDialog("删除好友", `确定删除 ${displayNameOf(d.username)} 吗？`))) break;
        await bridge("remove_friend", { username: d.username });
        S.detail = null;
        renderListPane(); renderMain();
        document.body.classList.remove("show-main");
        toast("已删除好友");
        break;
      }
      case "friend-block": {
        if (!(await confirmDialog("加入黑名单", `拉黑后 ${displayNameOf(d.username)} 将无法给你发消息`))) break;
        await bridge("block", { username: d.username });
        toast("已加入黑名单");
        break;
      }
      case "bl-remove":
        await bridge("unblock", { username: d.username });
        toast("已移出黑名单");
        break;
      case "req-accept": await bridge("accept_friend", { username: d.username }); toast("已成为好友"); break;
      case "req-decline": await bridge("decline_friend", { username: d.username }); break;

      // 群
      case "group-open": {
        const g = S.groups.find((x) => x.group_id === d.gid);
        if (!g) break;
        S.detail = { kind: "group", data: g };
        S.members = {};
        renderListPane(); renderGroupDetail();
        document.body.classList.add("show-main");
        break;
      }
      case "group-create":
        friendCheckDialog("创建群聊", async (picked) => {
          const name = await promptDialog("群聊名称", "输入群名");
          if (!name) return;
          const g = await bridge("create_group", { name, members: picked });
          toast("群聊已创建");
          await groupOpenAndChat(g.group_id);
        });
        break;
      case "inv-accept": await bridge("accept_invitation", { invitation_id: d.iid }); toast("已加入群聊"); break;
      case "inv-decline": await bridge("decline_invitation", { invitation_id: d.iid }); break;
      case "group-msg": await groupOpenAndChat(d.gid); break;
      case "group-online": {
        const r = await bridge("online_count", { group_id: d.gid });
        toast(`当前在线 ${r.online_count} 人`);
        break;
      }
      case "group-rename": {
        const name = await promptDialog("修改群名", "输入新群名", d.name || "");
        if (!name) break;
        await bridge("rename_group", { id: d.gid, name });
        toast("群名已更新");
        break;
      }
      case "group-invite":
        friendCheckDialog("邀请好友进群", async (picked) => {
          if (!picked.length) return;
          await bridge("invite_group", { group_id: d.gid, users: picked });
          toast("邀请完成");
          S.members[d.gid] = null;
          renderGroupDetail();
        });
        break;
      case "group-leave": {
        if (!(await confirmDialog("退出群聊", "退出后将不再接收该群消息"))) break;
        await bridge("leave_group", { group_id: d.gid });
        S.detail = null;
        renderListPane(); renderMain();
        document.body.classList.remove("show-main");
        toast("已退出群聊");
        break;
      }

      // 我的
      case "me-edit-nick": {
        const nick = await promptDialog("修改昵称", "输入新昵称", (S.me.nickname || ""));
        if (!nick) break;
        await bridge("update_profile", { nickname: nick });
        toast("昵称已更新");
        break;
      }
      case "pw-submit": {
        const msgEl = $("#pw-msg");
        msgEl.classList.add("hidden");
        try {
          await bridge("set_password", { old: $("#pw-old").value, new: $("#pw-new").value });
          msgEl.className = "form-ok"; msgEl.textContent = "密码修改成功";
          msgEl.classList.remove("hidden");
          $("#pw-old").value = ""; $("#pw-new").value = "";
        } catch (err) {
          msgEl.className = "form-err"; msgEl.textContent = err.message;
          msgEl.classList.remove("hidden");
        }
        break;
      }
      case "sessions-refresh": loadSessions(); break;
      case "session-revoke": {
        const cur = t.classList.contains("danger");
        const ok = await confirmDialog(cur ? "下线本机" : "下线该设备", cur ? "下线后本会话将立即失效，需要重新登录" : "该设备的登录状态将被吊销");
        if (!ok) break;
        await bridge("revoke_session", { session_id: d.sid });
        if (!cur) { toast("已下线该设备"); loadSessions(); }
        break;
      }
      case "me-logout": {
        if (!(await confirmDialog("退出登录", "确定要退出当前账号吗？"))) break;
        await fetch("/api/logout", { method: "POST" });
        location.reload();
        break;
      }
      case "kicked-relogin": location.reload(); break;
    }
  } catch (err) {
    showErr(err);
  }
});
function showErr(err) { toast(err.message || String(err), true); }
function convFromState(st) {
  return {
    id: st.conversation_id, type: st.conversation_type, peer: st.peer, group_id: st.group_id,
    name: st.peer || "", unread: st.unread_count, pinned: !!st.pinned_at, marked_unread: st.marked_unread,
    max_seq: st.max_seq, last_seq: st.max_seq, last_at: st.last_message_at || "",
    last_preview: "", peer_read_seq: st.peer_read_seq,
  };
}

// ---------------------------------------------------------------------------
// 打开会话

async function openConv(id, preloaded) {
  S.openConv = id;
  S.replyTo = null;
  document.body.classList.add("show-main");
  if (String(id).startsWith("peer:")) { await openPseudoConv(String(id).slice(5)); return; }
  renderChatSkeleton();
  try {
    const data = preloaded || await bridge("open_conv", { id });
    S.messages[id] = data.messages || [];
    S.convMeta[id] = { friend: data.friend || null, group: data.group || null, conversation: data.conversation };
    renderChat();
    renderChatSub();
    markRead();
  } catch (err) {
    $("#pane-main").innerHTML = `<div class="main-empty">${esc(err.message)}</div>`;
  }
}
function renderChatSkeleton() {
  $("#pane-main").innerHTML = `
  <div class="chat">
    <div class="chat-head"><div class="chat-title">…</div></div>
    <div class="chat-body"><div class="chat-loading">加载中…</div></div>
    <div class="chat-foot"><div class="chat-input-area"><textarea class="chat-input" disabled></textarea></div></div>
  </div>`;
}
function openPseudoConv(username) {
  S.openConv = "peer:" + username;
  S.replyTo = null;
  if (!S.messages[S.openConv]) S.messages[S.openConv] = [];
  renderChat();
}
async function groupOpenAndChat(gid) {
  const conv = S.convs.find((c) => c.type === "group" && c.group_id === gid);
  S.tab = "chat";
  renderListPane(); updateBadges();
  if (conv) await openConv(conv.id);
  else toast("群会话同步中，请稍后从聊天列表打开");
}

// ---------------------------------------------------------------------------
// 上传

async function uploadAndSend(file, msgType) {
  const fd = new FormData();
  fd.append("file", file);
  const resp = await fetch("/api/upload", { method: "POST", body: fd });
  const data = await resp.json();
  if (!resp.ok) throw new Error(data.error || "上传失败");
  await bridge("send_media", { id: S.openConv, msg_type: msgType, file: data.file_info });
}
$("#file-image").addEventListener("change", (e) => {
  const f = e.target.files[0];
  if (f && S.openConv) uploadAndSend(f, "image").catch(showErr);
});
$("#file-any").addEventListener("change", (e) => {
  const f = e.target.files[0];
  if (f && S.openConv) uploadAndSend(f, "file").catch(showErr);
});

// ---------------------------------------------------------------------------
// 登录页事件

$("#login-btn").addEventListener("click", doLogin);
$$("#view-login input").forEach((inp) => inp.addEventListener("keydown", (e) => {
  if (e.key === "Enter") doLogin();
}));

// 窄视口检测
function checkViewport() {
  if (innerWidth >= 720) document.body.classList.remove("show-main");
}
window.addEventListener("resize", checkViewport);

// 启动：立即尝试接入桥。已登录（cookie 会话有效）则收到 bootstrap 直接进主界面；
// 未登录则 ws 关闭后显示登录页。
connectBridge();
