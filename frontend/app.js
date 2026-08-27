'use strict';
/* ==========================================================================
   ClusterTalk — Frontend Application Logic
   --------------------------------------------------------------------------
   This file is organized into:
     1. Mock data (rooms, DMs, members, nodes, seed messages)
     2. ClusterTalkClient — the WebSocket-to-TCP bridge client. It runs in
        MOCK MODE by default (no server required to preview the UI) but is
        structured so swapping in the real bridge is a ~10 line change —
        see the "REAL BACKEND WIRING" comment block below the class.
     3. Rendering functions (pure-ish: state -> DOM)
     4. Event wiring or "controllers" per UI region
     5. Boot sequence

   --------------------------------------------------------------------------
   FIXES APPLIED IN THIS VERSION (see inline "// FIX:" comments):
     1. Added this._hasAuthenticated, a flag independent of the live
        this._connected flag, so onclose() can correctly tell "died before
        ever logging in" apart from "died after a fully-authenticated
        session was already chatting". Previously the DISCONNECT handler
        set this._connected = false BEFORE closing the socket, so by the
        time onclose ran, wasConnected always read false — even for a
        session that had been live for minutes — causing onclose to bail
        out early and never call _scheduleReconnect() at all. That's the
        root cause of the permanent "Reconnecting…" hang after a node was
        taken down and brought back up.
     2. DISCONNECT handler no longer sets this._connected = false itself —
        it just closes the socket and lets onclose() do the single source
        of truth bookkeeping (avoids the race above).
     3. _scheduleReconnect() no longer resets this._reconnecting = false
        BEFORE the reconnect attempt starts — only after it settles (in a
        finally block). Previously a second disconnect event arriving
        while a reconnect was still in flight could slip past the guard
        and fire a second concurrent connect() call, corrupting shared
        _loginResolve/_loginReject state and silently hanging one of them
        forever.
     4. _realSend() now checks the socket's readyState before sending,
        instead of only checking for null. Previously, typing/sending a
        message while the socket was CLOSING/CLOSED threw a silent
        "WebSocket is already in CLOSING or CLOSED state" console error
        and the message vanished with no feedback to the user.
     5. Every WebSocket event handler (onopen/onclose/onerror/onmessage)
        set up in _realConnect()'s proceed() now checks `this._socket
        !== socket` and bails out immediately if the socket it belongs to
        is no longer the client's CURRENT socket. Previously, handlers
        read/wrote shared instance state (this._connected,
        this._hasAuthenticated) with no way to tell whether the event
        they were reacting to came from the live socket or a superseded
        one. Concretely: socket A gets replaced by socket B (a normal
        reconnect); B logs in successfully and sets _hasAuthenticated =
        true; A's own onclose event, which was merely queued and hadn't
        fired yet, then runs *after* B has already succeeded, reads the
        now-true _hasAuthenticated flag (which describes B, not A), and
        concludes the healthy connection just dropped -- immediately
        scheduling another reconnect. That produced an infinite loop,
        cycling roughly every 1-2 seconds forever with the UI flickering
        between "Connected" and "Reconnecting…" regardless of whether the
        backend was actually healthy. This is the fix for that.
   ========================================================================== */

// ---------------------------------------------------------------------------
// 1. Mock data
// ---------------------------------------------------------------------------

const CURRENT_USER = { id: 'me', username: '', name: 'You', color: '#6366F1' };

const AVATAR_PALETTE = ['#6366F1', '#22C55C', '#F59E0B', '#EF4444', '#0EA5E9', '#EC4899', '#8B5CF6', '#14B8A6'];

function colorForName(name) {
  let hash = 0;
  for (let i = 0; i < name.length; i++) hash = name.charCodeAt(i) + ((hash << 5) - hash);
  return AVATAR_PALETTE[Math.abs(hash) % AVATAR_PALETTE.length];
}
function initialsForName(name) {
  const parts = name.trim().split(/\s+/);
  return parts.length === 1 ? parts[0].slice(0, 2).toUpperCase() : (parts[0][0] + parts[1][0]).toUpperCase();
}

// Preset chat channels. These are real rooms — clicking one sends a
// JOIN_ROOM to the backend. They start empty; messages are whatever real
// users actually send. Create more with "Create or join a room".
const ROOMS = [
  { id: 'general', name: 'general', topic: 'Company-wide announcements and casual conversation', unread: 0 },
  { id: 'watercooler', name: 'watercooler', topic: 'Off-topic chat, memes, and coffee breaks', unread: 0 },
  { id: 'engineering', name: 'engineering', topic: 'Backend, infra, and mesh architecture discussion', unread: 0 },
  { id: 'incident-response', name: 'incident-response', topic: 'Live incident coordination', unread: 0 },
];

// No fake direct messages / no fake member roster — the member panel is
// built from who is actually talking in the room (see renderMembers()).
const DIRECT_MESSAGES = [];
const MEMBERS = [];

// This node list mirrors what server.py's mesh (mesh.py PeerManager)
// would report — used purely to render the topology widget and the
// "connected node" stat, not a real backend query in mock mode.
const MESH_NODES = [
  { id: 'node-A', x: 46, y: 26 },
  { id: 'node-B', x: 214, y: 26 },
  { id: 'node-C', x: 130, y: 104 },
];
let CONNECTED_NODE_ID = 'node-A';

// Rooms start empty — real conversations only.
const SEED_MESSAGES = {};
const DM_SEED_MESSAGES = {};

const EMOJI_SET = ['😀','😂','😍','👍','🙌','🔥','🎉','🚀','❤️','😅','🤔','👀','✅','⚡','💡','🐛','🙏','😎','🥳','😢','👏','💯','🤝','☕'];

// ---------------------------------------------------------------------------
// 2. ClusterTalkClient — WS-to-TCP bridge client (mock-first)
// ---------------------------------------------------------------------------
//
// REAL BACKEND WIRING:
// ClusterTalk's backend (framing.py) speaks binary length-prefixed
// frames over raw TCP, which browsers can't open directly — hence the
// "WebSocket-to-TCP bridge" the brief calls for. To go live:
//   1. Run a small bridge process that accepts WebSocket connections
//      and proxies each one to a TCP connection against run_lb.py
//      (or a run_node.py directly), translating each WS text/binary
//      frame to/from framing.py's [4-byte length][type byte][JSON body]
//      wire format.
//   2. Set MOCK_MODE = false below and BRIDGE_URL to that bridge's
//      ws:// or wss:// address.
//   3. Replace the body of connect()/send() with the real WebSocket
//      calls already stubbed in the `_real*` methods — every call site
//      in this file goes through the client's public methods
//      (connect/sendMessage/joinRoom/onEvent), so nothing else changes.
function isMockModeEnabled() {
  try {
    const params = new URLSearchParams(window.location.search);
    return params.get('mock') === '1' || params.get('mock') === 'true';
  } catch {
    return false;
  }
}

const MOCK_MODE = isMockModeEnabled(); // falls back to mock data when the stack is not available
const API_BASE_URL = 'https://your-railway-backend-url.up.railway.app';
const BRIDGE_URL = `${API_BASE_URL.replace(/^http/, 'ws')}/bridge`;

class ClusterTalkClient {
  constructor() {
    this._listeners = {};
    this._socket = null;
    this._connected = false;
    this._reconnecting = false;
    this._reconnectAttempts = 0;
    this._reconnectTimer = null;
    // FIX #1: tracks whether the CURRENT connection attempt has ever
    // successfully authenticated (LOGIN_ACK ok:true). This is distinct
    // from this._connected, which onclose() also mutates — using the
    // same flag for both "have we authed" and "are we live right now"
    // is what caused the original race. Reset to false at the start of
    // every fresh connection attempt (see proceed() below).
    this._hasAuthenticated = false;
    this._sendQueue = [];
  }

  on(event, handler) {
    (this._listeners[event] = this._listeners[event] || []).push(handler);
    return this;
  }

  _emit(event, payload) {
    (this._listeners[event] || []).forEach((fn) => fn(payload));
  }

  async connect(opts) {
    if (MOCK_MODE) return this._mockConnect();
    return this._realConnect(opts);
  }

  sendMessage(roomId, text, attachments) {
    if (MOCK_MODE) return this._mockSend(roomId, text, attachments);
    return this._realSend(roomId, text, attachments);
  }

  joinRoom(roomId) {
    // In the real bridge this sends a JOIN_ROOM frame (see framing.py
    // MessageType.JOIN_ROOM) over the active WS connection.
    if (!MOCK_MODE) this._socket?.send(JSON.stringify({ type: 'JOIN_ROOM', room_id: roomId }));
  }

  // ---- mock implementation --------------------------------------------
  async _mockConnect() {
    await wait(1400); // simulate handshake latency for the boot screen
    this._connected = true;
    this._emit('connected', { nodeId: CONNECTED_NODE_ID });
    return true;
  }

  _mockSend(roomId, text, attachments) {
    const clientMsgId = 'c' + Math.random().toString(36).slice(2);
    // ACK simulation: mirrors the real backend's flow (framing.py's
    // ACK message type, sent once the node's session_manager has
    // durably recorded the outbound message).
    setTimeout(() => this._emit('ack', { clientMsgId, status: 'delivered' }), 550 + Math.random() * 400);
    if (Math.random() > 0.35) {
      setTimeout(() => this._emit('ack', { clientMsgId, status: 'read' }), 1800 + Math.random() * 1200);
    }
    return clientMsgId;
  }

  _startMockAmbientEvents() {
    // No ambient fake users or messages in mock mode.
    // The app should only show real login activity and real chat traffic.
  }

  // ---- real WebSocket + bridge implementation --------------------------------

  // Internal sequence counter — matches framing.py's seq field.
  // The backend uses this for exactly-once delivery + outbox replay.
  _nextSeq = 1;

  /**
   * Connect to the bridge, authenticate, and set up frame routing.
   * Call this after setting MOCK_MODE = false.
   *
   * Flow:
   *   1. Open WebSocket to BRIDGE_URL
   *   2. Send LOGIN frame (bridge forwards to backend as TCP LOGIN frame)
   *   3. Receive LOGIN_ACK — on ok=true: session live, emit 'connected'
   *                        — on ok=false: emit 'auth_error' with reason
   *
   * username / password come from localStorage or a login form.
   * Default here: prompted from the user on first connect.
   */
  _clearReconnectTimer() {
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
  }

  async _realConnect(opts = {}) {
    const { silent = false } = opts;
    // Show the built-in login form instead of prompt() -- unless this is
    // a silent auto-reconnect after a disconnect, in which case we reuse
    // the credentials from the last successful login instead of
    // interrupting the user with the form again.
    return new Promise((resolve, reject) => {
      this._loginResolve = resolve;
      this._loginReject  = reject;

      const proceed = (username, password, mode) => {
        this._clearReconnectTimer();
        this._authUsername = username;
        this._authPassword = password;
        this._authMode = mode;
        // FIX #1: reset the "have we authed on THIS connection attempt"
        // flag every time we open a fresh socket, so onclose() always
        // judges the attempt currently in flight, not a stale one.
        this._hasAuthenticated = false;
        try { this._socket?.close(); } catch (e) { /* replacing a prior attempt */ }
        const socket = new WebSocket(BRIDGE_URL);
        this._socket = socket;

        // FIX #5 (the "double reconnect loop" bug): every handler below
        // must check `this._socket !== socket` and bail out immediately
        // if true. Without this guard, an OLD socket that gets superseded
        // by a NEWER one (e.g. proceed() calling `.close()` on it, or a
        // brand-new reconnect attempt starting) can still fire its OWN
        // onopen/onclose/onerror/onmessage events *asynchronously*, well
        // after `this._socket` has already moved on to a different
        // instance. Those stale events read/write shared instance state
        // (`this._connected`, `this._hasAuthenticated`) that by then
        // reflects the NEW, healthy socket — not the dead one the event
        // actually came from. In practice this caused an infinite loop:
        // a new socket would log in successfully, and moments later the
        // OLD socket's delayed onclose would fire, wrongly conclude the
        // (actually-fine) connection had just dropped, and immediately
        // schedule ANOTHER reconnect — over and over, roughly every 1-2
        // seconds, forever, with the UI flickering between "Connected"
        // and "Reconnecting…" no matter how healthy the backend was.
        // Every handler now starts by checking it's still talking about
        // the socket currently in charge; if not, it's a no-op.

        socket.onopen = () => {
          if (this._socket !== socket) return;
          
          // ── LOGIN PATH ──────────────────────────────────────────────
          // Send LOGIN or REGISTER. For silent reconnects, include the
          // session_id so the load balancer can route us stickily, while
          // the backend authentically logs us in and recovers state.
          const type = mode === 'register' ? 'REGISTER' : 'LOGIN';
          const payload = {
            type, username, password,
            role: mode === 'admin' ? 'admin' : 'client',
          };
          if (silent && this._sessionId) {
            payload.session_id = this._sessionId;
          }
          socket.send(JSON.stringify(payload));
        };

        socket.onclose = () => {
            if (this._socket !== socket) return; // FIX #5: stale socket, ignore
            const hadAuthenticated = this._hasAuthenticated;
            const suppressReconnect = this._suppressReconnect; // FIX #6
            this._suppressReconnect = false;
            this._connected = false;
            this._emit('disconnected', {});
            if (!hadAuthenticated) {
              const rejectLogin = this._loginReject;
              this._loginReject = null;
              if (rejectLogin) rejectLogin(new Error('connection closed before authentication completed'));
              return;
            }
            if (suppressReconnect) {
              // FIX #6: we were deliberately replaced by a newer login for this
              // account (e.g. it's open in another tab/device) -- don't fight
              // over the session forever. Tell the user and stop here.
              setConnectionState('offline');
              showToast('You were logged in elsewhere — this tab is now disconnected.', 'danger');
              return;
            }
            // We WERE fully logged in and chatting — this is a real drop
            // (node went down, network blip, etc). Reconnect automatically.
            this._scheduleReconnect();
          };

        socket.onerror = (err) => {
          if (this._socket !== socket) return; // FIX #5: stale socket, ignore
          showLoginError('Cannot reach the server. Is the stack running? (python run_stack.py)');
        };

        socket.onmessage = (evt) => {
        if (this._socket !== socket) return; // FIX #5: stale socket, ignore
        let frame;
        try { frame = JSON.parse(evt.data); } catch { return; }
        const t = (frame.type || '').toUpperCase();

        // ── Auth responses ────────────────────────────────────────────
        if (t === 'LOGIN_ACK') {
          if (frame.ok) {
            CURRENT_USER.username = this._authUsername;
            CURRENT_USER.name = this._authUsername;
            hideLoginForm();
            this._connected = true;
            this._hasAuthenticated = true; // FIX #1: mark this attempt as authenticated
            this._sessionId = frame.session_id;
            localStorage.setItem('ct_username', this._authUsername);
            localStorage.setItem('ct_password', this._authPassword);
            localStorage.setItem('ct_role', frame.role || (this._authMode === 'admin' ? 'admin' : 'client'));
            this._emit('connected', { nodeId: frame.session_id?.slice(0, 8) || '?' });
            resolve(true);
            this._loginResolve = null;
            this._loginReject = null;
            this._flushSendQueue();
            if (this._authMode === 'admin') location.replace('admin.html');
          } else {
            showLoginError(this._authMode === 'register'
              ? 'Account created, but sign-in failed — try the Sign In tab.'
              : 'Wrong username or password.');
          }
          return;
        }
        if (t === 'REGISTER_ACK') {
          if (!frame.ok) {
            // Registration failed (e.g. username taken). Don't auto-login —
            // the user explicitly chose Create Account; guide them instead.
            const reason = (frame.reason || '').toLowerCase();
            showLoginError(reason.includes('taken')
              ? 'That username is already taken — use the Sign In tab.'
              : (frame.reason ? frame.reason[0].toUpperCase() + frame.reason.slice(1) : 'Could not create account.'));
            return;
          }
          // REGISTER_ACK ok=true — server auto-sends LOGIN_ACK next.
          return;
        }

        // ── Normal session frames (post-auth) ─────────────────────────
        if (t === 'MESSAGE') {
          // Incoming chat message from another user via the backend.
          this._emit('message', {
            roomId: frame.room_id || this._currentRoom || 'general',
            author: frame.from || 'unknown',
            text:   frame.text || '',
            seq:    frame.seq,
            self:   frame.from === CURRENT_USER.username,
            sent_at: frame.sent_at,
          });
          // Send ACK back so backend removes it from the outbox.
          this._socket?.send(JSON.stringify({ type: 'ACK', seq: frame.seq }));
          return;
        }
        if (t === 'ACK') {
          // Backend acknowledged OUR message (seq-based).
          this._emit('ack', { clientMsgId: this._pendingSeq?.[frame.seq], status: 'delivered' });
          return;
        }
        if (t === 'ROOM_JOINED') {
          this._currentRoom = frame.room_id;
          this._emit('room_joined', frame);
          return;
        }
        if (t === 'ROOMS_LIST') {
          this._emit('rooms_list', frame);
          return;
        }
        if (t === 'PONG') {
          this._emit('latency', { ms: Date.now() - (this._pingAt || Date.now()) });
          return;
        }
        if (t === 'ERROR') {
          this._emit('error', frame);
          console.warn('[ClusterTalk] backend error:', frame.code, frame.reason);
          
          if (frame.code === 'no_healthy_backend') {
            // All backends are down. Close the socket to trigger exponential
            // backoff reconnects instead of looping in a tight cycle.
            socket.close();
          }
          return;
        }
        if (t === 'DISCONNECT') {
          // FIX #6: distinguish "you were replaced by a newer login" from a
          // real backend/node failure. Previously this ignored frame.reason
          // entirely and always fell through to onclose()'s normal
          // hadAuthenticated=true -> _scheduleReconnect() path. That meant an
          // intentional eviction notice was treated exactly like a crash, so
          // the evicted client immediately reconnected + re-logged-in with the
          // same deterministic session_id -- which evicts whichever connection
          // just replaced it -- forever. Setting this flag lets onclose() tell
          // the two cases apart.
          if (frame.reason === 'logged_in_elsewhere') {
            this._suppressReconnect = true;
            this._evicted = true;
          }
          socket.close();
          return;
        }
        if (t === 'HELLO_ACK') {
          // Legacy reconnect path — treat as connected.
          this._connected = true;
          this._hasAuthenticated = true; // FIX #1: this counts as authenticated too
          this._sessionId = frame.session_id;
          this._emit('connected', { nodeId: frame.session_id?.slice(0, 8) || '?' });
          return;
        }

        // Forward anything else as a lowercased event so the app can
        // handle future frame types without touching the bridge.
        this._emit(t.toLowerCase(), frame);
        };
      }; // proceed() end

      if (silent) {
        const u = localStorage.getItem('ct_username');
        const p = localStorage.getItem('ct_password');
        if (u && p) { proceed(u, p, 'login'); return; }
        // No saved credentials -- fall back to the visible form.
      }
      showLoginForm(proceed);
    });   // Promise end
  }

  /**
   * Schedules a silent auto-reconnect with exponential backoff after an
   * unexpected disconnect (e.g. the LB force-closed us because our
   * backend went down). Reuses saved credentials -- no login prompt.
   * The client keeps retrying until a healthy backend is available again,
   * which is the intended failover behavior.
   */
  _scheduleReconnect() {
    if (this._reconnecting) return;

    this._reconnecting = true;
    const attempt = (this._reconnectAttempts || 0) + 1;
    this._reconnectAttempts = attempt;
    const delay = Math.min(1000 * 2 ** (attempt - 1), 15000);

    this._reconnectTimer = setTimeout(async () => {
      this._reconnectTimer = null;
      try {
        await this.connect({ silent: true });
        this._clearReconnectTimer();
        this._reconnectAttempts = 0; // reset backoff on success
      } catch (e) {
        this._reconnecting = false;
        this._scheduleReconnect(); // still down -- retry with longer delay
      } finally {
        this._reconnecting = false;
      }
    }, delay);
  }

  /**
   * Register a new account then immediately log in.
   * Returns a Promise that resolves true on success.
   */
  async register(username, password) {
    this._authUsername = username;
    this._authPassword = password;
    this._authMode = 'client'; // Registers are always clients
    
    if (!this._socket || this._socket.readyState !== WebSocket.OPEN) {
      // Open socket just for registration.
      await new Promise((resolve, reject) => {
        this._socket = new WebSocket(BRIDGE_URL);
        this._socket.onopen = resolve;
        this._socket.onerror = reject;
      });
    }
    this._socket.send(JSON.stringify({ type: 'REGISTER', username, password }));
  }

  /**
   * Send a chat message to a room.
   * Returns a client-generated msg ID (for matching ACKs in the UI).
   * If the socket isn't currently open, the message is queued locally
   * and automatically resent once the backend reconnects.
   */
  _realSend(roomId, text, attachments) {
    // FIX #4: guard against a dead/closing socket instead of only
    // checking for null. Sending on a CLOSING/CLOSED WebSocket throws
    // (or is silently dropped depending on the browser) -- previously
    // this surfaced only as a console error with no user-facing signal
    // and no record that the message was never actually delivered.
    const seq = this._nextSeq++;
    const clientMsgId = 'c' + Math.random().toString(36).slice(2);
    const sentAt = Date.now() / 1000;
    const item = { roomId, text, attachments, seq, clientMsgId, sentAt };

    if (!this._socket || this._socket.readyState !== WebSocket.OPEN) {
      console.warn('[ClusterTalk] cannot send — socket is not open (state:',
        this._socket ? this._socket.readyState : 'no socket', ')');
      this._sendQueue.push(item);
      showToast('Not connected — message queued and will resend once reconnected', 'danger');
      return clientMsgId;
    }

    this._pendingSeq = this._pendingSeq || {};
    this._pendingSeq[seq] = clientMsgId;
    this._socket.send(JSON.stringify({
      type:    'MESSAGE',
      room_id: roomId,
      text,
      seq,
      sent_at: sentAt,
      ...(attachments && attachments.length ? { attachments } : {}),
    }));
    return clientMsgId;
  }

  _flushSendQueue() {
    if (!this._socket || this._socket.readyState !== WebSocket.OPEN) {
      return;
    }

    this._pendingSeq = this._pendingSeq || {};
    while (this._sendQueue.length > 0) {
      const item = this._sendQueue[0];
      try {
        this._pendingSeq[item.seq] = item.clientMsgId;
        this._socket.send(JSON.stringify({
          type:    'MESSAGE',
          room_id: item.roomId,
          text:    item.text,
          seq:     item.seq,
          sent_at: item.sentAt,
          ...(item.attachments && item.attachments.length ? { attachments: item.attachments } : {}),
        }));
        this._sendQueue.shift();
      } catch (e) {
        // If sending fails mid-drain, leave the remaining items queued
        // until the next successful reconnect.
        break;
      }
    }
  }

  /**
   * Request the list of all active rooms.
   * Backend replies with a ROOMS_LIST frame.
   * Listen with: client.on('rooms_list', ({rooms}) => ...)
   */
  getRooms() {
    this._socket?.send(JSON.stringify({ type: 'GET_ROOMS' }));
  }

  /**
   * Send a PING to measure round-trip latency to the backend.
   * Listen with: client.on('latency', ({ms}) => ...)
   */
  ping() {
    this._pingAt = Date.now();
    this._socket?.send(JSON.stringify({ type: 'PING' }));
  }
}

function wait(ms) { return new Promise((r) => setTimeout(r, ms)); }

// ---------------------------------------------------------------------------
// Application state
// ---------------------------------------------------------------------------

const state = {
  activeRoomId: 'general',
  activeIsDm: false,
  theme: 'dark',
  notifsOn: true,
  leftCollapsed: false,
  rightCollapsed: false,
  messagesByRoom: {},           // roomId -> [message]
  pendingAcks: {},               // clientMsgId -> { roomId, el }
  isNearBottom: true,
  attachments: [],
};

for (const room of ROOMS) {
  state.messagesByRoom[room.id] = [];
}

function hydrateSeedMessage(m) {
  return {
    id: 'seed-' + Math.random().toString(36).slice(2),
    author: m.author,
    self: m.self,
    text: m.text,
    status: m.status || 'delivered',
    ts: Date.now() - m.minutesAgo * 60000,
    attachments: m.attachments || [],
    reactions: {},
  };
}

const client = new ClusterTalkClient();

// ---------------------------------------------------------------------------
// 3. Rendering
// ---------------------------------------------------------------------------

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

function escapeHtml(str) {
  return str.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// Minimal, dependency-free markdown: fenced code blocks, inline code,
// bold, italic, links. Escapes HTML first so nothing user-typed can
// inject markup — code blocks are pulled out before/after escaping via
// placeholders so their contents render literally.
function renderMarkdown(raw) {
  const blocks = [];
  let text = raw.replace(/```(\w*)\n([\s\S]*?)```/g, (_, lang, code) => {
    const idx = blocks.length;
    blocks.push({ lang, code });
    return `\u0000BLOCK${idx}\u0000`;
  });

  text = escapeHtml(text);
  text = text.replace(/`([^`\n]+)`/g, '<code>$1</code>');
  text = text.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
  text = text.replace(/(?<!\*)\*([^*\n]+)\*(?!\*)/g, '<em>$1</em>');
  text = text.replace(/\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g, '<a href="$2" target="_blank" rel="noopener">$1</a>');

  text = text.replace(/\u0000BLOCK(\d+)\u0000/g, (_, idx) => {
    const { lang, code } = blocks[+idx];
    return `<pre>${lang ? `<span class="code-lang">${escapeHtml(lang)}</span>` : ''}<code>${escapeHtml(code.trim())}</code></pre>`;
  });

  return text;
}

function formatTime(ts) {
  return new Date(ts).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
}
function dayLabel(ts) {
  const d = new Date(ts), today = new Date();
  const isSameDay = (a, b) => a.toDateString() === b.toDateString();
  if (isSameDay(d, today)) return 'Today';
  const yest = new Date(today); yest.setDate(today.getDate() - 1);
  if (isSameDay(d, yest)) return 'Yesterday';
  return d.toLocaleDateString([], { month: 'long', day: 'numeric', year: 'numeric' });
}

function avatarHtml(name, size) {
  const cls = size === 'sm' ? 'avatar-sm' : 'avatar';
  return `<div class="${cls}" style="background:${colorForName(name)}">${initialsForName(name)}</div>`;
}

function statusIconHtml(status) {
  if (status === 'sent') return `<span class="msg-status"><svg class="icon icon-sm"><use href="#icon-check"/></svg></span>`;
  if (status === 'delivered') return `<span class="msg-status delivered"><svg class="icon icon-sm"><use href="#icon-check-double"/></svg></span>`;
  if (status === 'read') return `<span class="msg-status read"><svg class="icon icon-sm"><use href="#icon-check-double"/></svg></span>`;
  return '';
}

function renderRoomList() {
  const el = $('#roomList');
  el.innerHTML = ROOMS.map((r) => `
    <div class="nav-item ${!state.activeIsDm && state.activeRoomId === r.id ? 'is-active' : ''}" data-room-id="${r.id}" data-is-dm="false">
      <svg class="icon nav-hash"><use href="#icon-hash"/></svg>
      <span class="nav-item-label">${escapeHtml(r.name)}</span>
      ${r.unread > 0 ? `<span class="unread-badge">${r.unread}</span>` : ''}
    </div>`).join('');
}

function renderDmList() {
  const el = $('#dmList');
  el.innerHTML = '';
}

// The member panel reflects who is ACTUALLY in the conversation: you,
// plus everyone whose message has appeared in the current room. No fake
// roster — a name only shows up once that person has really talked here.
function renderMembers() {
  if (state.activeIsDm) {
    $('#membersPanelTitle').textContent = 'Direct message';
    $('#membersList').innerHTML = '';
    return;
  }
  const msgs = state.messagesByRoom[state.activeRoomId] || [];
  const others = [];
  const seen = new Set();
  msgs.forEach((m) => {
    if (!m.self && m.author && !seen.has(m.author)) { seen.add(m.author); others.push(m.author); }
  });

  const roomName = (ROOMS.find((r) => r.id === state.activeRoomId) || {}).name || state.activeRoomId;
  const total = others.length + 1;   // + you
  $('#membersPanelTitle').textContent = `In #${roomName} — ${total}`;

  const selfRow = `
    <div class="member-row">
      <div class="avatar-sm" style="background:${CURRENT_USER.color}">YOU
        <span class="presence-dot online" style="position:absolute;right:-2px;bottom:-2px;width:7px;height:7px;"></span>
      </div>
      <span class="member-name">You</span>
      <span class="member-role-tag">you</span>
    </div>`;
  const otherRows = others.map((name) => `
    <div class="member-row">
      <div class="avatar-sm" style="background:${colorForName(name)}">${initialsForName(name)}
        <span class="presence-dot online" style="position:absolute;right:-2px;bottom:-2px;width:7px;height:7px;"></span>
      </div>
      <span class="member-name">${escapeHtml(name)}</span>
    </div>`).join('');

  $('#membersList').innerHTML =
    `<div class="member-group-label">IN THIS ROOM — ${total}</div>${selfRow}${otherRows}`;
}

// Signature widget: renders the live mesh topology (full mesh of
// backend nodes) with the node this client is currently connected to
// highlighted and pulsing, and small packets animating along the
// edges that touch it — a direct visual echo of mesh.py's real
// relay-to-every-peer behavior, instead of a generic "status: OK" card.
function renderMeshWidget() {
  const nodes = MESH_NODES;
  const edges = [];
  for (let i = 0; i < nodes.length; i++) {
    for (let j = i + 1; j < nodes.length; j++) edges.push([nodes[i], nodes[j]]);
  }

  const edgeLines = edges.map(([a, b]) =>
    `<line x1="${a.x}" y1="${a.y}" x2="${b.x}" y2="${b.y}" stroke="var(--border-strong)" stroke-width="1.5"/>`
  ).join('');

  const packets = edges
    .filter(([a, b]) => a.id === CONNECTED_NODE_ID || b.id === CONNECTED_NODE_ID)
    .map(([a, b], i) => `
      <circle r="2.6" fill="var(--accent)" class="mesh-packet">
        <animateMotion dur="${2.2 + i * 0.4}s" repeatCount="indefinite"
          path="M${a.x},${a.y} L${b.x},${b.y}" />
      </circle>`).join('');

  const nodeCircles = nodes.map((n) => {
    const isSelf = n.id === CONNECTED_NODE_ID;
    return `
      <g>
        <circle cx="${n.x}" cy="${n.y}" r="${isSelf ? 7 : 5.5}"
          fill="${isSelf ? 'var(--accent)' : 'var(--text-tertiary)'}"
          class="${isSelf ? 'mesh-node-self' : ''}"
          style="${isSelf ? 'filter:drop-shadow(0 0 6px var(--accent-glow));' : ''}"/>
        <text x="${n.x}" y="${n.y + (n.y > 60 ? 20 : -12)}" text-anchor="middle"
          font-size="10" font-family="var(--font-mono)" fill="${isSelf ? 'var(--text-primary)' : 'var(--text-tertiary)'}"
          font-weight="${isSelf ? '700' : '500'}">${n.id}${isSelf ? ' (you)' : ''}</text>
      </g>`;
  }).join('');

  $('#meshSvgWrap').innerHTML = `
    <svg viewBox="0 0 260 132" width="100%" height="100%">
      ${edgeLines}
      ${packets}
      ${nodeCircles}
    </svg>`;
}

function currentRoomMeta() {
  if (state.activeIsDm) {
    const dm = DIRECT_MESSAGES.find((d) => d.id === state.activeRoomId);
    return { name: dm.name, topic: dm.online ? 'Online' : 'Offline', isDm: true };
  }
  const room = ROOMS.find((r) => r.id === state.activeRoomId);
  return { name: room.name, topic: room.topic, isDm: false };
}

function renderHeader() {
  const meta = currentRoomMeta();
  $('#headerRoomName').textContent = meta.name;
  $('#headerTopic').textContent = meta.topic;
  $('#headerRoomName').previousElementSibling.style.display = meta.isDm ? 'none' : '';
  $('#composerInput').placeholder = meta.isDm ? `Message ${meta.name}…` : `Message #${meta.name}…`;
  $('#convoSearchInput').placeholder = meta.isDm ? `Search in ${meta.name}…` : `Search in #${meta.name}…`;
}

function reactionsHtml(reactions) {
  const entries = Object.entries(reactions || {});
  if (!entries.length) return '';
  return `<div class="msg-reactions">${entries.map(([emoji, r]) =>
    `<span class="reaction-chip ${r.reacted ? 'reacted' : ''}" data-emoji="${emoji}">${emoji} ${r.count}</span>`
  ).join('')}</div>`;
}

function messageRowHtml(msg, grouped) {
  const statusHtml = msg.self ? statusIconHtml(msg.status) : '';
  return `
    <div class="message-row ${msg.self ? 'is-self' : ''} ${grouped ? 'grouped' : ''}" data-msg-id="${msg.id}">
      <div class="avatar-slot">
        <span class="msg-hover-time">${formatTime(msg.ts)}</span>
        ${avatarHtml(msg.author, 'full')}
      </div>
      <div class="msg-body">
        <div class="msg-meta">
          <span class="msg-author ${msg.self ? 'is-self' : ''}">${escapeHtml(msg.author)}</span>
          <span class="msg-timestamp">${formatTime(msg.ts)}</span>
        </div>
        <div class="msg-text">${renderMarkdown(msg.text)}${statusHtml}</div>
        ${(msg.attachments || []).map(renderAttachmentHtml).join('')}
        ${reactionsHtml(msg.reactions)}
      </div>
      <div class="msg-hover-actions">
        <button class="icon-btn" data-action="react" title="React"><svg class="icon icon-sm"><use href="#icon-smile"/></svg></button>
        <button class="icon-btn" data-action="reply" title="Reply"><svg class="icon icon-sm"><use href="#icon-reply"/></svg></button>
        <button class="icon-btn" data-action="copy" title="Copy text"><svg class="icon icon-sm"><use href="#icon-copy"/></svg></button>
        ${msg.self ? `<button class="icon-btn" data-action="edit" title="Edit"><svg class="icon icon-sm"><use href="#icon-edit"/></svg></button>
        <button class="icon-btn" data-action="delete" title="Delete"><svg class="icon icon-sm" style="color:var(--danger)"><use href="#icon-trash"/></svg></button>` : ''}
      </div>
    </div>`;
}

function renderAttachmentHtml(att) {
  if (att.kind === 'image') return `<div class="msg-attachment"><img src="${att.url}" alt="${escapeHtml(att.name)}"></div>`;
  return `<div class="msg-file-chip"><svg class="icon"><use href="#icon-file"/></svg><span class="msg-file-name">${escapeHtml(att.name)}</span><span class="msg-file-size">${att.size}</span></div>`;
}

function renderMessageList(animateIn) {
  const msgs = state.messagesByRoom[state.activeRoomId] || [];
  const listEl = $('#messageList');
  let html = '';
  let lastDay = null, lastAuthor = null, lastTs = 0;

  msgs.forEach((msg) => {
    const day = dayLabel(msg.ts);
    if (day !== lastDay) {
      html += `<div class="date-separator">${day}</div>`;
      lastAuthor = null;
    }
    const grouped = lastAuthor === msg.author && (msg.ts - lastTs) < 4 * 60000;
    html += messageRowHtml(msg, grouped);
    lastDay = day; lastAuthor = msg.author; lastTs = msg.ts;
  });

  listEl.innerHTML = html || `<div style="text-align:center;color:var(--text-tertiary);padding:40px 0;font-size:var(--fs-sm);">No messages yet — say hello 👋</div>`;
  if (animateIn) listEl.classList.add('room-transition-in');
  scrollToBottom(false);
}

function scrollToBottom(smooth) {
  const region = $('#chatScrollRegion');
  region.scrollTo({ top: region.scrollHeight, behavior: smooth ? 'smooth' : 'auto' });
  hideNewMessagesPill();
}

function isScrollNearBottom() {
  const region = $('#chatScrollRegion');
  return region.scrollHeight - region.scrollTop - region.clientHeight < 120;
}

function showNewMessagesPill() { $('#newMessagesPill').classList.add('is-visible'); }
function hideNewMessagesPill() { $('#newMessagesPill').classList.remove('is-visible'); }

// ---------------------------------------------------------------------------
// 4. Controllers / event wiring
// ---------------------------------------------------------------------------

function switchRoom(roomId, isDm) {
  if (state.activeRoomId === roomId && state.activeIsDm === isDm) return;
  // Tell the backend to move this session into the room we're switching
  // to, so live broadcasts for it actually reach us (and stop for the
  // room we're leaving). No-op in mock mode. The backend confirms with
  // ROOM_JOINED, which sets the client's _currentRoom for inbound routing.
  client.joinRoom(roomId);
  const listEl = $('#messageList');
  listEl.classList.add('room-transition-out');
  setTimeout(() => {
    state.activeRoomId = roomId;
    state.activeIsDm = isDm;
    if (!isDm) { const r = ROOMS.find((x) => x.id === roomId); if (r) r.unread = 0; }
    renderRoomList(); renderDmList(); renderHeader(); renderMessageList(true); renderMembers();
    listEl.classList.remove('room-transition-out');
  }, 140);

  if (window.innerWidth <= 760) closeMobileNav();
}

function wireRoomAndDmClicks() {
  document.body.addEventListener('click', (e) => {
    const item = e.target.closest('.nav-item[data-room-id]');
    if (!item) return;
    switchRoom(item.dataset.roomId, item.dataset.isDm === 'true');
  });
}

function wireSectionCollapse() {
  $$('.section-header').forEach((header) => {
    header.addEventListener('click', () => {
      $('#' + header.dataset.toggleSection).classList.toggle('collapsed');
    });
  });
}

function wireSidebarCollapse() {
  $('#collapseLeftBtn').addEventListener('click', () => {
    state.leftCollapsed = !state.leftCollapsed;
    $('#appRoot').classList.toggle('left-collapsed', state.leftCollapsed);
  });
  $('#collapseRightBtn').addEventListener('click', () => {
    state.rightCollapsed = !state.rightCollapsed;
    $('#appRoot').classList.toggle('right-collapsed', state.rightCollapsed);
    $('#collapseRightBtn').classList.toggle('is-active', !state.rightCollapsed);
  });
}

function wireMobileNav() {
  $('#mobileMenuBtn').style.display = '';
  $('#mobileMenuBtn').addEventListener('click', () => $('#appRoot').classList.add('mobile-nav-open'));
  $('#mobileScrim').addEventListener('click', closeMobileNav);
}
function closeMobileNav() { $('#appRoot').classList.remove('mobile-nav-open'); }

function wireThemeToggle() {
  $('#themeToggleBtn').addEventListener('click', () => {
    state.theme = state.theme === 'dark' ? 'light' : 'dark';
    document.documentElement.setAttribute('data-theme', state.theme);
    $('#themeIconUse').setAttribute('href', state.theme === 'dark' ? '#icon-moon' : '#icon-sun');
  });
}

function wireNotifToggle() {
  $('#notifToggleBtn').addEventListener('click', () => {
    state.notifsOn = !state.notifsOn;
    $('#notifToggleBtn').classList.toggle('is-active', state.notifsOn);
    $('#notifIconUse').setAttribute('href', state.notifsOn ? '#icon-bell' : '#icon-bell-off');
    showToast(state.notifsOn ? 'Notifications enabled' : 'Notifications muted', 'success');
  });
}

function wireHeaderSearch() {
  $('#headerSearchBtn').addEventListener('click', () => {
    const panel = $('#inConvoSearch');
    const visible = panel.style.display !== 'none';
    panel.style.display = visible ? 'none' : 'block';
    if (!visible) $('#convoSearchInput').focus();
  });
  $('#convoSearchInput').addEventListener('input', (e) => {
    const q = e.target.value.trim().toLowerCase();
    $$('#messageList .message-row').forEach((row) => {
      const text = row.querySelector('.msg-text').textContent.toLowerCase();
      row.style.display = !q || text.includes(q) ? '' : 'none';
    });
  });
}

function wireSidebarSearch() {
  $('#sidebarSearch').addEventListener('input', (e) => {
    const q = e.target.value.trim().toLowerCase();
    $$('.nav-item[data-room-id]').forEach((item) => {
      const label = item.querySelector('.nav-item-label').textContent.toLowerCase();
      item.style.display = !q || label.includes(q) ? '' : 'none';
    });
  });
}

function autoResizeTextarea(el) {
  el.style.height = 'auto';
  el.style.height = Math.min(el.scrollHeight, 200) + 'px';
}

function wireComposer() {
  const input = $('#composerInput');
  const sendBtn = $('#sendBtn');

  function updateSendState() {
    sendBtn.disabled = !input.value.trim() && state.attachments.length === 0;
  }

  input.addEventListener('input', () => { autoResizeTextarea(input); updateSendState(); });
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); trySend(); }
  });
  sendBtn.addEventListener('click', trySend);

  function trySend() {
    const text = input.value.trim();
    if (!text && state.attachments.length === 0) return;
    sendMessage(text, state.attachments);
    input.value = '';
    autoResizeTextarea(input);
    state.attachments = [];
    renderAttachmentPreviews();
    updateSendState();
    sendBtn.classList.add('is-sent');
    setTimeout(() => sendBtn.classList.remove('is-sent'), 400);
  }

  updateSendState();
}

function sendMessage(text, attachments) {
  const msg = {
    id: 'local-' + Math.random().toString(36).slice(2),
    author: CURRENT_USER.name,
    self: true,
    text: text || (attachments.length ? '' : ''),
    status: 'sent',
    ts: Date.now(),
    attachments: attachments.map((a) => ({ ...a })),
    reactions: {},
  };
  state.messagesByRoom[state.activeRoomId].push(msg);
  appendMessageToDom(msg);

  const clientMsgId = client.sendMessage(state.activeRoomId, text, attachments);
  if (clientMsgId) {
    state.pendingAcks[clientMsgId] = { msgId: msg.id };
  } else {
    // FIX #4 follow-through: sendMessage() returned null because the
    // socket wasn't open — surface that instead of leaving the message
    // stuck at a single "sent" checkmark forever with no explanation.
    showToast('Not connected — message will resend once reconnected', 'danger');
  }
}

function appendMessageToDom(msg) {
  const listEl = $('#messageList');
  const msgs = state.messagesByRoom[state.activeRoomId];
  const prev = msgs[msgs.length - 2];
  const grouped = prev && prev.author === msg.author && (msg.ts - prev.ts) < 4 * 60000 && dayLabel(prev.ts) === dayLabel(msg.ts);

  const wasNearBottom = isScrollNearBottom();
  const needsDaySeparator = !prev || dayLabel(prev.ts) !== dayLabel(msg.ts);

  const wrap = document.createElement('div');
  wrap.innerHTML = (needsDaySeparator ? `<div class="date-separator">${dayLabel(msg.ts)}</div>` : '') + messageRowHtml(msg, grouped);
  Array.from(wrap.children).forEach((child) => {
    child.classList.add('msg-enter');
    listEl.appendChild(child);
  });

  if (wasNearBottom || msg.self) scrollToBottom(true);
  else showNewMessagesPill();

  if (!msg.self) renderMembers();   // a new participant may have appeared
}

function updateMessageStatus(localMsgId, status) {
  const msgs = state.messagesByRoom[state.activeRoomId];
  const msg = msgs.find((m) => m.id === localMsgId);
  if (msg) msg.status = status;
  const row = document.querySelector(`.message-row[data-msg-id="${localMsgId}"] .msg-text`);
  if (!row) return;
  const existing = row.querySelector('.msg-status');
  if (existing) existing.outerHTML = statusIconHtml(status);
}

function wireMessageHoverActions() {
  $('#messageList').addEventListener('click', (e) => {
    const chip = e.target.closest('.reaction-chip');
    if (chip) {
      const row = chip.closest('.message-row');
      addReaction(row.dataset.msgId, chip.dataset.emoji);
      return;
    }

    const btn = e.target.closest('[data-action]');
    if (!btn) return;
    const row = btn.closest('.message-row');
    const msgId = row.dataset.msgId;
    const action = btn.dataset.action;
    const textEl = row.querySelector('.msg-text');

    if (action === 'copy') {
      navigator.clipboard?.writeText(textEl.textContent.trim());
      showToast('Message copied', 'success');
    } else if (action === 'reply') {
      $('#composerInput').focus();
      showToast('Replying — thread view coming soon', 'success');
    } else if (action === 'react') {
      e.stopPropagation();
      openEmojiPickerFor(row);
    } else if (action === 'edit') {
      showToast('Editing is not wired up in this demo build', 'success');
    } else if (action === 'delete') {
      const msgs = state.messagesByRoom[state.activeRoomId];
      state.messagesByRoom[state.activeRoomId] = msgs.filter((m) => m.id !== msgId);
      row.remove();
      showToast('Message deleted', 'danger');
    }
  });
}

function addReaction(msgId, emoji) {
  const msgs = state.messagesByRoom[state.activeRoomId];
  const msg = msgs.find((m) => m.id === msgId);
  if (!msg) return;
  msg.reactions = msg.reactions || {};
  const existing = msg.reactions[emoji] || { count: 0, reacted: false };
  if (existing.reacted) {
    existing.count -= 1;
    existing.reacted = false;
    if (existing.count <= 0) delete msg.reactions[emoji];
    else msg.reactions[emoji] = existing;
  } else {
    existing.count += 1;
    existing.reacted = true;
    msg.reactions[emoji] = existing;
  }

  const row = document.querySelector(`.message-row[data-msg-id="${msgId}"] .msg-body`);
  if (!row) return;
  const existingBlock = row.querySelector('.msg-reactions');
  const html = reactionsHtml(msg.reactions);
  if (existingBlock) existingBlock.outerHTML = html;
  else row.insertAdjacentHTML('beforeend', html);
}

function openEmojiPickerFor(row) {
  const popover = $('#emojiPopover');
  popover.dataset.targetMsg = row.dataset.msgId;
  popover.style.display = 'grid';
  popover.classList.add('anim-pop-in');
}

function wireAttachments() {
  const fileInput = $('#fileInput');
  $('#attachBtn').addEventListener('click', () => fileInput.click());
  fileInput.addEventListener('change', () => {
    Array.from(fileInput.files).forEach((file) => {
      const isImage = file.type.startsWith('image/');
      const att = { name: file.name, size: formatBytes(file.size), kind: isImage ? 'image' : 'file', url: isImage ? URL.createObjectURL(file) : null };
      state.attachments.push(att);
    });
    fileInput.value = '';
    renderAttachmentPreviews();
    $('#sendBtn').disabled = false;
  });
}

function formatBytes(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

function renderAttachmentPreviews() {
  const row = $('#attachmentPreviewRow');
  row.innerHTML = state.attachments.map((a, i) => `
    <div class="attachment-chip">
      <div class="thumb">${a.kind === 'image' ? `<img src="${a.url}">` : `<svg class="icon icon-sm"><use href="#icon-file"/></svg>`}</div>
      <span>${escapeHtml(a.name)}</span>
      <button data-idx="${i}" aria-label="Remove attachment"><svg class="icon" style="width:11px;height:11px"><use href="#icon-x"/></svg></button>
    </div>`).join('');
  row.querySelectorAll('button[data-idx]').forEach((btn) => {
    btn.addEventListener('click', () => {
      state.attachments.splice(+btn.dataset.idx, 1);
      renderAttachmentPreviews();
    });
  });
}

function wireEmojiPicker() {
  const popover = $('#emojiPopover');
  popover.innerHTML = EMOJI_SET.map((e) => `<button type="button">${e}</button>`).join('');
  $('#emojiBtn').addEventListener('click', (e) => {
    e.stopPropagation();
    delete popover.dataset.targetMsg; // opened from composer, not a message's react action
    popover.style.display = popover.style.display === 'none' ? 'grid' : 'none';
    popover.classList.add('anim-pop-in');
  });
  popover.addEventListener('click', (e) => {
    const btn = e.target.closest('button');
    if (!btn) return;
    const emoji = btn.textContent;
    if (popover.dataset.targetMsg) {
      addReaction(popover.dataset.targetMsg, emoji);
      delete popover.dataset.targetMsg;
    } else {
      const input = $('#composerInput');
      input.value += emoji;
      input.dispatchEvent(new Event('input'));
      input.focus();
    }
    popover.style.display = 'none';
  });
  document.addEventListener('click', (e) => {
    if (!popover.contains(e.target) && e.target.id !== 'emojiBtn') popover.style.display = 'none';
  });
}

function wireScrollTracking() {
  const region = $('#chatScrollRegion');
  region.addEventListener('scroll', () => {
    state.isNearBottom = isScrollNearBottom();
    if (state.isNearBottom) hideNewMessagesPill();
  });
  $('#newMessagesPill').addEventListener('click', () => scrollToBottom(true));
}

function wireCreateRoomModal() {
  const scrim = $('#roomModalScrim');
  const open = () => { scrim.style.display = 'grid'; $('#roomNameInput').focus(); };
  const close = () => { scrim.style.display = 'none'; $('#roomNameInput').value = ''; $('#roomTopicInput').value = ''; };

  $('#createRoomBtn').addEventListener('click', open);
  $('#roomModalCancelBtn').addEventListener('click', close);
  scrim.addEventListener('click', (e) => { if (e.target === scrim) close(); });

  $('#roomModalCreateBtn').addEventListener('click', () => {
    const name = $('#roomNameInput').value.trim().toLowerCase().replace(/\s+/g, '-');
    if (!name) { showToast('Room name is required', 'danger'); return; }
    const topic = $('#roomTopicInput').value.trim() || 'No topic set yet';
    if (!ROOMS.find((r) => r.id === name)) {
      ROOMS.push({ id: name, name, topic, unread: 0 });
      state.messagesByRoom[name] = [];
      renderRoomList();
    }
    close();
    switchRoom(name, false);
    showToast(`Joined #${name}`, 'success');
  });
}

// ---------------------------------------------------------------------------
// Typing indicator
// ---------------------------------------------------------------------------

let typingTimeout = null;
function showTypingIndicator(author) {
  if (state.activeIsDm || state.activeRoomId !== 'general') return;
  $('#typingText').textContent = `${author} is typing…`;
  $('#typingIndicator').classList.add('is-visible');
  clearTimeout(typingTimeout);
  typingTimeout = setTimeout(() => $('#typingIndicator').classList.remove('is-visible'), 2200);
}

// ---------------------------------------------------------------------------
// Toasts
// ---------------------------------------------------------------------------

function showToast(text, kind) {
  const stack = $('#toastStack');
  const el = document.createElement('div');
  el.className = `toast ${kind === 'danger' ? 'toast-danger' : ''}`;
  el.innerHTML = `<svg class="icon icon-sm"><use href="#icon-${kind === 'danger' ? 'x' : 'check'}"/></svg><span>${escapeHtml(text)}</span>`;
  stack.appendChild(el);
  setTimeout(() => {
    el.classList.add('toast-leaving');
    setTimeout(() => el.remove(), 220);
  }, 3200);
}

// ---------------------------------------------------------------------------
// Ripple effect for buttons
// ---------------------------------------------------------------------------

function wireRipples() {
  document.body.addEventListener('pointerdown', (e) => {
    const btn = e.target.closest('.btn, .icon-btn, .send-btn');
    if (!btn) return;
    const rect = btn.getBoundingClientRect();
    const span = document.createElement('span');
    const size = Math.max(rect.width, rect.height) * 1.4;
    span.className = 'ripple-span';
    span.style.width = span.style.height = size + 'px';
    span.style.left = (e.clientX - rect.left - size / 2) + 'px';
    span.style.top = (e.clientY - rect.top - size / 2) + 'px';
    const prevPos = getComputedStyle(btn).position;
    if (prevPos === 'static') btn.style.position = 'relative';
    btn.appendChild(span);
    setTimeout(() => span.remove(), 600);
  });
}

// ---------------------------------------------------------------------------
// Connection status + node stats
// ---------------------------------------------------------------------------

function setConnectionState(kind) {
  const dot = $('#connectionDot'), text = $('#connectionText');
  dot.classList.remove('reconnecting', 'offline');
  if (kind === 'connected') { text.textContent = 'Connected'; }
  else if (kind === 'reconnecting') { dot.classList.add('reconnecting'); text.textContent = 'Reconnecting…'; }
  else { dot.classList.add('offline'); text.textContent = 'Disconnected'; }
}

function wireClientEvents() {
  client.on('disconnected', () => {
    setConnectionState(client._evicted ? 'offline' : 'reconnecting');
  });

  client.on('connected', ({ nodeId }) => {
    CONNECTED_NODE_ID = nodeId || CONNECTED_NODE_ID;
    setConnectionState('connected');
    // Join the room the UI is currently showing so the backend session's
    // room matches the view and its broadcasts reach us from the start.
    client.joinRoom(state.activeRoomId);
  });

  client.on('latency', ({ ms }) => {
    const el = $('#statLatency');
    if (!el) return;
    el.textContent = `${ms} ms`;
    el.className = 'stat-value ' + (ms < 40 ? 'good' : ms < 90 ? 'warn' : 'bad');
  });

  client.on('ack', ({ clientMsgId, status }) => {
    const pending = state.pendingAcks[clientMsgId];
    if (pending) updateMessageStatus(pending.msgId, status);
  });

  client.on('typing', ({ author }) => showTypingIndicator(author));

  client.on('message', ({ roomId, author, text, sent_at }) => {
    const msg = {
      id: 'in-' + Math.random().toString(36).slice(2),
      author,
      self: author === CURRENT_USER.username,
      text,
      status: 'delivered',
      ts: sent_at != null ? sent_at * 1000 : Date.now(),
      attachments: [],
      reactions: {},
    };
    state.messagesByRoom[roomId] = state.messagesByRoom[roomId] || [];
    state.messagesByRoom[roomId].push(msg);
    if (state.activeRoomId === roomId && !state.activeIsDm) appendMessageToDom(msg);
    else if (!state.activeIsDm) {
      const room = ROOMS.find((r) => r.id === roomId);
      if (room) { room.unread += 1; renderRoomList(); }
    }
    if (state.notifsOn && roomId !== state.activeRoomId) showToast(`${author}: ${text}`, 'success');
  });
}

// ---------------------------------------------------------------------------
// 5. Boot sequence
// ---------------------------------------------------------------------------


// ---------------------------------------------------------------------------
// Login Form — shown instead of prompt() when MOCK_MODE = false
// ---------------------------------------------------------------------------

function injectLoginFormHTML() {
  const div = document.createElement('div');
  div.id = 'loginOverlay';
  div.innerHTML = `
    <div id="loginBox">
      <div id="loginLogo"><img src="logo.png" onerror="this.onerror=null;this.src='logo.svg'" alt="ClusterTalk" /></div>
      <h2 id="loginTitle">ClusterTalk</h2>
      <p id="loginSubtitle">Distributed mesh chat</p>
      <div id="loginTabs">
        <button type="button" id="tabSignin">Client</button>
        <button type="button" id="tabRegister">Create Account</button>
        <button type="button" id="tabAdmin">Admin</button>
      </div>
      <div id="loginError" style="display:none"></div>
      <input id="loginUsername" type="text"     placeholder="Username (min 3 chars)" autocomplete="username" />
      <input id="loginPassword" type="password" placeholder="Password (min 6 chars)" autocomplete="current-password" />
      <button id="loginBtn">Sign In</button>
      <p id="loginHint">Welcome back — sign in to continue.</p>
    </div>`;
  document.body.appendChild(div);

  const style = document.createElement('style');
  style.textContent = `
    #loginOverlay {
      position: fixed; inset: 0; z-index: 9999;
      background: var(--bg-primary, #0f1117);
      display: flex; align-items: center; justify-content: center;
    }
    #loginBox {
      background: var(--bg-secondary, #1a1d27);
      border: 1px solid var(--border-subtle, #2a2d3a);
      border-radius: 16px; padding: 40px 36px;
      width: 100%; max-width: 380px;
      display: flex; flex-direction: column; gap: 14px;
      box-shadow: 0 24px 64px rgba(0,0,0,0.5);
    }
    #loginLogo {
      width: 60px; height: 60px;
      display: flex; align-items: center; justify-content: center;
      margin-bottom: 4px;
    }
    #loginLogo img { width: 100%; height: 100%; object-fit: contain;
      filter: drop-shadow(0 4px 16px rgba(34,211,238,0.35)); }
    #loginTitle { color: var(--text-primary, #fff); font-size: 22px; font-weight: 700; margin: 0; }
    #loginSubtitle { color: var(--text-tertiary, #888); font-size: 13px; margin: 0 0 6px; }
    #loginTabs { display: flex; gap: 5px; background: var(--bg-tertiary, #12151e);
      padding: 4px; border-radius: 11px; border: 1px solid var(--border-subtle, #2a2d3a); }
    #loginTabs button { flex: 1; background: transparent; border: none; cursor: pointer;
      font-family: inherit; font-size: 13px; font-weight: 600; padding: 9px; border-radius: 8px;
      color: var(--text-tertiary, #8a90a2); transition: all .15s; }
    #loginTabs button.active { background: var(--accent, #6366f1); color: #fff; box-shadow: 0 2px 8px rgba(99,102,241,.35); }
    #loginAdminLink { color: var(--text-tertiary, #888); font-size: 12px; text-align: center;
      text-decoration: none; margin-top: 2px; transition: color .15s; }
    #loginAdminLink:hover { color: var(--accent, #7c7fff); }
    #loginError {
      background: rgba(239,68,68,0.15); border: 1px solid rgba(239,68,68,0.4);
      color: #f87171; border-radius: 8px; padding: 10px 14px; font-size: 13px;
    }
    #loginUsername, #loginPassword {
      background: var(--bg-tertiary, #12151e);
      border: 1px solid var(--border-subtle, #2a2d3a);
      border-radius: 10px; color: var(--text-primary, #fff);
      font-size: 14px; padding: 11px 14px; outline: none; transition: border-color .15s;
    }
    #loginUsername:focus, #loginPassword:focus { border-color: var(--accent, #6366f1); }
    #loginBtn {
      background: var(--accent, #6366f1); color: #fff; border: none;
      border-radius: 10px; padding: 12px; font-size: 15px; font-weight: 600;
      cursor: pointer; margin-top: 4px; transition: opacity .15s;
    }
    #loginBtn:hover { opacity: 0.88; }
    #loginBtn:disabled { opacity: 0.5; cursor: not-allowed; }
    #loginHint { color: var(--text-tertiary, #888); font-size: 12px; text-align: center; margin: 0; }
  `;
  document.head.appendChild(style);
}

let _loginCallback = null;
let _loginMode = 'login';   // 'login' | 'register' | 'admin'

function setLoginMode(mode) {
  _loginMode = mode;
  const si = document.getElementById('tabSignin');
  const rg = document.getElementById('tabRegister');
  const ad = document.getElementById('tabAdmin');
  const btn = document.getElementById('loginBtn');
  const hint = document.getElementById('loginHint');
  if (si) si.classList.toggle('active', mode === 'login');
  if (rg) rg.classList.toggle('active', mode === 'register');
  if (ad) ad.classList.toggle('active', mode === 'admin');
  if (btn) btn.textContent = mode === 'register' ? 'Create Account' : (mode === 'admin' ? 'login' : 'login');
  if (hint) hint.textContent = mode === 'register'
    ? 'Choose a username & password \u2014 your client account is created instantly.'
    : (mode === 'admin' ? 'Use an administrator account to open the dashboard.' : 'Sign in to join the conversation.');
  hideLoginError();
}

function showLoginForm(callback) {
  _loginCallback = callback;
  if (!document.getElementById('loginOverlay')) {
    injectLoginFormHTML();
  }
  const overlay = document.getElementById('loginOverlay');
  overlay.style.display = 'flex';

  const btn    = document.getElementById('loginBtn');
  const userEl = document.getElementById('loginUsername');
  const passEl = document.getElementById('loginPassword');

  userEl.value = localStorage.getItem('ct_username') || '';
  passEl.value = localStorage.getItem('ct_password') || '';

  // Returning users default to Sign In; brand-new visitors to Create Account.
  setLoginMode(userEl.value ? 'login' : 'register');
  document.getElementById('tabSignin').onclick = () => setLoginMode('login');
  document.getElementById('tabRegister').onclick = () => setLoginMode('register');
  document.getElementById('tabAdmin').onclick = () => setLoginMode('admin');

  function doLogin() {
    const username = userEl.value.trim();
    const password = passEl.value;
    if (username.length < 3) { showLoginError('Username must be at least 3 characters.'); userEl.focus(); return; }
    if (password.length < 6) { showLoginError('Password must be at least 6 characters.'); passEl.focus(); return; }
    btn.disabled = true;
    btn.textContent = _loginMode === 'register' ? 'Creating\u2026' : 'Signing in\u2026';
    hideLoginError();
    _loginCallback(username, password, _loginMode);
  }

  btn.onclick = doLogin;
  passEl.onkeydown = (e) => { if (e.key === 'Enter') doLogin(); };
  userEl.onkeydown = (e) => { if (e.key === 'Enter') passEl.focus(); };
  setTimeout(() => userEl.focus(), 50);
}

function hideLoginForm() {
  const overlay = document.getElementById('loginOverlay');
  if (overlay) overlay.style.display = 'none';
}

function showLoginError(msg) {
  const errEl = document.getElementById('loginError');
  const btn   = document.getElementById('loginBtn');
  if (errEl) { errEl.textContent = msg; errEl.style.display = 'block'; }
  if (btn)   { btn.disabled = false; btn.textContent = _loginMode === 'register' ? 'Create Account' : (_loginMode === 'admin' ? 'login' : 'login'); }
}

function hideLoginError() {
  const errEl = document.getElementById('loginError');
  if (errEl) errEl.style.display = 'none';
}

async function boot() {
  renderRoomList();
  renderDmList();
  // No fake DMs — hide the Direct Messages section entirely.
  const dmSec = document.getElementById('dmSection');
  if (dmSec) dmSec.style.display = 'none';
  renderMembers();
  renderHeader();
  renderMessageList(false);

  wireRoomAndDmClicks();
  wireSectionCollapse();
  wireSidebarCollapse();
  if (window.innerWidth <= 760) wireMobileNav();
  wireThemeToggle();
  wireNotifToggle();
  wireHeaderSearch();
  wireSidebarSearch();
  wireComposer();
  wireMessageHoverActions();
  wireAttachments();
  wireEmojiPicker();
  wireScrollTracking();
  wireCreateRoomModal();
  wireRipples();
  wireClientEvents();

  setConnectionState('reconnecting');
  $('#bootText').textContent = 'Negotiating handshake with node-A…';
  try {
    await client.connect();
  } catch {
    // The login overlay remains visible with its specific error message.
    // A subsequent user action (or a reconnect after a live session drops)
    // starts a fresh, independent connection attempt.
    return;
  }
  $('#bootText').textContent = 'Connected.';
  await wait(250);
  $('#bootScreen').classList.add('is-hidden');
}

document.addEventListener('DOMContentLoaded', boot);