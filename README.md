<<<<<<< HEAD
# ClusterTalk

A distributed, real-time chat system built on a **stateful proxy-mesh
architecture**. Browser clients talk to a load balancer over a WebSocket
=======
<div align="center">

# 🔗 ClusterTalk

**A distributed, real-time chat system built on a stateful proxy-mesh architecture.**

![Python](https://img.shields.io/badge/Python-3.13-blue?logo=python&logoColor=white)
![WebSockets](https://img.shields.io/badge/Protocol-WebSocket_%2B_TCP-4B8BBE)
![SQLite](https://img.shields.io/badge/Persistence-SQLite_(WAL)-003B57?logo=sqlite&logoColor=white)
![License](https://img.shields.io/badge/Status-Active-brightgreen)

[Architecture](#architecture) · [Setup](#setup) · [Run](#run-everything-one-command) · [Admin Dashboard](#admin-dashboard) · [Tests](#tests)

</div>

Browser clients talk to a load balancer over a WebSocket
>>>>>>> 2f0032574c3e705f77cf85092b6b18abdacaff52
bridge; the LB routes them (with sticky sessions) to a mesh of chat nodes
that relay room messages to each other and survive node failure with
zero message loss.

```
 Browser ──WS──▶ ws_bridge ──TCP──▶ load balancer ──TCP──▶ chat node ─┐
 (frontend)      (:8080)            (:9000)                 (:8765)    │ full mesh
                                                                       ▼
                                                            peer node ◀┘
```

## Architecture

| Layer | Path | Responsibility |
|-------|------|----------------|
| **Protocol** | `protocol/framing.py` | Length-prefixed binary wire format: `[4B length][1B type][orjson payload]` |
| **Node** | `node/server.py` | Auth (REGISTER/LOGIN), rooms, message broadcast, ACK/retry, exactly-once delivery |
| **Session state** | `node/session_state.py` | Per-connection seq numbers, outbox buffering, inbound dedup |
| **Persistence** | `node/persistence.py` | Crash-safe SQLite (WAL) store for sessions, outbox, users, rooms |
| **Session manager** | `node/session_manager.py` | Ties in-memory state to persistence + cross-node recovery |
| **Mesh** | `node/mesh.py` | Node-to-node RELAY (with loop prevention) + cross-node session recovery |
| **Ingress / LB** | `ingress/lb.py` | Sticky-session routing, health checks, dynamic node registration |
| **Bridge** | `ingress/ws_bridge.py` | Translates browser WebSocket JSON ⇄ backend binary TCP frames |
| **Admin API** | `ingress/admin_server.py` | HTTP/JSON view of the LB's live state (`/api/stats`, `/api/simulate`) |
| **Frontend** | `frontend/` | Vanilla-JS chat UI (`index.html`) + admin dashboard (`admin.html`) |

## Requirements

- Python 3.13 (3.11+ should work)
- D
ependencies in `requirements.txt` (`orjson`, `websockets`; `redis` optional)

## Setup

```bash
# from the ClusterTalk/ directory
python -m venv .venv
.venv\Scripts\activate            # Windows
# source .venv/bin/activate       # macOS/Linux
python -m pip install -r requirements.txt
```

## Run everything (one command)

```bash
python run_stack.py --open
```

This boots the node, load balancer, WebSocket bridge, and a static server
for the frontend, then prints the URL (default
<http://127.0.0.1:5500/index.html>). Sign in with **any** username (3+
chars) and password (6+ chars) — a new account is created automatically on
first sign-in, and the same credentials log you back in afterward.

Boot a 3-node mesh instead (rooms span all three nodes):

```bash
python run_stack.py --mesh --open
```

Press **Ctrl+C** to stop the whole stack.

## Admin dashboard

<<<<<<< HEAD
Sign in through **Admin Login** at
**<http://127.0.0.1:5500/index.html>** to open the live load-balancer control
board. Configure its bootstrap credentials before starting the stack:

```powershell
$env:CLUSTERTALK_ADMIN_USERNAME = 'admin'
$env:CLUSTERTALK_ADMIN_PASSWORD = '123456'
python run_stack.py --mesh --open
```

The admin account is created only when both variables are present; client
registration can never create an administrator. The dashboard lets you:
=======
Open **<http://127.0.0.1:5500/admin.html>** (or click the server icon in the
chat sidebar footer) for a live load-balancer control board where you can:
>>>>>>> 2f0032574c3e705f77cf85092b6b18abdacaff52

- **See active servers** — how many chat nodes are up (healthy / total), each
  with its address, static/dynamic tag, and requests it has handled.
- **Add / remove servers** — "＋ Add Server" launches a real chat node that
  registers itself with the LB; "✕ Remove" stops a node you added.
- **Take a server up / down** — pull a node out of rotation and watch traffic
  shift to the rest (failover), then bring it back.
- **Connected clients (sticky + failover/failback)** — connect a pool of clients
  that each stick to a server. Take a server down and its clients **fail over**
  evenly to the others; bring it back and they **fail back** to it.
- **Simulate traffic** — fire a burst of N clients and watch the LB spread them
  round-robin across the healthy servers (paused/down servers get **0**).
- **Traffic distribution** — a live bar chart of the split across servers.
- **How it works** — sticky-first, then round-robin; health checks every 3s;
  dynamic register/deregister; even split under load.

All numbers are **real**, read from and controlled through the load balancer's
admin API (`http://127.0.0.1:9200`). Run with `--mesh` to start with three
servers. The admin port is configurable: `run_lb.py --admin-port 9200` (`0`
disables it); server add/remove needs `--register-port` (the launcher sets it).

## Accounts

<<<<<<< HEAD
The login screen has **Client Login**, **Create Account**, and **Admin Login**
options. New users pick a username (3+ chars) and password (6+ chars) on the
Create Account tab; returning users use Client Login. Passwords are stored
hashed (PBKDF2-HMAC-SHA256). The chat shows
=======
The login screen has **Sign In** and **Create Account** tabs. New users pick a
username (3+ chars) and password (6+ chars) on the Create Account tab; returning
users sign in. Passwords are stored hashed (PBKDF2-HMAC-SHA256). The chat shows
>>>>>>> 2f0032574c3e705f77cf85092b6b18abdacaff52
**only real participants** — the member list is built from who has actually
talked in the room, with no placeholder users or seeded messages.

## Run components manually

```bash
# 1. a chat node (client port 8765)
cd node && python run_node.py --port 8765 --db clustertalk.db

# 2. the load balancer in front of it (port 9000)
cd ingress && python run_lb.py --backend 127.0.0.1:8765

# 3. the browser bridge (WebSocket 8080 -> LB 9000)
cd ingress && python ws_bridge.py --tcp-port 9000 --ws-port 8080

# 4. serve the frontend and open frontend/index.html
cd frontend && python -m http.server 5500
```

The frontend connects to `ws://localhost:8080/bridge` (`BRIDGE_URL` in
`frontend/app.js`). Set `MOCK_MODE = true` in that file to preview the UI
with canned data and no backend.

## Tests

```bash
python run_tests.py
```

Runs all nine suites (protocol, session state/persistence/manager, node
integration, mesh, cross-node recovery, dynamic registration, LB). Each is
a standalone script — you can also run any single one directly, e.g.
`cd node && python test_server.py`.

## Branding

The app ships with `frontend/logo.svg` (a hexagonal mesh-cluster mark). To use
your own logo instead, drop a **`frontend/logo.png`** — the login screen, chat
sidebar, boot screen, and admin header all prefer `logo.png` and fall back to
the SVG automatically, so no code change is needed.

## Troubleshooting

- **Blank screen / old UI after an edit?** Your browser cached a stale
  `style.css`/`app.js`. `run_stack.py` serves the frontend with caching
  disabled, so a normal reload picks up changes — but if you opened the page
  before that, do a one-time hard refresh (**Ctrl+Shift+R**, or Cmd+Shift+R
  on macOS). If you serve the frontend with a different static server, the
  same hard-refresh applies.
- **`no_healthy_backend` at the login screen?** The node/LB aren't up. Start
  the whole stack with `python run_stack.py` rather than opening
  `index.html` on its own.

## Notes & known limitations

<<<<<<< HEAD
- **Auth is cluster-wide.** `run_stack.py --mesh` passes every node the same
  `clustertalk-auth.db`, so a reconnect after failover can authenticate on any
  healthy node. For multi-host production deployments, replace that SQLite
  path with a central identity-store adapter; do not place SQLite on an
  unreliable network share. Room membership is a single room per session (the
  active view); the mesh broadcasts that room across nodes.
=======
- **Auth is per-node.** A user registered on node A can only log in on node
  B if the nodes share the same SQLite file. Room membership is a single
  room per session (the active view); the mesh broadcasts that room across
  nodes.
>>>>>>> 2f0032574c3e705f77cf85092b6b18abdacaff52
- **Redis is optional.** Without it (or without a reachable server), the LB
  keeps sticky sessions in an in-memory cache and logs a warning instead of
  failing.
- Message delivery is **at-least-once on the wire, exactly-once in
  processing** (seq + ACK + inbound dedup), and unacked messages survive a
  node crash (persisted outbox) or a client reconnect (replay).
<<<<<<< HEAD
=======

---

<div align="center">
Built with ❤️ InoTech Solutions (Pvt) Ltd Internship
</div>

>>>>>>> 2f0032574c3e705f77cf85092b6b18abdacaff52
