"""
ClusterTalk — Admin API server.

A tiny HTTP+JSON server that exposes the Load Balancer's live state to the
admin dashboard (frontend/admin.html) and lets an operator control the
cluster. It runs in a background thread in the SAME process as the LB, so
it can read/drive the LB's in-memory state directly (the LB's admin_*
methods are thread-safe).

Endpoints (all CORS-enabled — the dashboard is served from a different port):

    GET  /api/stats                        -> live snapshot of the LB
    POST /api/simulate?n=50                -> route N synthetic clients
    POST /api/servers/toggle?address=&down=1  -> take a server out of / into rotation
    POST /api/servers/add                  -> spawn a NEW chat node that registers
    POST /api/servers/remove?address=      -> stop a node the dashboard spawned
    OPTIONS *                              -> CORS preflight (204)

The "add server" feature launches a real `run_node.py` subprocess that
dynamically registers with the LB — so the new node genuinely joins the
cluster and starts receiving traffic. Requires the LB to have a
registration port (run_lb.py --register-port); without it, add returns an
error.

Dependency-free (stdlib only).
"""

# ingress/admin_server.py
from __future__ import annotations

import atexit
import http.server
import json
import logging
import socket
import subprocess
import sys
import threading
import urllib.parse
from pathlib import Path

logger = logging.getLogger("clustertalk.admin")


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class NodeSpawner:
    """
    Launches and tracks extra chat-node processes that register themselves
    with the LB, so the admin can scale the cluster up and down live.
    """

    def __init__(self, lb, python_exe: str, node_dir: str,
                 register_host: str, register_port: int | None):
        self._lb = lb
        self._python = python_exe
        self._node_dir = node_dir
        self._register_host = register_host if register_host not in ("0.0.0.0", "", None) else "127.0.0.1"
        self._register_port = register_port
        self._procs: dict[str, subprocess.Popen] = {}   # address -> process
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return self._register_port is not None

    def addresses(self) -> set[str]:
        with self._lock:
            return set(self._procs.keys())

    def add(self) -> dict:
        if not self.enabled:
            return {"ok": False, "reason": "LB has no registration port; cannot spawn nodes"}
        port = _free_port()
        address = f"127.0.0.1:{port}"
        cmd = [
            self._python, "-u", "run_node.py",
            "--host", "127.0.0.1", "--port", str(port),
            "--advertise-host", "127.0.0.1",
            "--lb-register-host", self._register_host,
            "--lb-register-port", str(self._register_port),
            "--db", f"admin_node_{port}.db",
        ]
        try:
            proc = subprocess.Popen(cmd, cwd=self._node_dir)
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "reason": f"failed to launch node: {exc}"}
        with self._lock:
            self._procs[address] = proc
        logger.info("admin spawned new node at %s (pid %s)", address, proc.pid)
        return {"ok": True, "address": address,
                "note": "node launching — it will appear once it registers"}

    def remove(self, address: str) -> dict:
        with self._lock:
            proc = self._procs.pop(address, None)
        if proc is None:
            return {"ok": False, "reason": "not an admin-managed server (use Down instead)"}
        try:
            proc.terminate()
        except Exception:  # noqa: BLE001
            pass
        # Drop it from the LB pool immediately rather than waiting for the
        # registration socket to time out.
        self._lb.admin_remove_backend(address)
        logger.info("admin removed node at %s", address)
        return {"ok": True, "address": address}

    def cleanup(self) -> None:
        with self._lock:
            procs = list(self._procs.values())
            self._procs.clear()
        for proc in procs:
            try:
                proc.terminate()
            except Exception:  # noqa: BLE001
                pass


def make_handler(lb, spawner: NodeSpawner | None):
    class AdminHandler(http.server.BaseHTTPRequestHandler):
        # ── helpers ──────────────────────────────────────────────────────
        def _cors(self):
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.send_header("Cache-Control", "no-store")

        def _send_json(self, obj, status=200):
            body = json.dumps(obj).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self._cors()
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _query(self):
            return urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)

        def _stats(self):
            snap = lb.admin_snapshot()
            managed = spawner.addresses() if spawner else set()
            snap["can_add_servers"] = bool(spawner and spawner.enabled)
            for s in snap["servers"]:
                s["removable"] = s["address"] in managed
            return snap

        # ── routes ───────────────────────────────────────────────────────
        def do_OPTIONS(self):
            self.send_response(204)
            self._cors()
            self.end_headers()

        def do_GET(self):
            path = urllib.parse.urlparse(self.path).path
            if path in ("/api/stats", "/stats"):
                self._send_json(self._stats())
            elif path in ("/", "/api", "/api/health"):
                self._send_json({"ok": True, "service": "clustertalk-admin"})
            else:
                self._send_json({"error": "not_found"}, status=404)

        def do_POST(self):
            path = urllib.parse.urlparse(self.path).path
            q = self._query()
            if path in ("/api/simulate", "/simulate"):
                try:
                    n = int(q.get("n", ["50"])[0])
                except (ValueError, TypeError):
                    n = 50
                self._send_json(lb.admin_simulate(max(1, min(n, 1000))))
            elif path == "/api/servers/toggle":
                address = q.get("address", [""])[0]
                down = q.get("down", ["1"])[0] in ("1", "true", "True", "yes")
                self._send_json(lb.admin_set_down(address, down))
            elif path == "/api/servers/add":
                self._send_json(spawner.add() if spawner else
                                {"ok": False, "reason": "spawning disabled"})
            elif path == "/api/servers/remove":
                address = q.get("address", [""])[0]
                self._send_json(spawner.remove(address) if spawner else
                                {"ok": False, "reason": "spawning disabled"})
            elif path == "/api/clients/add":
                try:
                    n = int(q.get("n", ["30"])[0])
                except (ValueError, TypeError):
                    n = 30
                self._send_json(lb.admin_add_clients(n))
            elif path == "/api/clients/clear":
                self._send_json(lb.admin_clear_clients())
            else:
                self._send_json({"error": "not_found"}, status=404)

        def log_message(self, *args):   # keep the console quiet
            return

    return AdminHandler


def start_admin_server(
    lb, host: str, port: int,
    register_host: str = "0.0.0.0", register_port: int | None = None,
) -> http.server.ThreadingHTTPServer:
    """
    Start the admin API on host:port in a daemon thread. If register_port
    is provided, the dashboard can also spawn/kill real chat nodes.
    """
    spawner = None
    if register_port is not None:
        node_dir = str(Path(__file__).resolve().parent.parent / "node")
        spawner = NodeSpawner(lb, sys.executable, node_dir, register_host, register_port)
        atexit.register(spawner.cleanup)

    httpd = http.server.ThreadingHTTPServer((host, port), make_handler(lb, spawner))
    thread = threading.Thread(target=httpd.serve_forever, name="admin-http", daemon=True)
    thread.start()
    logger.info("Admin API listening on http://%s:%s/api/stats%s",
                host, port, "  (server add/remove enabled)" if spawner else "")
    return httpd
