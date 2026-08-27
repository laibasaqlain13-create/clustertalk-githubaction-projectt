"""
ClusterTalk — one-command full-stack launcher.

Starts every process the app needs and serves the frontend, so the whole
thing comes up with a single command:

    python run_stack.py

It boots, in order:

    1. Chat node   (node/run_node.py)        TCP  127.0.0.1:8765
    2. Load balancer (ingress/run_lb.py)     TCP  127.0.0.1:9000  -> node
    3. WebSocket bridge (ingress/ws_bridge.py)  WS 127.0.0.1:8080  -> LB
    4. Static file server for the frontend      HTTP 127.0.0.1:5500

The browser frontend (frontend/app.js) connects to ws://localhost:8080/bridge,
which the bridge proxies to the LB, which routes to the node.

Architecture:

    Browser ──WS──> Bridge(8080) ──TCP──> LB(9000) ──TCP──> Node(8765)

Press Ctrl+C to stop everything cleanly.

Options:
    python run_stack.py --open        also open the app in your browser
    python run_stack.py --mesh        boot a 3-node mesh behind the LB
                                      (rooms span all three nodes)
"""
# run_stack.py
from __future__ import annotations

import argparse
import functools
import http.server
import os
import socketserver
import subprocess
import sys
import threading
import time
import webbrowser
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PY = sys.executable  # the interpreter running this script (ideally the venv's)

NODE_HOST = "127.0.0.1"
NODE_PORT = 8765
LB_PORT = 9000
BRIDGE_WS_PORT = 8090
FRONTEND_HOST = "0.0.0.0"
FRONTEND_PORT = int(os.environ.get("PORT", 8080))

_procs: list[tuple[str, subprocess.Popen]] = []


def _spawn(name: str, args: list[str], cwd: Path) -> subprocess.Popen:
    print(f"[stack] starting {name:7s}: {Path(args[0]).name} {' '.join(args[1:])}")
    proc = subprocess.Popen([PY, "-u", *args], cwd=str(cwd))
    _procs.append((name, proc))
    return proc


class _NoCacheHandler(http.server.SimpleHTTPRequestHandler):
    """Serves the frontend with caching disabled, so edits to app.js/CSS
    are always picked up on reload instead of the browser silently serving
    a stale cached copy (which otherwise makes a fixed bug still look
    broken until a manual hard-refresh)."""

    def end_headers(self):
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def log_message(self, *args):  # quieter console
        pass


def _serve_frontend() -> socketserver.TCPServer:
    handler = functools.partial(_NoCacheHandler, directory=str(ROOT / "frontend"))
    # allow_reuse_address avoids "address already in use" on quick restarts
    socketserver.TCPServer.allow_reuse_address = True
    httpd = socketserver.TCPServer((FRONTEND_HOST, FRONTEND_PORT), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd


def _boot_single_node() -> None:
    _spawn(
        "node",
        ["run_node.py", "--host", NODE_HOST, "--port", str(NODE_PORT), "--db", "clustertalk.db"],
        cwd=ROOT / "node",
    )
    time.sleep(1.5)
    _spawn(
        "lb",
        ["run_lb.py", "--host", NODE_HOST, "--port", str(LB_PORT),
         "--backend", f"{NODE_HOST}:{NODE_PORT}",
         # register listener so the admin dashboard can spawn extra nodes
         "--register-port", "9100"],
        cwd=ROOT / "ingress",
    )
    time.sleep(1.0)


def _boot_mesh() -> None:
    """3 meshed nodes, all registering with the LB dynamically."""
    nodes = [
        # (client_port, mesh_port, node_id, peer_mesh_ports, db)
        (8765, 9765, "A", [9766, 9767], "a.db"),
        (8766, 9766, "B", [9765, 9767], "b.db"),
        (8767, 9767, "C", [9765, 9766], "c.db"),
    ]
    # LB with dynamic registration listener; nodes announce themselves.
    _spawn(
        "lb",
        ["run_lb.py", "--host", NODE_HOST, "--port", str(LB_PORT), "--register-port", "9100"],
        cwd=ROOT / "ingress",
    )
    time.sleep(1.0)
    auth_db = str(ROOT / "clustertalk-auth.db")
    for client_port, mesh_port, node_id, peers, db in nodes:
        args = ["run_node.py", "--host", NODE_HOST, "--port", str(client_port),
                "--mesh-port", str(mesh_port), "--node-id", node_id, "--db", db,
                "--auth-db", auth_db,
                "--advertise-host", NODE_HOST,
                "--lb-register-host", NODE_HOST, "--lb-register-port", "9100"]
        for p in peers:
            args += ["--peer", f"{NODE_HOST}:{p}"]
        _spawn(f"node-{node_id}", args, cwd=ROOT / "node")
        time.sleep(0.4)
    time.sleep(1.0)


def _shutdown() -> None:
    print("\n[stack] shutting down...")
    for name, proc in reversed(_procs):
        if proc.poll() is None:
            proc.terminate()
    deadline = time.time() + 5
    for name, proc in reversed(_procs):
        remaining = max(0.0, deadline - time.time())
        try:
            proc.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            proc.kill()
    print("[stack] all processes stopped.")


def _entry_url(use_mock: bool = False) -> str:
    suffix = "?mock=1" if use_mock else ""
    return f"http://{NODE_HOST}:{FRONTEND_PORT}/index.html{suffix}"


def _check_primary_boot() -> bool:
    dead = [name for name, proc in _procs if proc.poll() is not None]
    if dead:
        print("[stack] primary services exited during startup; switching to mock fallback mode")
        return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full ClusterTalk stack")
    parser.add_argument("--open", action="store_true", help="open the app in a browser")
    parser.add_argument("--mesh", action="store_true", help="boot a 3-node mesh")
    args = parser.parse_args()

    print("=" * 64)
    print("  ClusterTalk — full stack launcher")
    print("=" * 64)

    try:
        if args.mesh:
            _boot_mesh()
        else:
            _boot_single_node()

        # WebSocket bridge -> LB
        _spawn(
            "bridge",
            ["ws_bridge.py", "--ws-host", NODE_HOST, "--ws-port", str(BRIDGE_WS_PORT),
             "--tcp-host", NODE_HOST, "--tcp-port", str(LB_PORT)],
            cwd=ROOT / "ingress",
        )
        time.sleep(0.8)
        use_mock = not _check_primary_boot()
    except Exception as exc:
        print(f"[stack] failed to start primary stack: {exc}")
        use_mock = True

    _serve_frontend()
    url = _entry_url(use_mock=use_mock)
    admin_url = f"http://{NODE_HOST}:{FRONTEND_PORT}/admin.html"

    print("=" * 64)
    if use_mock:
        print("  ClusterTalk is running in mock fallback mode.")
        print(f"    User chat : {url}")
        print("    The UI will use mock data because the backend stack could not start cleanly.")
    else:
        print("  ClusterTalk is UP.")
        print(f"    User chat : {url}")
        print(f"    Admin board: {admin_url}  (load balancer live stats)")
        print("  Sign in with any username (3+ chars) and password (6+ chars);")
        print("  a new account is created automatically on first sign-in.")
        print("  Tip: run with --mesh to see 3 servers being load-balanced.")
    print("  Press Ctrl+C to stop everything.")
    print("=" * 64)

    if args.open:
        try:
            webbrowser.open(url)
        except Exception:
            pass

    try:
        while True:
            time.sleep(1)
            for name, proc in _procs:
                if proc.poll() is not None:
                    print(f"[stack] WARNING: {name} exited (code {proc.returncode}).")
                    raise KeyboardInterrupt
    except KeyboardInterrupt:
        pass
    finally:
        _shutdown()


if __name__ == "__main__":
    main()
