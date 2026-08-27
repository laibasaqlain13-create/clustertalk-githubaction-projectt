"""
Run a single ChatNode as a standalone process.

Usage (single node, no mesh -- unchanged from before):
    py run_node.py                       # defaults: 127.0.0.1:8765, ./clustertalk.db
    py run_node.py --port 9000 --db mynode.db

Usage (3-node mesh, so a room can span all three -- run each in its own
terminal):
    py run_node.py --port 8765 --mesh-port 9765 --node-id A --peer 127.0.0.1:9766 --peer 127.0.0.1:9767 --db a.db
    py run_node.py --port 8766 --mesh-port 9766 --node-id B --peer 127.0.0.1:9765 --peer 127.0.0.1:9767 --db b.db
    py run_node.py --port 8767 --mesh-port 9767 --node-id C --peer 127.0.0.1:9765 --peer 127.0.0.1:9766 --db c.db

--mesh-port is a SEPARATE port from --port: clients connect to --port,
other nodes connect to each other on --mesh-port. --peer takes the
OTHER node's mesh-port (host:port), repeatable -- one --peer per other
node in the mesh. If --mesh-port is omitted, the node runs exactly as
before with no mesh at all.

Usage (register dynamically with an LB instead of a static --backend
entry -- see ingress/lb.py's DYNAMIC REGISTRATION / run_lb.py's
--register-port):
    py run_node.py --port 8765 --db a.db --advertise-host 127.0.0.1 \
        --lb-register-host 127.0.0.1 --lb-register-port 9100

--advertise-host is the address OTHER machines should use to reach this
node's --port; defaults to --host, or 127.0.0.1 if --host is 0.0.0.0
(which nothing outside this box can actually dial). Combine with
--mesh-port/--peer freely -- registration and mesh are independent.

Once running, connect a client through the WebSocket bridge + frontend
(see the project README), or write frames directly -- a plain telnet/nc
won't work since the wire protocol is binary length-prefixed frames.
"""

import argparse
import asyncio
import logging
import os

from database import default_database_path
from server import ChatNode


def _parse_peer(value: str) -> tuple[str, int]:
    host, _, port = value.rpartition(":")
    if not host or not port.isdigit():
        raise argparse.ArgumentTypeError(f"--peer must be host:port, got {value!r}")
    return host, int(port)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run a ClusterTalk chat node")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8765)))
    parser.add_argument("--db", default=None)
    parser.add_argument("--auth-db", default=None,
                        help="shared credentials SQLite DB; use the same path for every mesh node")
    parser.add_argument("--node-id", default=None, help="stable identity for this node in the mesh")
    parser.add_argument("--mesh-host", default="0.0.0.0")
    parser.add_argument("--mesh-port", type=int, default=None,
                         help="if set, this node joins the mesh on this port")
    parser.add_argument("--peer", action="append", type=_parse_peer, default=[],
                         dest="peers", metavar="HOST:PORT",
                         help="another node's mesh-port; repeat once per peer")
    parser.add_argument("--advertise-host", default=None,
                         help="address other machines/the LB should use to reach --port "
                              "(defaults to --host, or 127.0.0.1 if --host is 0.0.0.0)")
    parser.add_argument("--lb-register-host", default=None,
                         help="LB's registration host (see run_lb.py --register-port)")
    parser.add_argument("--lb-register-port", type=int, default=None,
                         help="if set (with --lb-register-host), this node dynamically "
                              "registers itself with the LB instead of needing a static "
                              "--backend entry there")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    node = ChatNode(
        db_path=args.db or default_database_path(), host=args.host, port=args.port,
        node_id=args.node_id, mesh_host=args.mesh_host,
        mesh_port=args.mesh_port, peer_addresses=args.peers,
        advertise_host=args.advertise_host,
        lb_register_host=args.lb_register_host,
        lb_register_port=args.lb_register_port,
        auth_db_path=args.auth_db,
    )
    await node.start()
    try:
        await node.serve_forever()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await node.stop()


if __name__ == "__main__":
    asyncio.run(main())
