"""
Run the Ingress LB as a standalone process, pointed at one or more
already-running backend nodes.

Usage (static backends -- unchanged from before):
    py run_lb.py --backend 127.0.0.1:8765 --backend 127.0.0.1:8766

Usage (dynamic registration -- nodes announce themselves instead of
being hardcoded here; see run_node.py's --lb-register-host/--port):
    py run_lb.py --register-port 9100

Both can be combined: static --backend entries and dynamically
registered nodes are routed to identically, side by side.
"""

# ingress/run_lb.py
import argparse
import asyncio
import logging

from lb import LoadBalancer
from admin_server import start_admin_server


def _parse_backend(spec: str) -> tuple[str, int]:
    host, port = spec.rsplit(":", 1)
    return host, int(port)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run the ClusterTalk Ingress LB")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument(
        "--backend", action="append", default=[],
        help="host:port of a statically-configured backend node -- repeat for multiple",
    )
    parser.add_argument("--register-host", default="0.0.0.0",
                         help="host the dynamic node-registration listener binds to")
    parser.add_argument("--register-port", type=int, default=None,
                         help="if set, nodes can register themselves here instead of "
                              "needing a --backend entry (see run_node.py --lb-register-port)")
    parser.add_argument("--admin-port", type=int, default=9200,
                         help="HTTP port for the admin dashboard API "
                              "(GET /api/stats, POST /api/simulate). 0 disables it.")
    args = parser.parse_args()

    if not args.backend and args.register_port is None:
        parser.error("need at least one --backend, or --register-port for dynamic "
                     "registration -- otherwise there's nowhere to route clients to")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    backends = [_parse_backend(b) for b in args.backend]
    lb = LoadBalancer(
        backends, host=args.host, port=args.port,
        register_host=args.register_host, register_port=args.register_port,
    )
    await lb.start()

    if args.admin_port:
        start_admin_server(
            lb, args.host, args.admin_port,
            register_host=args.register_host, register_port=args.register_port,
        )

    try:
        await lb.serve_forever()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await lb.stop()


if __name__ == "__main__":
    asyncio.run(main())