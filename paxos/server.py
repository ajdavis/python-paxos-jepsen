import argparse
import dataclasses
import logging
import os.path
import signal
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Type

import requests
from flask import Flask, jsonify, request

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from core import *
from message import *

"""
A single Paxos server process with Paxos agents serving various roles:
- Proposer / Learner
- Acceptor

The agents communicate with each other, and with agents in other server
processes, via HTTP requests. Clients (see client.py) communicate with Replicas.
"""

app = Flask('PyPaxos')

server_id = uuid.uuid4().hex


@app.route('/server_id', methods=['GET'])
def get_server_id():
    """Used to find self in configuration."""
    return jsonify(server_id)


@app.route('/proposer/client-request', methods=['POST'])
def client_request():
    """Receive client request, see client.py."""
    return handle(proposer, ClientRequest)


@app.route('/acceptor/prepare', methods=['POST'])
def prepare():
    """Receive Phase 1a message."""
    return handle(acceptor, Prepare)


@app.route('/proposer/promise', methods=['POST'])
def promise():
    """Receive Phase 1b message."""
    return handle(proposer, Promise)


@app.route('/acceptor/accept', methods=['POST'])
def accept():
    """Receive Phase 2a message."""
    return handle(acceptor, Accept)


@app.route('/proposer/accepted', methods=['POST'])
def accepted():
    """Receive Phase 2b message."""
    return handle(proposer, Accepted)


def handle(agent: Agent, message_type: Type[Message]):
    try:
        return jsonify(dataclasses.asdict(
                agent.receive(message_type.from_dict(request.json))))
    except Exception:
        logging.error("Processing input: %s", request.json)
        raise


def reverse_url(endpoint: str):
    """Map handler function name to URL.

    Like Flask's url_for, but doesn't require a request context.
    """
    return app.url_map.bind("example").build(endpoint)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Paxos node")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--config", type=argparse.FileType(), required=True,
                        help="Config file (see example-config)")
    parser.add_argument("--log-file", default=None)

    args = parser.parse_args()

    # Uses stdout/stderr if log_file is None.
    logging.basicConfig(
        filename=args.log_file,
        format="[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
        level=logging.INFO)
    logger = logging.getLogger("server")

    config = Config.from_file(args.config, default_port=args.port)
    assert config.nodes
    proposer = Proposer(config=config,
                        propose_url=reverse_url("prepare"),
                        accept_url=reverse_url("accept"))
    proposer.run()
    acceptor = Acceptor(config=config,
                        promise_url=reverse_url("promise"),
                        accepted_url=reverse_url("accepted"))
    acceptor.run()
    # Run Flask app in background so we can do "finding self" logic below.
    executor = ThreadPoolExecutor()
    app_done = executor.submit(lambda: app.run(host="0.0.0.0", port=args.port))

    logger.info("Finding self in config of %s nodes", len(config.nodes))
    start = time.monotonic()
    found_self = False
    reason: Optional[Exception] = None
    while time.monotonic() - start < 20 * len(config.nodes) and not found_self:
        for n in config.nodes:
            node_url = f"http://{n}{reverse_url('get_server_id')}"
            try:
                if requests.get(node_url, timeout=10).json() == server_id:
                    config.set_self(n)
                    found_self = True
                    logger.info("Found self: %s", n)
                    break
            except requests.RequestException as exc:
                reason = exc
                time.sleep(1)
                continue

    if not found_self:
        logger.error("Failed to find self in config, reason: %s", reason)
        # Simpler than the self-pipe trick, if brutal.
        os.kill(os.getpid(), signal.SIGTERM)

    app_done.result()
