import argparse
import dataclasses
import json
import logging
import os.path
import sys
from typing import Type

from flask import Flask, jsonify, request
from flask.logging import default_handler

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

root = logging.getLogger()
root.setLevel(logging.INFO)
root.addHandler(default_handler)
app = Flask('PyPaxos')


@app.route('/proposer/client-request', methods=['POST'])
def client_request():
    """Receive client request, see client.py."""
    return handle(proposer, ClientRequest)


@app.route('/acceptor/prepare', methods=['POST'])
def prepare():
    """Receive Phase 1a message."""
    return handle(acceptor, Prepare)


@app.route('/acceptor/accept', methods=['POST'])
def accept():
    """Receive Phase 2a message."""
    return handle(acceptor, Accept)


def handle(agent: Agent, message_type: Type[Message]):
    return jsonify(
        dataclasses.asdict(agent.receive(message_type.from_dict(request.json))))


def reverse_url(endpoint: str):
    """Map handler function name to URL.

    Like Flask's url_for, but doesn't require a request context.
    """
    return app.url_map.bind("example").build(endpoint)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Paxos node")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--config", type=argparse.FileType(), required=True,
                        help="JSON config file (see example-config.json)")
    args = parser.parse_args()
    config = Config(**json.load(args.config))
    proposer = Proposer(config=config,
                        port=args.port,
                        propose_url=reverse_url("prepare"),
                        accept_url=reverse_url("accept"))
    proposer.run()
    acceptor = Acceptor(config, args.port)
    acceptor.run()
    app.run(host="0.0.0.0", port=args.port)
