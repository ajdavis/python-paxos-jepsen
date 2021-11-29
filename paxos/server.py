import argparse
import json

from flask import Flask, jsonify, request
from flask.logging import default_handler

from core import *

"""
A single Paxos server process with Paxos agents serving various roles (terms
from "Paxos Made Moderately Complex"):
- Replica
- Leader
- Acceptor

The agents communicate with each other, and with agents in other server
processes, via HTTP requests. Clients (see client.py) communicate with Replicas.
"""

root = logging.getLogger()
root.setLevel(logging.INFO)
root.addHandler(default_handler)
app = Flask('PyPaxos')


@app.route('/replica/client-request', methods=['POST'])
def client_request():
    return jsonify(replica.receive(ClientRequest.from_dict(request.json)))


@app.route('/leader/propose', methods=['POST'])
def propose():
    return jsonify(leader.receive(Proposal.from_dict(request.json)))


@app.route('/acceptor/prepare', methods=['POST'])
def prepare():
    # TODO
    pass


@app.route('/acceptor/accept', methods=['POST'])
def accept():
    # TODO
    pass


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
    replica = Replica(config=config,
                      port=args.port,
                      propose_url=reverse_url("propose"))
    replica.run()
    leader = Leader(config=config,
                    port=args.port,
                    prepare_url=reverse_url("prepare"),
                    accept_url=reverse_url("accept"))
    leader.run()
    acceptor = Acceptor(config, args.port)
    acceptor.run()
    app.run(host="0.0.0.0", port=args.port)
