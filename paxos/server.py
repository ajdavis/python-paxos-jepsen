import argparse
import json

from flask import Flask, jsonify, request
from flask.logging import default_handler

from core import *

root = logging.getLogger()
root.setLevel(logging.INFO)
root.addHandler(default_handler)
app = Flask('PyPaxos')


@app.route('/proposer/client-request', methods=['POST'])
def client_request():
    proposer.receive(ClientRequest(**request.json))
    return jsonify("ok")


@app.route('/acceptor/prepare', methods=['POST'])
def prepare():
    acceptor.receive(Prepare(**request.json))
    return jsonify("ok")


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

    proposer = Proposer(config, args.port, reverse_url("prepare"))
    proposer.run()

    acceptor = Acceptor(config, args.port)
    acceptor.run()

    app.run(host="0.0.0.0", port=args.port, debug=True, use_reloader=False)
