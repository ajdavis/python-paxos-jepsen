import argparse
import dataclasses
import os
import sys
import typing
import logging

from message import ClientReply

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from core import ClientRequest, Config
from network import send

logging.basicConfig()


def main(raw_config: typing.IO, port: int, server: int, payload: int):
    config = Config.from_file(raw_config, default_port=port)
    node = config.nodes[server]
    # pid is unique enough, all clients can use command_id 1.
    r = ClientRequest(client_id=os.getpid(),
                      command_id=1,
                      payload=payload)

    raw_reply = send(
        node=node,
        url='/proposer/client-request',
        raw_message=dataclasses.asdict(r),
        timeout=20)

    if raw_reply is None:
        sys.exit(1)

    reply = ClientReply.from_dict(raw_reply)
    # Like "[1, 2, 3]".
    print(reply.state)


if __name__ == '__main__':
    parser = argparse.ArgumentParser("Paxos client")
    parser.add_argument(
        "config", type=argparse.FileType(),
        help="Config file (see example-config)")
    parser.add_argument(
        "--port", type=int, default=5000)
    parser.add_argument(
        "--server", type=int, default=0,
        help="Server number (0 through number of nodes in config)")
    parser.add_argument(
        "payload", type=int)
    args = parser.parse_args()
    main(args.config, args.port, args.server, args.payload)
