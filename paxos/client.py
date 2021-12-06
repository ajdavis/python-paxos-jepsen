import argparse
import dataclasses
import os
import sys
import typing
import logging

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from core import ClientRequest, Config
from network import send_to_all

logging.basicConfig()


def main(raw_config: typing.IO, port: int, payload: int, n_servers: int):
    config = Config.from_file(raw_config)
    assert n_servers <= len(config.nodes)
    nodes = config.nodes[:n_servers]
    # pid is unique enough, all clients can use command_id 1.
    r = ClientRequest(client_id=os.getpid(),
                      command_id=1,
                      payload=payload)
    replies = send_to_all(
        nodes=nodes,
        port=port,
        url='/proposer/client-request',
        raw_message=dataclasses.asdict(r),
        timeout=120)

    for r in replies:
        print(r)


if __name__ == '__main__':
    parser = argparse.ArgumentParser("Paxos client")
    parser.add_argument("config", type=argparse.FileType(),
                        help="Config file (see example-config)")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--servers", type=int, default=1,
                        help="Number of servers to send the message to")
    parser.add_argument("payload", type=int)
    args = parser.parse_args()
    main(args.config, args.port, args.payload, args.servers)
