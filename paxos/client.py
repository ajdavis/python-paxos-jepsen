import argparse
import dataclasses
import json
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from core import ClientRequest, Config
from network import send_to_all


def main(raw_config: dict, payload: int, n_servers: int):
    config = Config(**raw_config)
    assert n_servers <= len(config.nodes)
    servers = config.nodes[:n_servers]
    # pid is unique enough, all clients can use command_id 1.
    r = ClientRequest(client_id=os.getpid(),
                      command_id=1,
                      payload=payload)
    replies = send_to_all(
        servers, '/proposer/client-request', dataclasses.asdict(r))

    for r in replies:
        print(r)


if __name__ == '__main__':
    parser = argparse.ArgumentParser("Paxos client")
    parser.add_argument("config", type=argparse.FileType(),
                        help="Config file (see example-config)")
    parser.add_argument("--servers", type=int, default=1,
                        help="Number of servers to send the message to")
    parser.add_argument("payload", type=int)
    args = parser.parse_args()
    main(json.load(args.config), args.payload, args.servers)
