import argparse
import dataclasses
import json
import os

from core import ClientRequest, Config
from network import send_to_all


def main(raw_config: dict, value: int):
    config = Config(**raw_config)
    # All clients can use command_id 1, pid is unique enough.
    r = ClientRequest(client_id=os.getpid(), command_id=1, new_value=value)
    replies = send_to_all(
        config.nodes, '/coordinator/client-request', dataclasses.asdict(r))

    for r in replies:
        print(r)


if __name__ == '__main__':
    parser = argparse.ArgumentParser("Paxos client")
    parser.add_argument("config", type=argparse.FileType(),
                        help="JSON config file (see example-config.json)")
    parser.add_argument("value", type=int)
    args = parser.parse_args()
    main(json.load(args.config), args.value)
