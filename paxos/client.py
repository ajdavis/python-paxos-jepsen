import argparse
import dataclasses
import json
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from core import ClientRequest, Config
from network import send_to_all


def main(raw_config: dict, value: int):
    config = Config(**raw_config)
    r = ClientRequest(new_value=value)
    replies = send_to_all(
        config.nodes, '/proposer/client-request', dataclasses.asdict(r))

    for r in replies:
        print(r)


if __name__ == '__main__':
    parser = argparse.ArgumentParser("Paxos client")
    parser.add_argument("config", type=argparse.FileType(),
                        help="JSON config file (see example-config.json)")
    parser.add_argument("value", type=int)
    args = parser.parse_args()
    main(json.load(args.config), args.value)
