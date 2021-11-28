import argparse
import dataclasses
import json

from core import ClientRequest, Config
from network import send_all


def main(raw_config: dict, value: int):
    config = Config(**raw_config)
    replies = send_all(config.nodes, '/coordinator/client-request',
                       dataclasses.asdict(ClientRequest(new_value=value)))

    for r in replies:
        print(r)


if __name__ == '__main__':
    parser = argparse.ArgumentParser("Paxos client")
    parser.add_argument("config", type=argparse.FileType(),
                        help="JSON config file (see example-config.json)")
    parser.add_argument("value", type=int)
    args = parser.parse_args()
    main(json.load(args.config), args.value)
