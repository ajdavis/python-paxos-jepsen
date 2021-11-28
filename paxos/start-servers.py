import argparse
import json
import os.path
import subprocess
import sys

from core import Config


def main(raw_config: dict, config_path: str):
    config = Config(**raw_config)
    nodes = []
    for s in config.nodes:
        host, port = s.split(':')
        nodes.append(subprocess.Popen([
            sys.executable,
            os.path.join(os.path.dirname(__file__), 'server.py'),
            '--port',
            port,
            '--config',
            config_path
        ]))

    for n in nodes:
        n.wait()


if __name__ == '__main__':
    parser = argparse.ArgumentParser("Paxos orchestrator")
    parser.add_argument("config", type=argparse.FileType(),
                        help="JSON config file (see example-config.json)")
    args = parser.parse_args()
    main(json.load(args.config), args.config.name)
