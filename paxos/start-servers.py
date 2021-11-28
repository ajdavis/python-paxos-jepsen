import argparse
import json
import os.path
import subprocess
import sys


def main(config: dict, config_path: str):
    nodes = []
    for s in config["nodes"]:
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
    parser = argparse.ArgumentParser("Paxos node")
    parser.add_argument("config", type=argparse.FileType(),
                        help="JSON config file (see example-config.json)")
    args = parser.parse_args()
    main(json.load(args.config), args.config.name)
