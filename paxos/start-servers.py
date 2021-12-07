import argparse
import os.path
import subprocess
import sys
import typing

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from core import Config


def main(raw_config: typing.IO, config_path: str, port: int):
    config = Config.from_file(raw_config, default_port=port)
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
                        help="Config file (see example-config)")
    parser.add_argument("--port", type=int, default=5000,
                        help="Default port (if not in config)")
    args = parser.parse_args()
    main(args.config, args.config.name, args.port)
