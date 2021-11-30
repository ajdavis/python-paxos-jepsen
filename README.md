Python implementation of Multi-Paxos with a stable leader and reconfiguration, roughly following
"Formal Verification of Multi-Paxos for Distributed Consensus", Chand et al 2016.

Run `python3 paxos/start-servers.py paxos/example-config.json` to start 3 servers. Each runs several
Paxos "agents": leader, acceptor, and replica. Their shared state is just a list of ints, initially
empty.

Use `python3 paxos/client.py paxos/example-config.json 1` to append 1 (or a number of your choice)
to the list of ints. My goal is to make this list linearizable, and test it with Jepsen.

Requires Python 3.10 or later. Set up with `python3 -m pip install -r paxos/requirements.txt`.
