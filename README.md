# Python Multi-Paxos and Jepsen

I want to implement Multi-Paxos and test it with Jepsen, as an exercise to understand both.

## Paxos

`paxos/` has a Python implementation of Multi-Paxos, roughly following "Formal Verification of
Multi-Paxos for Distributed Consensus", Chand et al 2016. It has no stable leader, no election
protocol, no reconfiguration, no Fast Paxos. What it lacks in features it makes up for in bugs. It
can't run very long since it uses more memory and passes larger messages with each operation.

Requires Python 3.10 or later. Set up with `python3 -m pip install -r paxos/requirements.txt`.

Run `python3 paxos/start-servers.py paxos/example-config.json` to start 3 servers. Their shared
state is just an appendable list of ints, initially empty. (An appendable list is Jepsen's favorite
data structure for testing consistency.)

Use `python3 paxos/client.py paxos/example-config.json 1` to append 1 (or a number of your choice)
to the list of ints. My goal is to make this list a linearizable data structure, and test it with
Jepsen.

## Jepsen

TBD.
