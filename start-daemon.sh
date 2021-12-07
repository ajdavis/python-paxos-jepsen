#!/bin/bash

start-stop-daemon --start --background --chdir /home/admin/python-paxos-jepsen/ --chuid admin \
  --make-pidfile --pidfile /var/paxos.pid --startas /bin/bash -- -c \
  "exec /usr/local/bin/python3.9 paxos/server.py --config /home/admin/nodes > /home/admin/paxos.log 2>&1"
