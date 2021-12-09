# Python Multi-Paxos and Jepsen

I want to implement Multi-Paxos and test it with Jepsen, as an exercise to understand both.

## Paxos

`paxos/` has a Python implementation of Multi-Paxos, roughly following "Formal Verification of
Multi-Paxos for Distributed Consensus", Chand et al 2016. It has no stable leader, no election
protocol, no reconfiguration, no Fast Paxos. What it lacks in features it makes up for in bugs. It
can't run very long since it uses more memory and passes larger messages with each operation.

Requires Python 3.9 or later. Set up with `python3 -m pip install -r paxos/requirements.txt`.

Run `python3 paxos/start-servers.py paxos/example-config` to start 3 servers. Their shared state is
just an appendable list of ints, initially empty. (An appendable list is a useful data structure for
testing linearizability.)

Use `python3 paxos/client.py paxos/example-config 1` to append 1 (or a number of your choice) to the
list of ints. My goal is to make this list a linearizable data structure, and test it with Jepsen.

## Jepsen

`jepsen/` has Clojure code that uses Jepsen, and the [Knossos](https://github.com/jepsen-io/knossos)
checker, to test that the Paxos replicated state machine (the appendable list) is linearizable when
clients are concurrently adding numbers to it, sending requests to different nodes at once.

### EC2 setup

Using either
the [Jepsen AMI from the AWS Marketplace](https://aws.amazon.com/marketplace/pp/prodview-ykhheuyq5qdnq)
, or a Debian 10 Buster AMI, make one controller node and some worker nodes. I used t3.xlarge for
the controller, since it does heavy analysis at the end of a test, and three t3.mediums for the
others. Name them "control", "worker0", "worker1", etc. Make sure they're world-accessible on ports
22 and 5000 (which they'll use for Paxos messages). On each:

```shell
sudo apt update
sudo apt install -y wget build-essential libreadline-dev libncursesw5-dev libssl-dev \
 libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev zlib1g-dev jq awscli \
 default-jre libzip4 git rsync

# Install Python 3.9
curl -LO https://www.python.org/ftp/python/3.9.9/Python-3.9.9.tgz
tar xf Python-3.9.9.tgz
cd Python-3.9.9/
./configure --enable-optimizations && make -j8 && sudo make altinstall
cd

# Install Clojure
curl -O https://download.clojure.org/install/linux-install-1.10.3.1040.sh
chmod +x linux-install-1.10.3.1040.sh
sudo ./linux-install-1.10.3.1040.sh

# Install Clojure build tool, Leiningen
curl -LO https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
chmod +x lein
mkdir -p ~/bin
mv lein ~/bin
```

The controller must be able to ssh into the workers, so put a private key in its `~/.ssh/key.pem`,
and the public key in `~/.ssh/authorized_keys` on the workers.

On the controller:

```shell
# Jepsen needs this file to exist, it seems
touch ~/.ssh/known_hosts
# Get list of workers - you'll be asked for an AWS API key
aws configure
aws ec2 describe-instances --filters "Name=tag:Name,Values=worker*" \
  "Name=instance-state-name,Values=running" \
  --query "Reservations[].Instances[].PublicDnsName" \
  | jq -jr '.[]|. + "\n"' > ~/nodes
```

### Run Jepsen

```
# Some hardcoded paths assume the code is in /home/admin/python-paxos-jepsen
cd ~
git clone git@github.com:ajdavis/python-paxos-jepsen.git
cd python-paxos-jepsen/jepsen/jepsen.paxos
~/bin/lein run test --nodes-file ~/nodes --ssh-private-key ~/.ssh/key.pem --username admin
```

Leiningen installs the project's Clojure dependencies, then Jepsen starts up a "nemesis" that causes
random network partitions on the works. It starts some clients (5 by default, override
with `--concurrency`) that contact the worker nodes over HTTP on port 5000 and try to append random
ints to the shared list. Each time a client appends an int, the chosen worker replies with the
current contents of the list, which Jepsen stores for later analysis. After the test, Knossos
verifies the history is linearizable.
