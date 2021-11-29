import dataclasses
import logging
import queue
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass

from message import (ClientReply,
                     Decision,
                     DecisionReply,
                     Message,
                     Proposal,
                     is_reconfig,
                     ClientRequest)
from network import send

WINDOW = 1
"""Number of slots we wait before executing a reconfiguration."""


@dataclass
class Config:
    nodes: list[str]


class Agent:
    """An agent (or "process") fulfilling a role in the Paxos protocol."""

    def __init__(self, config: Config, port: int):
        self._config = config
        self._port = port
        self.__q: queue.Queue[Agent._QEntry] = queue.Queue()
        self.__executor = ThreadPoolExecutor()

    def run(self):
        future = self.__executor.submit(self._main_loop, self.__q)

        def done_callback(f: Future):
            try:
                f.result()
            except Exception as exc:
                logging.exception(self.__class__.__name__)

        future.add_done_callback(done_callback)

    def receive(self, message: Message) -> Message:
        """Handle a request, return the reply."""
        logging.getLogger("paxos").info(
            "%s port %d got %s", self.__class__.__name__, self._port,
            message)

        future = Future()
        self.__q.put(Agent._QEntry(message, future))
        return future.result()

    @dataclass
    class _QEntry:
        message: Message
        reply_future: Future[Message]

    def _main_loop(self, q: queue.Queue["Agent._QEntry"]) -> None:
        raise NotImplementedError()

    def _send_message(self, node: str, url: str, message: Message) -> None:
        """Send message without waiting for a reply."""
        self.__executor.submit(
            lambda: send(node, url, dataclasses.asdict(message)))


def _first(seq):
    return next(seq, None)  # Return None if seq is empty.


class Replica(Agent):
    """Fig. 1 of "Paxos Made Moderately Complex"."""

    def __init__(self, config: Config, port: int, propose_url: str):
        super().__init__(config, port)
        self._propose_url = propose_url
        # Replicated state is just an appendable list of numbers, see README.
        self._state: list[int] = []
        # Next slot in which this replica hasn't proposed any new_value.
        self._slot_in: int = 1
        # Next slot for which we're awaiting a decision, <= slot_in.
        self._slot_out: int = 1
        # ClientRequests we've received but haven't yet proposed.
        self._requests: set[ClientRequest] = set()
        # Proposals we've sent, for which we're awaiting decisions.
        self._proposals: set[Proposal] = set()
        # Proposals that have been decided (aka chosen).
        self._decisions: set[Proposal] = set()
        # Nodes that are leaders (all nodes, in my implementation).
        self._leaders: set[str] = set(config.nodes)
        # Not in "Paxos Made Moderately Complex": waiting clients' futures.
        self._futures: dict[ClientRequest, Future[Message]] = {}

    def _propose(self):
        while self._slot_in < self._slot_out + WINDOW and self._requests:
            # If a pending reconfig is ready, do it.
            if reconfig := _first(
                (d for d in self._decisions
                 if d.slot == self._slot_in - WINDOW
                    and is_reconfig(d.client_request))
            ):
                self._leaders = reconfig.client_request.new_leaders

            # Choose an arbitrary pending request to propose for current slot.
            c: ClientRequest = next(iter(self._requests))
            preempt = _first(
                (d for d in self._decisions if d.slot == self._slot_in))

            if not preempt:
                self._requests.remove(c)
                for leader in self._leaders:
                    self._send_message(
                        leader, self._propose_url, Proposal(self._slot_in, c))

            self._slot_in += 1

    def _perform(self, client_request: ClientRequest):
        # Skip reconfigs and operations we already performed.
        if (
            _first(d for d in self._decisions
                   if d.slot < self._slot_out
                      and d.client_request == client_request)
            or is_reconfig(client_request)
        ):
            self._slot_out += 1
        else:
            # All operations in my code are "append int to list".
            self._state.append(client_request.new_value)
            self._slot_out += 1

        # Send reply to the client.
        self._futures.pop(client_request).set_result(ClientReply(self._state))

    def _main_loop(self, q: queue.Queue[Agent._QEntry]) -> None:
        while True:
            entry = q.get()
            if isinstance(entry.message, ClientRequest):
                self._requests.add(entry.message)
                # We'll resolve the future / reply to client eventually.
                self._futures[entry.message] = entry.reply_future
            else:
                assert isinstance(entry.message, Decision)
                self._decisions.add(entry.message)
                # Leader ignores this reply anyway.
                entry.reply_future.set_result(DecisionReply())

                # Perform the chosen op and any later ones unblocked by it.
                while d := _first(d for d in self._decisions
                                  if d.slot == self._slot_out):
                    if p := _first(p for p in self._proposals
                                   if p.slot == self._slot_out):
                        self._proposals.remove(p)
                        if p.client_request != d.client_request:
                            # Failed proposal, retry later.
                            self._requests.add(p.client_request)

                    # Increments _slot_out, keeping the while loop going.
                    self._perform(d.client_request)


class Leader(Agent):
    """Fulfills Coordinator, Proposer, and Learner roles."""

    def __init__(
            self, config: Config, port: int, prepare_url: str, accept_url: str):
        super().__init__(config, port)
        self._prepare_url: str = prepare_url
        self._accept_url: str = accept_url
        try:
            # My id is my position in the list of nodes in the config.
            self._id: int = 1 + next(i for i, node in enumerate(config.nodes)
                                     if int(node.split(":")[1]) == port)
        except StopIteration:
            raise ValueError(f"Couldn't find port {port} in config: {config}")

        self._highest_round: int = 0
        self._value: int | None = None


class Acceptor(Agent):
    """Just the Acceptor role."""

    def __init__(self, config: Config, port: int):
        super().__init__(config, port)
