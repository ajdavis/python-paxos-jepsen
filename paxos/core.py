import dataclasses
import logging
import queue
import uuid
from collections import defaultdict, deque
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Optional, Sequence

from dataclasses import dataclass

from message import *
from network import send_to_all

__all__ = [
    "Config",
    "Agent",
    "Proposer",
    "Acceptor",
]


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
            except Exception:
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

    def _send_to_all(self, url: str, message: Message) -> None:
        """Send message to all nodes without awaiting reply."""
        self.__executor.submit(
            send_to_all, self._config.nodes, url, dataclasses.asdict(message))


class Proposer(Agent):
    """Proposer, also fulfilling the Learner role."""

    def __init__(self,
                 config: Config,
                 port: int,
                 propose_url: str,
                 accept_url: str):
        super().__init__(config, port)
        self._propose_url = propose_url
        self._accept_url = accept_url
        # "pBal" in Chand. Include a UUID to ensure a unique tiebreaker.
        self._ballot: Ballot = Ballot(1, uuid.uuid4().hex)
        # ClientRequests we haven't used in Accept messages.
        self._requests_unserviced: deque[ClientRequest] = deque()
        # "Promise" messages received from Acceptors.
        self._promises: dict[Ballot, list[Promise]] = defaultdict(list)
        # "Accepted" messages received from Acceptors.
        self._accepteds: dict[Ballot, list[Accepted]] = defaultdict(list)
        # Map slot to value (None is undecided), and whether it's been applied.
        self._decisions: dict[Slot, tuple[Optional[Value], bool]] = {}
        # Clients waiting for a response.
        self._futures: dict[Value, Future[Message]] = {}
        # The replicated state machine (RSM) is just an appendable list of ints.
        self._state: list[int] = []

    def _handle_client_request(self,
                               client_request: ClientRequest,
                               future: Future[Message]) -> None:
        # Phase 1a, Fig. 2 of Chand.
        self._requests_unserviced.appendleft(client_request)
        self._futures[client_request.get_value()] = future
        self._ballot.inc += 1
        prepare = Prepare(self._port, self._ballot)
        self._send_to_all(self._propose_url, prepare)

    def _handle_promise(self,
                        promise: Promise,
                        future: Future[Message]) -> None:
        # Phase 2a, Fig. 4 of Chand.
        future.set_result(OK())
        self._promises[promise.ballot].append(promise)
        promises = self._promises[promise.ballot]
        if len(promises) <= len(self._config.nodes) // 2:
            # No majority yet.
            return

        self._promises.pop(promise.ballot)
        # Highest-ballot-numbered value for each slot.
        slot_values = max_sv([p.voted for p in promises])
        # Choose new slots for the client's values.
        new_slot = max((sv.slot for sv in slot_values), default=0) + 1
        while self._requests_unserviced:
            # TODO: some circumstance where we should put it back?
            cr = self._requests_unserviced.pop()
            slot_values.add(SlotValue(new_slot, cr.get_value()))
            new_slot += 1

        accept = Accept(self._port, self._ballot, list(slot_values))
        self._send_to_all(self._accept_url, accept)

    def _handle_accepted(self,
                         accepted: Accepted,
                         future: Future[Message]) -> None:
        # This is a Learner procedure, and Chand doesn't cover Learners.
        future.set_result(OK())
        self._accepteds[accepted.ballot].append(accepted)
        accepteds = self._accepteds[accepted.ballot]
        if len(accepteds) <= len(self._config.nodes) // 2:
            # No majority yet.
            return

        self._accepteds.pop(accepted.ballot)
        # TODO: assert no conflicts among accepteds.
        for sv in accepted.voted:
            if sv.slot not in self._decisions:
                # TODO: do we need Applied for correctness?
                self._decisions[sv.slot] = (sv.value, False)  # Applied=False.

        # Update the RSM with newly unblocked decisions.
        for slot, (value, applied) in sorted(self._decisions.items()):
            if value is None:
                # Still undecided, can't execute any later slots.
                break

            if applied:
                continue

            self._apply(value)
            self._decisions[slot] = (value, True)  # Applied=True.

    def _min_undecided_slot(self):
        """First slot without a majority-accepted value."""
        return min(
            (s for s, v in self._decisions.items() if v is None),
            default=len(self._decisions))

    def _apply(self, value: Value):
        """Actually update the RSM and reply to the client."""
        self._state.append(value.payload)
        if value in self._futures:
            # This server is the one responsible for replying to the client.
            self._futures.pop(value).set_result(ClientReply(self._state))

    def _main_loop(self, q: queue.Queue[Agent._QEntry]) -> None:
        while True:
            entry = q.get()
            if isinstance(entry.message, ClientRequest):
                self._handle_client_request(entry.message, entry.reply_future)
            elif isinstance(entry.message, Promise):
                self._handle_promise(entry.message, entry.reply_future)
            elif isinstance(entry.message, Accepted):
                self._handle_accepted(entry.message, entry.reply_future)
            else:
                entry.reply_future.set_exception(
                    ValueError(f"Unexpected {entry.message}"))


# Fig. 4 of Chand, auxiliary operators.
def max_sv(vs: Sequence[VotedSet]) -> set[SlotValue]:
    """(slot, val) with highest-ballot-numbered value for each slot.

    Incoming vs is a list of dicts, which map slot to highest-balloted PValue,
    which is (ballot, slot, value). For each slot, choose highest-balloted
    PValue among all dicts, and return (slot, value).

    See test_max_sv().
    """
    slot_to_ballot_value: dict[Slot, tuple[Ballot, Value]] = {}
    for v in vs:
        for slot, pvalue in v.items():
            assert pvalue.slot == slot
            if (slot not in slot_to_ballot_value
                    or slot_to_ballot_value[slot][0] < pvalue.ballot):
                slot_to_ballot_value[slot] = (pvalue.ballot, pvalue.value)

    return {SlotValue(slot, value)
            for slot, (_, value) in slot_to_ballot_value.items()}


class Acceptor(Agent):
    """Fulfills only the Acceptor role."""

    def __init__(self,
                 config: Config,
                 port: int,
                 promise_url: str,
                 accepted_url: str):
        super().__init__(config, port)
        self._promise_url = promise_url
        self._accepted_url = accepted_url
        # Highest ballot seen. "aBal" in Chand.
        self._ballot: Ballot = Ballot.min()
        # Highest ballot voted for per slot. "aVoted" in Chand. Grows forever.
        self._voted: VotedSet = {}

    def _handle_prepare(self, prepare: Prepare) -> None:
        # Phase 1b, Fig. 3 in Chand.
        if prepare.ballot <= self._ballot:
            return

        self._ballot = prepare.ballot
        promise = Promise(self._port, self._ballot, self._voted)
        self._send_to_all(self._promise_url, promise)

    def _handle_accept(self, accept: Accept) -> None:
        # Phase 2b, Fig. 5 in Chand. Note < for accept and <= for prepare.
        if accept.ballot < self._ballot:
            # Ignore.
            return

        assert accept.ballot >= self._ballot
        self._ballot = accept.ballot
        # TODO: right?
        accept_voted_set = {sv.slot: PValue(accept.ballot, sv.slot, sv.value)
                            for sv in accept.voted}
        self._voted.update(accept_voted_set)
        accepted = Accepted(self._port, self._ballot, accept.voted)
        self._send_to_all(self._accepted_url, accepted)

    def _main_loop(self, q: queue.Queue[Agent._QEntry]) -> None:
        while True:
            entry = q.get()
            # Replies are meaningless, we respond by sending new messages.
            entry.reply_future.set_result(OK())
            if isinstance(entry.message, Prepare):
                self._handle_prepare(entry.message)
            else:
                assert isinstance(entry.message, Accept)
                self._handle_accept(entry.message)
