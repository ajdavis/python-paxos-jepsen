import math
import dataclasses
import logging
import queue
import time
import typing
from collections import defaultdict, deque
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Optional, Sequence

from dataclasses import dataclass

from message import *
from network import send, send_to_all

__all__ = [
    "Config",
    "Agent",
    "Proposer",
    "Acceptor",
]

_logger = logging.getLogger("paxos")


@dataclass
class Config:
    nodes: list[str]
    _me: Optional[str] = dataclasses.field(init=False)
    _found_self: Future[str] = dataclasses.field(
        default_factory=lambda: Future(), init=False)

    @classmethod
    def from_file(cls, config_file: typing.IO, default_port: int):
        def gen():
            for line in config_file.readlines():
                line = line.strip()
                if not line:
                    continue

                if ":" in line:
                    yield line
                else:
                    yield f"{line}:{default_port}"

        return Config(list(gen()))

    def set_self(self, self_node: str):
        """Set my entry in 'nodes'."""
        self._found_self.set_result(self_node)

    def get_self(self) -> str:
        """Get my entry in 'nodes'. Blocks waiting for set_self()."""
        return self._found_self.result()


class Agent:
    """An agent (or "process") fulfilling a role in the Paxos protocol."""

    def __init__(self, config: Config):
        self._config = config
        self.__q: queue.Queue[Agent._QEntry] = queue.Queue()
        self.__executor = ThreadPoolExecutor()

    def get_uri(self) -> str:
        """Like hostname:port. Can block awaiting Config.set_self()."""
        return self._config.get_self()

    def run(self) -> None:
        future = self.__executor.submit(self._main_loop, self.__q)

        def done_callback(f: Future):
            try:
                f.result()
            except Exception:
                logging.exception(self.__class__.__name__)

        future.add_done_callback(done_callback)

    def receive(self, message: Message) -> Message:
        """Handle a request, return the reply."""
        _logger.info("%s got %s", self.__class__.__name__, message)
        future = Future()
        self.__q.put(Agent._QEntry(message, future))
        return future.result()

    @dataclass
    class _QEntry:
        message: Message
        reply_future: Future[Message]

    def _main_loop(self, q: queue.Queue["Agent._QEntry"]) -> None:
        raise NotImplementedError()

    def _send(self, node: str, url: str, message: Message) -> None:
        """Send message without awaiting reply."""
        _logger.info("Send %s to %s%s", message, node, url)
        self.__executor.submit(
            send, node=node, url=url, raw_message=dataclasses.asdict(message))

    def _send_to_all(self, url: str, message: Message) -> None:
        """Send message to all nodes without awaiting reply."""
        _logger.info("Send %s to all nodes, url %s", message, url)
        self.__executor.submit(send_to_all,
                               nodes=self._config.nodes,
                               url=url,
                               raw_message=dataclasses.asdict(message))


class Proposer(Agent):
    """Proposer, also fulfilling the Learner role."""

    def __init__(self,
                 config: Config,
                 propose_url: str,
                 accept_url: str):
        super().__init__(config)
        self._propose_url = propose_url
        self._accept_url = accept_url
        self._max_ts = -1
        # "pBal" in Chand. Don't init until we can call get_self() w/o deadlock.
        self._ballot: Optional[Ballot] = None
        # ClientRequests we haven't used in Accept messages.
        self._requests_unserviced: deque[ClientRequest] = deque()
        # "Promise" messages received from Acceptors.
        self._promises: dict[Ballot, list[Promise]] = defaultdict(list)
        # Values we've proposed, which are awaiting Accepted messages.
        self._proposals: dict[Slot, Value] = {}
        # "Accepted" messages received from Acceptors.
        self._accepteds: dict[Ballot, list[Accepted]] = defaultdict(list)
        # Map slot to value (None is undecided), and whether it's been applied.
        self._decisions: dict[Slot, tuple[Optional[Value], bool]] = {}
        # Clients waiting for a response.
        self._futures: dict[Value, Future[Message]] = {}
        # The replicated state machine (RSM) is just an appendable list of ints.
        self._state: list[int] = []

    def _record_ts(self, ts: float):
        self._max_ts = max(self._max_ts, ts)

    def _next_ts(self):
        ts = time.monotonic()
        if ts <= self._max_ts:
            ts = math.nextafter(self._max_ts, math.inf)

        self._record_ts(ts)
        return ts

    def _get_ballot(self, should_inc: bool):
        #  Include this server's URL as a unique tiebreaker.
        new_ballot = Ballot(self._next_ts(), self._config.get_self())
        if self._ballot is None:
            self._ballot = new_ballot
        elif should_inc:
            assert new_ballot > self._ballot  # _next_ts should ensure this.
            self._ballot = new_ballot

        return self._ballot

    def _handle_client_request(self,
                               client_request: ClientRequest,
                               future: Future[Message]) -> None:
        # Phase 1a, Fig. 2 of Chand.
        self._requests_unserviced.appendleft(client_request)
        self._futures[client_request.get_value()] = future
        self._send_prepare()

    def _send_prepare(self):
        prepare = Prepare(self.get_uri(), self._get_ballot(should_inc=True))
        self._send_to_all(self._propose_url, prepare)

    def _handle_promise(self,
                        promise: Promise,
                        future: Future[Message]) -> None:
        # Phase 2a, Fig. 4 of Chand.
        future.set_result(OK())
        self._promises[promise.ballot].append(promise)
        self._record_ts(promise.ballot.ts)
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
            cr = self._requests_unserviced.pop()
            slot_values.add(SlotValue(new_slot, cr.get_value()))
            assert new_slot not in self._proposals
            self._proposals[new_slot] = cr.get_value()
            _logger.info("Proposing %s for slot %s", cr.get_value(), new_slot)
            new_slot += 1

        accept = Accept(self.get_uri(), promise.ballot, list(slot_values))
        self._send_to_all(self._accept_url, accept)

    def _handle_accepted(self,
                         accepted: Accepted,
                         future: Future[Message]) -> None:
        # This is a Learner procedure, and Chand doesn't cover Learners.
        future.set_result(OK())
        self._accepteds[accepted.ballot].append(accepted)
        self._record_ts(accepted.ballot.ts)
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
        # TODO: don't think this is right. Some proposals for lower slots could
        # still be undecided. Should propose None for those and await decision?
        _logger.info(
            'Decisions (slot, value, applied): %s',
            [(slot, value.payload, applied)
             for slot, (value, applied) in sorted(self._decisions.items())])

        for slot, (value, applied) in sorted(self._decisions.items()):
            if proposal := self._proposals.pop(slot, None):
                if proposal != value:
                    # Failed proposal.
                    _logger.info("Re-enqueue %s", proposal)
                    # TODO: just make Value and ClientRequest the same.
                    cr = ClientRequest(**dataclasses.asdict(proposal))
                    assert cr not in self._requests_unserviced
                    assert proposal in self._futures
                    self._requests_unserviced.appendleft(cr)

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
            try:
                entry = q.get(timeout=1)
            except queue.Empty:
                # Any failed Prepare attempts?
                if self._requests_unserviced:
                    _logger.info("%s unserviced requests, send Prepare again",
                                 len(self._requests_unserviced))
                    self._send_prepare()

                continue

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
                 promise_url: str,
                 accepted_url: str):
        super().__init__(config)
        self._promise_url = promise_url
        self._accepted_url = accepted_url
        # Highest ballot seen. "aBal" in Chand.
        self._ballot: Ballot = Ballot.min()
        # Highest ballot voted for per slot. "aVoted" in Chand. Grows forever.
        self._voted: VotedSet = {}

    def _handle_prepare(self, prepare: Prepare) -> None:
        # Phase 1b, Fig. 3 in Chand.
        if prepare.ballot <= self._ballot:
            _logger.info("Ignore Phase 1a Prepare with stale %s, mine is %s",
                         prepare.ballot, self._ballot)
            return

        self._ballot = prepare.ballot
        promise = Promise(self.get_uri(), self._ballot, self._voted)
        self._send(prepare.from_uri, self._promise_url, promise)

    def _handle_accept(self, accept: Accept) -> None:
        # Phase 2b, Fig. 5 in Chand. Note < for accept and <= for prepare.
        if accept.ballot < self._ballot:
            _logger.info("Ignore Phase 2a Accept with stale %s, mine is %s",
                         accept.ballot, self._ballot)
            return

        self._ballot = accept.ballot
        # TODO: right?
        accept_voted_set = {sv.slot: PValue(accept.ballot, sv.slot, sv.value)
                            for sv in accept.voted}
        self._voted.update(accept_voted_set)
        accepted = Accepted(self.get_uri(), self._ballot, accept.voted)
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
