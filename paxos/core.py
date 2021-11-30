import dataclasses
import logging
import queue
from collections import defaultdict, deque
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Sequence

from dataclasses import dataclass

from message import *
from network import send_to_all

__all__ = [
    "Config",
    "Agent",
    "Proposer",
    "Acceptor",
]

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

    # def _send_to_all_and_recv(
    #     self,
    #     url: str,
    #     message: Message,
    #     reply_type: Type[Message]
    # ) -> list[Message | None]:
    #     """Send message to all nodes and get replies, omitting errors."""
    #     replies = send_to_all(
    #         self._config.nodes, url, dataclasses.asdict(message))
    #     return [reply_type.from_dict(reply)
    #             for reply in replies if reply is not None]


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
        # "pBal" in Chand.
        # TODO: make a tuple (id, inc)
        self._ballot_number: Ballot = self._port
        # ClientRequests we haven't turned into Accept messages.
        self._client_requests: deque[ClientRequest] = deque()
        # Promise messages received from Acceptors.
        self._promises: dict[Ballot, list[Promise]] = defaultdict(list)
        # Accepted messages received from Acceptors.
        self._accepteds: dict[Ballot, list[Accepted]] = defaultdict(list)
        # Slots with majority-accepted values.
        self._decisions: dict[Slot, Value | None] = {}
        # Clients waiting for a response.
        self._futures: dict[Ballot, Future[Message]] = {}
        # The replicated state machine (RSM) is just an appendable list of ints.
        self._state: list[int] = []

    def _handle_client_request(self, client_request: ClientRequest) -> None:
        # Phase 1a, Fig. 2 of Chand.
        self._client_requests.appendleft(client_request)
        self._ballot_number += len(self._config.nodes)
        prepare = Prepare(self._port, self._ballot_number)
        self._send_to_all(self._propose_url, prepare)

    def _handle_promise(self, promise: Promise):
        # Phase 2a, Fig. 4 of Chand.
        self._promises[promise.ballot].append(promise)
        promises = self._promises[promise.ballot]
        if len(promises) <= len(self._config.nodes) // 2:
            # No majority yet.
            return

        # TODO?
        # self._promises.pop(promise.ballot)
        # Highest-ballot-numbered value for each slot.
        slot_values = max_sv([p.voted for p in promises])
        # Choose new slots for the client's values.
        new_slot = max((sv.slot for sv in slot_values), default=0) + 1
        while self._client_requests:
            cr = self._client_requests.pop()
            slot_values.add(SlotValue(new_slot, cr.new_value))
            new_slot += 1

        accept = Accept(self._port, self._ballot_number, list(slot_values))
        self._send_to_all(self._accept_url, accept)

    def _handle_accepted(self, accepted: Accepted):
        # This is a Learner procedure, and Chand doesn't cover Learners.
        self._accepteds[accepted.ballot].append(accepted)
        accepteds = self._accepteds[accepted.ballot]
        if len(accepteds) <= len(self._config.nodes) // 2:
            # No majority yet.
            return

        undecided = self._min_undecided_slot()
        for sv in accepted.voted:
            self._decisions[sv.slot] = sv.value

        # Update the RSM with newly unblocked decisions.
        while self._decisions.get(undecided) is not None:
            self._perform(self._decisions[undecided])
            undecided += 1

        # TODO?
        # self._accepteds.pop(accepted.ballot)

    def _main_loop(self, q: queue.Queue[Agent._QEntry]) -> None:
        while True:
            entry = q.get()
            if isinstance(entry.message, ClientRequest):
                self._handle_client_request(entry.message)
            elif isinstance(entry.message, Promise):
                self._handle_promise(entry.message)
            elif isinstance(entry.message, Accepted):
                self._handle_accepted(entry.message)
            else:
                assert False, f"Unexpected {entry.message}"

    def _min_undecided_slot(self):
        return min(
            (s for s, v in self._decisions.items() if v is None), default=1)

    def _perform(self, new_value: int):
        """Actually """
        self._state.append(new_value)


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
            if slot_to_ballot_value.get(slot, (-1, -1))[0] < pvalue.ballot:
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
        self._ballot_number: Ballot = -1
        # Highest ballot voted for per slot. "aVoted" in Chand. Grows forever.
        self._voted: VotedSet = {}

    def _handle_prepare(self, prepare: Prepare) -> None:
        # Phase 1b, Fig. 3 in Chand.
        if prepare.ballot <= self._ballot_number:
            return

        self._ballot_number = prepare.ballot
        promise = Promise(self._port, self._ballot_number, self._voted)
        self._send_to_all(self._promise_url, promise)

    def _handle_accept(self, accept: Accept) -> None:
        # Phase 2b, Fig. 5 in Chand. Note < for accept and <= for prepare.
        if accept.ballot < self._ballot_number:
            # Ignore.
            return

        assert accept.ballot >= self._ballot_number
        self._ballot_number = accept.ballot
        # TODO: right?
        accept_voted_set = {sv.slot: PValue(accept.ballot, sv.slot, sv.value)
                            for sv in accept.voted}
        self._voted.update(accept_voted_set)
        accepted = Accepted(self._port, self._ballot_number, accept.voted)
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
