import dataclasses
import logging
import queue
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Sequence, Type, cast

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

    def _send_to_all(
        self,
        url: str,
        message: Message,
        reply_type: Type[Message]
    ) -> list[Message | None]:
        """Send message to all nodes and get replies, omitting errors."""
        replies = send_to_all(
            self._config.nodes, url, dataclasses.asdict(message))
        return [reply_type.from_dict(reply)
                for reply in replies if reply is not None]


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
        self._ballot_number: BallotNumber = 0
        # Clients waiting for a response.
        self._futures: dict[BallotNumber, Future[Message]] = {}
        # The replicated state machine (RSM) is just an appendable list of ints.
        self._state: list[int] = []

    def _main_loop(self, q: queue.Queue[Agent._QEntry]) -> None:
        while True:
            entry = q.get()
            if isinstance(entry.message, ClientRequest):
                # Phase 1a, Fig. 2 of Chand.
                # TODO: right?
                self._ballot_number += 1
                prepare = Prepare(self._port, self._ballot_number)
                # TODO: Less atomic. Put replies in self._q.
                promises = cast(
                    list[Promise],
                    self._send_to_all(self._propose_url, prepare, Promise))

                # Phase 2a, Fig. 4 of Chand.
                if len(promises) <= len(self._config.nodes) // 2:
                    entry.reply_future.set_exception(
                        ValueError("phase 1 no majority"))
                    continue

                # Highest-ballot-numbered value for each slot.
                slot_values = max_sv([p.voted for p in promises])
                # Choose a new slot for the client's value.
                new_slot = max((sv[0] for sv in slot_values), default=0) + 1
                slot_values.add((new_slot, entry.message.new_value))

                accept = Accept(
                    self._port, self._ballot_number, list(slot_values))
                accepteds = cast(
                    list[Accepted],
                    self._send_to_all(self._accept_url, accept, Accepted))
                if len(accepteds) <= len(self._config.nodes) // 2:
                    entry.reply_future.set_exception(
                        ValueError("phase 2 no majority"))
                    continue

                # Update the replicated state machine (RSM).
                self._perform(entry.message.new_value)
                entry.reply_future.set_result(ClientReply(self._state))
                # We'll resolve the future / reply to client eventually.
                # TODO: needed?
                # self._futures[self._ballot_number] = entry.reply_future
            else:
                assert False, f"Unexpected {entry.message}"

    def _perform(self, new_value: int):
        self._state.append(new_value)


# Fig. 4 of Chand, auxiliary operators.
def max_sv(vs: Sequence[VotedSet]) -> set[SV]:
    """(slot, val) with highest-ballot-numbered value for each slot.

    Incoming vs is a list of dicts, which map slot to highest-balloted PValue,
    which is (ballot, slot, value). For each slot, choose highest-balloted
    PValue among all dicts, and return (slot, value).

    See test_max_sv().
    """
    slot_to_ballot_value: dict[SlotNumber, tuple[BallotNumber, Value]] = {}
    for v in vs:
        for slot, pvalue in v.items():
            assert pvalue[1] == slot
            ballot, _, value = pvalue
            if slot_to_ballot_value.get(slot, (-1, -1))[0] < ballot:
                slot_to_ballot_value[slot] = (ballot, value)

    return {(slot, value) for slot, (_, value) in slot_to_ballot_value.items()}


class Acceptor(Agent):
    """Fulfills only the Acceptor role."""
    def __init__(self, config: Config, port: int):
        super().__init__(config, port)
        # Highest ballot seen. "aBal" in Chand.
        self._ballot_number: BallotNumber = -1
        # Highest ballot voted for per slot. "aVoted" in Chand. Grows forever.
        self._voted: VotedSet = {}

    def _handle_prepare(self, prepare: Prepare,
                        future: Future[Message]) -> None:
        # Phase 1b, Fig. 3 in Chand.
        if prepare.ballot <= self._ballot_number:
            future.set_exception(ValueError("stale prepare"))
            return

        self._ballot_number = prepare.ballot
        promise = Promise(self._port, self._ballot_number, self._voted)
        future.set_result(promise)

    def _handle_accept(self, accept: Accept, future: Future[Message]) -> None:
        # Phase 2b, Fig. 5 in Chand. Note < for accept and <= for prepare.
        if accept.ballot < self._ballot_number:
            future.set_exception(ValueError("stale accept"))
            return

        assert accept.ballot >= self._ballot_number
        self._ballot_number = accept.ballot
        # TODO: right?
        accept_voted_set = {slot: (accept.ballot, slot, value)
                            for slot, value in accept.voted}
        self._voted.update(accept_voted_set)
        accepted = Accepted(self._port, self._ballot_number, accept.voted)
        future.set_result(accepted)

    def _main_loop(self, q: queue.Queue[Agent._QEntry]) -> None:
        while True:
            entry = q.get()
            if isinstance(entry.message, Prepare):
                self._handle_prepare(entry.message, entry.reply_future)
            else:
                assert isinstance(entry.message, Accept)
                self._handle_accept(entry.message, entry.reply_future)
