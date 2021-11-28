import dataclasses
import logging
import threading
from dataclasses import dataclass
from typing import Optional

from network import send


@dataclass
class Config:
    nodes: list[str]


@dataclass
class Message:
    pass


@dataclass
class ClientRequest(Message):
    new_value: int


@dataclass
class ClientReply(Message):
    new_value: int


ProposalNumber = list[int, int]
"""Proposal number type: [n, node_id].

Dataclass/tuple would be better, but list[int, int] roundtrips via JSON easily.
"""


@dataclass
class Prepare(Message):
    """Phase 1a message."""
    proposal_number: ProposalNumber


@dataclass
class PrepareOK(Message):
    """Phase 1b message."""
    proposal_number: ProposalNumber
    highest_accept: ProposalNumber
    accepted_value: int


@dataclass
class Accept(Message):
    """Phase 2a message."""
    proposal_number: ProposalNumber
    value: int


@dataclass
class AcceptOK(Message):
    """Phase 2b message."""
    proposal_number: ProposalNumber


def send_message(node: str, url: str, message: Message) -> Optional[dict]:
    return send(node, url, dataclasses.asdict(message))


class Node:
    def __init__(self, config: Config, port: int):
        self._config = config
        self._port = port

    def receive(self, message: Message) -> Message:
        """Handle a request and return the reply."""
        logging.getLogger("paxos").info(
            "%s port %d got %s", self.__class__.__name__, self._port,
            message)
        return dataclasses.asdict(self._handle(message))

    def _handle(self, message: Message) -> Message:
        raise NotImplementedError()


class Coordinator(Node):
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
        self._value: Optional[int] = None
        self._mutex = threading.Lock()

    def _handle(self, message: Message) -> ClientReply:
        assert isinstance(message, ClientRequest)

        with self._mutex:
            self._value = None
            self._highest_round += 1
            # Save in case it changes when we release the mutex.
            highest_round = self._highest_round
            prepare_message = Prepare([highest_round, self._id])

        prepare_oks = []
        # TODO: parallelize
        for node in self._config.nodes:
            if reply := send_message(node, self._prepare_url, prepare_message):
                prepare_oks.append(PrepareOK(**reply))

        if len(prepare_oks) <= len(self._config.nodes) // 2:
            raise ValueError("prepare failed, didn't get a majority")

        highest_prepare_ok = max(prepare_oks, key=lambda p: p.highest_accept)

        with self._mutex:
            if self._highest_round != highest_round or self._value is not None:
                raise ValueError(
                    f"round {highest_round} preempted by {self._highest_round}")

            self._value = (message.new_value
                           if highest_prepare_ok.accepted_value is None
                           else highest_prepare_ok.accepted_value)

            decided_value = self._value == message.new_value
            accept_message = Accept([highest_round, self._id], self._value)

        accept_oks = []
        # TODO: parallelize
        for node in self._config.nodes:
            if reply := send_message(node, self._accept_url, accept_message):
                accept_oks.append(AcceptOK(**reply))

        if len(accept_oks) <= len(self._config.nodes) // 2:
            raise ValueError("accept failed, didn't get a majority")

        if not decided_value:
            raise ValueError("accept failed, preempted by another proposal")

        return ClientReply(message.new_value)


class Acceptor(Node):
    """Just the Acceptor role."""

    def __init__(self, config: Config, port: int):
        super().__init__(config, port)
        self._highest_prepare: ProposalNumber = [-1, -1]
        self._highest_accept: ProposalNumber = [-1, -1]
        self._accepted_value: Optional[int] = None
        self._mutex = threading.Lock()

    def _handle(self, message: Message) -> PrepareOK | AcceptOK:
        with self._mutex:
            if isinstance(message, Prepare):
                pn = message.proposal_number
                if pn > self._highest_prepare:
                    self._highest_prepare = pn
                    return PrepareOK(
                        pn, self._highest_accept, self._accepted_value)

                raise ValueError("stale prepare message")
            else:
                assert isinstance(message, Accept)
                pn = message.proposal_number
                if (pn >= self._highest_prepare and pn != self._highest_accept):
                    self._highest_prepare = self._highest_accept = pn
                    self._accepted_value = message.value
                    return AcceptOK(pn)

                raise ValueError("stale accept message")
