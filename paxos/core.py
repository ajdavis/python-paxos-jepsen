import dataclasses
import logging
import queue
import threading
from dataclasses import dataclass

import requests


@dataclass
class Config:
    nodes: list[str]


@dataclass
class Message:
    pass


@dataclass
class ClientRequest(Message):
    new_value: int


ProposalNumber = list[int, int]
"""Proposal number type: [n, node_id].

Dataclass/tuple would be better, but list[int, int] roundtrips via JSON easily.
"""


@dataclass
class Prepare(Message):
    proposal_number: ProposalNumber


@dataclass
class PrepareOK(Message):
    proposal_number: ProposalNumber
    highest_accept: ProposalNumber
    accepted_value: int


class Node:
    def __init__(self, config: Config, port: int):
        self._config = config
        self._port = port
        self._inbox = queue.Queue()
        self._thread = threading.Thread(target=self._run)

    def receive(self, message: Message):
        logging.getLogger("paxos").info(
            "%s port %d got %s", self.__class__.__name__, self._port, message)
        self._inbox.put(message)
        # TODO: thread the reply back to caller

    def run(self):
        self._thread.start()

    def _run(self):
        while True:
            message: Message = self._inbox.get()
            reply = self._handle(message)
            # TODO: thread the reply back to caller

    def _send(self, node: str, url: str, message: Message, reply_type: type):
        try:
            reply = requests.post(f"http://{node}{url}",
                                  json=dataclasses.asdict(message))
        except requests.exceptions.RequestException as exc:
            logging.getLogger("requests").warning(exc)
        else:
            self.receive(reply_type(**reply.json))

    def _handle(self, message: Message):
        raise NotImplementedError()


class Proposer(Node):
    def __init__(self, config: Config, port: int, prepare_url: str):
        super().__init__(config, port)
        self._prepare_url = prepare_url
        try:
            self._id = next(i for i, node in enumerate(config.nodes)
                            if int(node.split(":")[1]) == port)
        except StopIteration:
            raise ValueError(f"Couldn't find port {port} in config: {config}")

        self._proposal_number = self._id

    def _handle(self, message: Message):
        assert isinstance(message, ClientRequest)
        prepare_message = Prepare(self.__next_proposal_number())
        for node in self._config.nodes:
            self._send(node, self._prepare_url, prepare_message, PrepareOK)

    def __next_proposal_number(self) -> ProposalNumber:
        self._proposal_number += 1
        return [self._proposal_number, self._id]


class Acceptor(Node):
    def __init__(self, config: Config, port: int):
        super().__init__(config, port)
        self._highest_prepare = [-1, -1]
        self._highest_accept = [-1, -1]
        self._accepted_value = None

    def _handle(self, message: Message):
        assert isinstance(message, Prepare)
        return PrepareOK(message.proposal_number,
                         self._highest_accept,
                         self._accepted_value)
