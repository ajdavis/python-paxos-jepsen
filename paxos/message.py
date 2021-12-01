import dataclasses
import functools
import types
import typing
from dataclasses import dataclass
from typing import TypeAlias

__all__ = [
    "Ballot",
    "Slot",
    "SlotValue",
    "PValue",
    "VotedSet",
    "Message",
    "Value",
    "ClientRequest",
    "ClientReply",
    "Prepare",
    "Promise",
    "Accept",
    "Accepted",
    "OK",
]

Slot: TypeAlias = int


@dataclass(unsafe_hash=True)
class JSONish:
    """Base class. Interoperates with JSON-ish dicts."""

    @classmethod
    def from_dict(cls: typing.Type["JSONish"], dct: dict[str, typing.Any]):
        fieldnames = set(f.name for f in dataclasses.fields(cls))
        if extra := set(dct.keys()) - fieldnames:
            raise ValueError(f"extra fields for {cls.__name__}: {extra}")

        def make_field(typ, val):
            if isinstance(typ, types.UnionType):
                # Like "thing | None".
                errors = []
                for subtype in typ.__args__:
                    if issubclass(subtype, types.NoneType) and val is None:
                        return None

                    try:
                        return subtype(val)
                    except Exception as exc:
                        errors.append(str(exc))

                raise TypeError(
                    f"can't convert {val} to {typ}: {', '.join(errors)}")

            if isinstance(typ, typing.GenericAlias):
                container_class = typ.__origin__
                # Like list[int].
                if issubclass(container_class, typing.Sequence):
                    elem_class = typ.__args__[0]
                    return container_class(
                        make_field(elem_class, v) for v in val)
                elif issubclass(container_class, typing.Dict):
                    key_class, value_class = typ.__args__
                    return container_class(
                        (key_class(k), make_field(value_class, v))
                        for k, v in val.items())
                else:
                    assert False, f"not implemented for {container_class}"

            if issubclass(typ, JSONish) and isinstance(val, dict):
                return typ.from_dict(val)

            return typ(val)

        return cls(**{
            f.name: make_field(f.type, dct.get(f.name))
            for f in dataclasses.fields(cls)
        })


@dataclass(unsafe_hash=True)
class Value(JSONish):
    client_id: int
    command_id: int
    payload: int


@functools.total_ordering
@dataclass(unsafe_hash=True)
class Ballot(JSONish):
    inc: int
    server_id: int

    @classmethod
    def min(cls):
        return cls(-1, -1)

    def __lt__(self, other):
        if not isinstance(other, Ballot):
            return NotImplemented

        return (self.inc, self.server_id) < (other.inc, other.server_id)


@dataclass(unsafe_hash=True)
class SlotValue(JSONish):
    """A (slot number, value) pair, called "SV" in Chand."""
    slot: Slot
    value: Value


@dataclass(unsafe_hash=True)
class PValue(JSONish):
    """As in Chand, a (ballot, slot, value) 3-tuple.

    I think the name means "proposal value".
    """
    ballot: Ballot
    slot: Slot
    value: Value


VotedSet: TypeAlias = dict[Slot, PValue]
"""Tracks how Acceptors have voted."""


@dataclass(unsafe_hash=True)
class Message(JSONish):
    """Base class for Paxos protocol requests and replies."""
    pass


@dataclass(unsafe_hash=True)
class ClientRequest(Message, Value):
    def get_value(self) -> Value:
        # In case a client request ever has more than a value.
        return Value(**dataclasses.asdict(self))


@dataclass(unsafe_hash=True)
class ClientReply(Message):
    state: list[int]
    """Replicated state machine's new state."""


@dataclass(unsafe_hash=True)
class Prepare(Message):
    """Phase 1a message."""
    # "from" in Chand.
    from_port: int
    # "bal" in Chand.
    ballot: Ballot


@dataclass(unsafe_hash=True)
class Promise(Message):
    """Phase 1b message."""
    # "from" in Chand.
    from_port: int
    # "bal" in Chand.
    ballot: Ballot
    voted: VotedSet


@dataclass(unsafe_hash=True)
class Accept(Message):
    """Command an acceptor to accept! Phase 2a message."""
    # "from" in Chand.
    from_port: int
    # "bal" in Chand.
    ballot: Ballot
    # "propSV" in Chand. A logical set, but JSON requires a list.
    voted: list[SlotValue]


@dataclass(unsafe_hash=True)
class Accepted(Accept):
    """Phase 2b message."""


@dataclass(unsafe_hash=True)
class OK(Message):
    """Acknowledge a message."""
    pass
