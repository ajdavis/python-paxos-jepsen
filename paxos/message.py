import dataclasses
import types
import typing
from dataclasses import dataclass
from typing import TypeAlias

__all__ = [
    "BallotNumber",
    "SlotNumber",
    "Value",
    "SV",
    "PValue",
    "VotedSet",
    "Message",
    "ClientRequest",
    "ClientReply",
    "Prepare",
    "Promise",
    "Accept",
    "Accepted",
]

BallotNumber: TypeAlias = int
SlotNumber: TypeAlias = int
Value: TypeAlias = int
SV: TypeAlias = tuple[SlotNumber, Value]
# TODO: make this a dataclass.
PValue: TypeAlias = tuple[BallotNumber, SlotNumber, Value]
VotedSet: TypeAlias = dict[SlotNumber, PValue]


@dataclass
class Message:
    @classmethod
    def from_dict(cls: typing.Type["Message"], dct: dict[str, typing.Any]):
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

            if issubclass(typ, Message) and isinstance(val, dict):
                return typ.from_dict(val)

            return typ(val)

        return cls(**{
            f.name: make_field(f.type, dct.get(f.name))
            for f in dataclasses.fields(cls)
        })


@dataclass(unsafe_hash=True)
class ClientRequest(Message):
    new_value: Value


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
    ballot: BallotNumber


@dataclass(unsafe_hash=True)
class Promise(Message):
    """Phase 1b message."""
    # "from" in Chand.
    from_port: int
    # "bal" in Chand.
    ballot: BallotNumber
    voted: VotedSet


@dataclass(unsafe_hash=True)
class Accept(Message):
    """Command an acceptor to accept! Phase 2a message."""
    # "from" in Chand.
    from_port: int
    # "bal" in Chand.
    ballot: BallotNumber
    # "propSV" in Chand. A logical set, but JSON requires a list.
    voted: list[SV]


@dataclass(unsafe_hash=True)
class Accepted(Accept):
    """Phase 2b message."""
