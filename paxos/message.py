import dataclasses
import types
import typing
from dataclasses import dataclass


@dataclass
class Message:
    @classmethod
    def from_dict(cls: typing.Type["Message"], dct: dict[str, typing.Any]):
        def make_field(typ, val):
            if val is None:
                return val

            if isinstance(typ, types.UnionType):
                # Like "thing | None".
                assert len(typ.__args__) == 2
                assert any(a for a in typ.__args__ if a is types.NoneType)
                typ = next(a for a in typ.__args__ if a is not types.NoneType)

            if isinstance(typ, typing.GenericAlias):
                container_class = typ.__origin__
                elem_class = typ.__args__[0]
                # Like list[int].
                if issubclass(container_class, typing.Sequence):
                    return container_class(
                        make_field(elem_class, v) for v in val)
                else:
                    assert False, f"Not implemented for {container_class}"

            if issubclass(typ, Message) and isinstance(val, dict):
                return typ.from_dict(val)

            return typ(val)

        return cls(**{
            f.name: make_field(f.type, dct.get(f.name))
            for f in dataclasses.fields(cls)
        })


@dataclass(unsafe_hash=True)
class ClientRequest(Message):
    client_id: int
    """Kappa in "Paxos Made Moderately Complex"."""
    command_id: int
    """cid in "Paxos Made Moderately Complex"."""
    new_value: int | None
    """op in "Paxos Made Moderately Complex".
    
    The operation is "append this int to the state, which is a list of ints.
    """
    new_leaders: list[str] | None
    """Present if this is a reconfiguration request."""


def is_reconfig(message: Message) -> bool:
    return (isinstance(message, ClientRequest)
            and message.new_leaders is not None)


@dataclass(unsafe_hash=True)
class ClientReply(Message):
    state: list[int]
    """Replicated state machine's new state."""


@dataclass(unsafe_hash=True)
class Proposal(Message):
    slot: int
    client_request: ClientRequest
    """c in "Paxos Made Moderately Complex"."""


@dataclass(unsafe_hash=True)
class Decision(Proposal):
    pass


@dataclass(unsafe_hash=True)
class DecisionReply(Message):
    pass
