import dataclasses
import typing
from dataclasses import dataclass

__all__ = [
    "Message",
    "ClientRequest",
    "ClientReply",
    "Proposal"
]


@dataclass
class Message:
    @classmethod
    def from_dict(cls: typing.Type["Message"], dct: dict[str, typing.Any]):
        def make_field(typ, val):
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

        types: dict[str, Message] = {
            f.name: f.type for f in dataclasses.fields(cls)}

        return cls(**{
            name: make_field(types[name], value)
            for name, value in dct.items()
        })


@dataclass
class ClientRequest(Message):
    client_id: int
    """Kappa in "Paxos Made Moderately Complex"."""
    command_id: int
    """cid in "Paxos Made Moderately Complex"."""
    new_value: int
    """op in "Paxos Made Moderately Complex".
    
    The operation is "append this int to the state, which is a list of ints.
    """


@dataclass
class ClientReply(Message):
    command_id: int
    """To which command this is the reply."""


@dataclass
class Proposal(Message):
    slot_in: int
    client_request: ClientRequest
    """c in "Paxos Made Moderately Complex"."""
