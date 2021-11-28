import dataclasses
import unittest
from dataclasses import dataclass

from ..message import Message


@dataclass
class A(Message):
    value: int


@dataclass
class B(Message):
    a: A


@dataclass
class C(Message):
    bs: list[B]


class MessageTest(unittest.TestCase):
    def test_from_dict(self):
        for json, obj in [
            ({"a": {"value": 1}},
             B(A(1))),
            ({"bs": [{"a": {"value": 1}}, {"a": {"value": 2}}]},
             C([B(A(1)), B(A(2))]))
        ]:
            with self.subTest(str(obj)):
                self.assertEqual(obj.__class__.from_dict(json), obj)
                self.assertEqual(dataclasses.asdict(obj), json)
