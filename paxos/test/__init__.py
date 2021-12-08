import unittest
from dataclasses import asdict, dataclass
from typing import Optional

from flask.json import dumps, loads

from message import *
from core import max_sv


@dataclass
class A(Message):
    value: int


@dataclass
class B(Message):
    a: A


@dataclass
class C(Message):
    bs: list[B]


@dataclass
class D(Message):
    voted: VotedSet


@dataclass
class E(Message):
    x: Optional[int]


class MessageTest(unittest.TestCase):
    def test_from_dict(self):
        for jsn, obj in [
            ('{"a": {"value": 1}}',
             B(A(1))),
            ('{"bs": [{"a": {"value": 1}}, {"a": {"value": 2}}]}',
             C([B(A(1)), B(A(2))])),
            ('{"ballot": {"ts": 2, "server_id": "foo"}, "from_uri": "host:1"}',
             Prepare("host:1", Ballot(2, "foo"))),
            ('''
             {
               "ballot": {
                 "ts": 2,
                 "server_id": "foo"
               },
               "from_uri": "host:1",
               "voted": {
                 "1": {
                   "ballot": {
                     "ts": 1,
                     "server_id": "foo"
                   },
                   "slot": 3,
                   "value": {
                     "client_id": 4,
                     "command_id": 5,
                     "payload": 6
                   }
                 }
               }
             }''',
             Promise("host:1", Ballot(2, "foo"),
                     {1: PValue(Ballot(1, "foo"), 3, Value(4, 5, 6))}))
        ]:
            with self.subTest(str(obj)):
                # Test from_dict.
                self.assertEqual(obj.__class__.from_dict(loads(jsn)), obj)
                # Test as_dict. Use dumps/loads to erase JSON whitespace diffs.
                self.assertEqual(dumps(asdict(obj)), dumps(loads(jsn)))

    def test_extra_field(self):
        with self.assertRaises(ValueError):
            A.from_dict({"a": 1, "extra": 2})

    def test_missing_field(self):
        with self.assertRaises(TypeError):
            A.from_dict({})

    def test_optional_present(self):
        self.assertEqual(E.from_dict({"x": 1}), E(1))

    def test_optional_absent(self):
        self.assertEqual(E.from_dict({}), E(None))

    def test_optional_type_error(self):
        with self.assertRaises(TypeError):
            # Should be int or None.
            E.from_dict({"x": "string"})


class MaxSVTest(unittest.TestCase):
    """Test the MaxSV operator from Fig. 4 of Chand."""

    def test_max_sv(self):
        self.assertEqual(
            {
                # (slot, value).
                SlotValue(1, Value(1, 2, 3)),
                SlotValue(2, Value(1, 2, 5)),
            },
            max_sv([{
                # slot: (ballot, slot, value).
                # Will be preempted by ballot 2 below.
                1: PValue(Ballot(1, ""), 1, Value(1, 2, 2)),
                # The chosen value for slot 2.
                2: PValue(Ballot(4, ""), 2, Value(1, 2, 5)),
            }, {
                # The chosen value for slot 1.
                1: PValue(Ballot(2, ""), 1, Value(1, 2, 3)),
            }, {
                # Preempted by ballot 4  above.
                2: PValue(Ballot(3, ""), 2, Value(1, 2, 11))
            }]))
