import unittest
from dataclasses import asdict, dataclass

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
    x: int | None


class MessageTest(unittest.TestCase):
    def test_from_dict(self):
        for jsn, obj in [
            ('{"a": {"value": 1}}',
             B(A(1))),
            ('{"bs": [{"a": {"value": 1}}, {"a": {"value": 2}}]}',
             C([B(A(1)), B(A(2))])),
            ('{"ballot": 2, "from_port": 5000}',
             Prepare(5000, 2)),
            ('{"ballot": 2, "from_port": 5000, "voted": {"1":'
             ' {"ballot": 1, "slot": 3, "value": 4}}}',
             Promise(5000, 2, {1: PValue(1, 3, 4)}))
        ]:
            with self.subTest(str(obj)):
                self.assertEqual(obj.__class__.from_dict(loads(jsn)), obj)
                self.assertEqual(dumps(asdict(obj)), jsn)

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
                SlotValue(1, 3),
                SlotValue(2, 5),
            },
            max_sv([{
                # slot: (ballot, slot, value).
                1: PValue(1, 1, 2),  # Will be preempted by ballot 2 below.
                2: PValue(4, 2, 5),  # The chosen value for slot 2.
            }, {
                1: PValue(2, 1, 3),  # The chosen value for slot 1.
            }, {
                2: PValue(3, 2, 11)  # Preempted by ballot 4  above.
            }]))
