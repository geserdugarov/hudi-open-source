#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import math
import unittest

from hudi.errors import SortOrderViolation
from hudi.record import Record
from hudi.sortkey import FINGERPRINT_BYTES, SortKey


def rec(*key, **payload):
    seq = payload.pop("__seq", 0)
    return Record(primary_key=key, payload=payload, sequence=seq)


class SortKeyConstructionTests(unittest.TestCase):
    def test_empty_columns_rejected(self):
        with self.assertRaises(ValueError):
            SortKey(columns=())

    def test_descending_length_must_match(self):
        with self.assertRaises(ValueError):
            SortKey(columns=("a", "b"), descending=(True,))

    def test_default_descending_is_all_ascending(self):
        sk = SortKey(columns=("a", "b"))
        self.assertEqual(sk.descending, (False, False))


class SortKeyExtractTests(unittest.TestCase):
    def test_extract_pulls_columns_in_order(self):
        sk = SortKey(columns=("region", "id"))
        r = rec("ignored", region="us", id=42, extra="zz")
        self.assertEqual(sk.extract(r), ("us", 42))

    def test_missing_column_yields_none(self):
        sk = SortKey(columns=("region", "id"))
        r = rec(0, id=1)
        self.assertEqual(sk.extract(r), (None, 1))


class SortKeyOrderingTests(unittest.TestCase):
    def test_ascending_total_order(self):
        sk = SortKey(columns=("id",))
        records = [rec(i, id=i) for i in (3, 1, 4, 1, 5, 9, 2, 6)]
        ordered = sorted(records, key=sk.sort_tuple)
        self.assertEqual([r.payload["id"] for r in ordered], [1, 1, 2, 3, 4, 5, 6, 9])

    def test_descending_column_reverses(self):
        sk = SortKey(columns=("id",), descending=(True,))
        records = [rec(i, id=i) for i in (3, 1, 2)]
        ordered = sorted(records, key=sk.sort_tuple)
        self.assertEqual([r.payload["id"] for r in ordered], [3, 2, 1])

    def test_composite_key_sorts_lexicographically(self):
        sk = SortKey(columns=("region", "id"))
        records = [
            rec(1, region="us", id=2),
            rec(2, region="eu", id=10),
            rec(3, region="us", id=1),
            rec(4, region="eu", id=1),
        ]
        ordered = sorted(records, key=sk.sort_tuple)
        self.assertEqual(
            [(r.payload["region"], r.payload["id"]) for r in ordered],
            [("eu", 1), ("eu", 10), ("us", 1), ("us", 2)],
        )

    def test_mixed_direction_composite_key(self):
        sk = SortKey(columns=("region", "id"), descending=(False, True))
        records = [
            rec(1, region="us", id=1),
            rec(2, region="us", id=2),
            rec(3, region="eu", id=5),
        ]
        ordered = sorted(records, key=sk.sort_tuple)
        self.assertEqual(
            [(r.payload["region"], r.payload["id"]) for r in ordered],
            [("eu", 5), ("us", 2), ("us", 1)],
        )

    def test_compare_returns_signed_int(self):
        sk = SortKey(columns=("id",))
        a = rec(0, id=1)
        b = rec(0, id=2)
        self.assertEqual(sk.compare(a, b), -1)
        self.assertEqual(sk.compare(b, a), 1)
        self.assertEqual(sk.compare(a, a), 0)

    def test_none_sorts_first_under_total_order(self):
        sk = SortKey(columns=("id",))
        records = [rec(0, id=2), rec(0, id=None), rec(0, id=1)]
        ordered = sorted(records, key=sk.sort_tuple)
        self.assertEqual([r.payload["id"] for r in ordered], [None, 1, 2])

    def test_heterogeneous_types_dont_raise(self):
        # Native sort would raise TypeError comparing int with str. The
        # SortKey wrapper buckets by type-name and produces a stable order.
        sk = SortKey(columns=("id",))
        records = [rec(0, id=1), rec(0, id="a"), rec(0, id=2)]
        ordered = sorted(records, key=sk.sort_tuple)
        self.assertEqual(len(ordered), 3)
        # All ints come before all strs (qualname "int" < "str").
        self.assertEqual([type(r.payload["id"]).__name__ for r in ordered],
                         ["int", "int", "str"])
        self.assertEqual([r.payload["id"] for r in ordered[:2]], [1, 2])


class SortKeyTotalOrderEdgeCaseTests(unittest.TestCase):
    """Native ``<`` is partial or undefined for several common Python types
    and for NaN. The SortKey must still produce a true total order."""

    def test_nan_does_not_violate_antisymmetry(self):
        # Native float comparison: nan < x and x < nan are both False, so a
        # naive ``compare`` could return +1 in both directions. The wrapper
        # must put NaN in its own deterministic bucket.
        sk = SortKey(columns=("v",))
        a = Record(primary_key=(0,), payload={"v": float("nan")}, sequence=0)
        b = Record(primary_key=(1,), payload={"v": 1.0}, sequence=0)
        self.assertEqual(sk.compare(a, b), -sk.compare(b, a))
        # NaN deterministically sorts after real floats.
        self.assertGreater(sk.compare(a, b), 0)

    def test_two_nans_compare_equal(self):
        sk = SortKey(columns=("v",))
        a = Record(primary_key=(0,), payload={"v": float("nan")}, sequence=0)
        b = Record(primary_key=(1,), payload={"v": float("nan")}, sequence=0)
        self.assertEqual(sk.compare(a, b), 0)

    def test_nan_and_inf_order(self):
        # +inf is a real float; NaN must sort strictly after it.
        sk = SortKey(columns=("v",))
        records = [
            Record(primary_key=(i,), payload={"v": v}, sequence=0)
            for i, v in enumerate([float("nan"), 0.0, float("inf"), -1.0])
        ]
        ordered = sorted(records, key=sk.sort_tuple)
        values = [r.payload["v"] for r in ordered]
        # First three are real floats in ascending order, last is NaN.
        self.assertEqual(values[:3], [-1.0, 0.0, float("inf")])
        self.assertTrue(math.isnan(values[3]))

    def test_complex_value_rejected(self):
        sk = SortKey(columns=("v",))
        r = Record(primary_key=(0,), payload={"v": 1 + 2j}, sequence=0)
        with self.assertRaises(SortOrderViolation):
            sk.sort_tuple(r)

    def test_dict_value_rejected(self):
        # ``dict`` has no ``<`` at all in Python 3.
        sk = SortKey(columns=("v",))
        r = Record(primary_key=(0,), payload={"v": {"a": 1}}, sequence=0)
        with self.assertRaises(SortOrderViolation):
            sk.compare(r, r)

    def test_set_value_rejected(self):
        # ``set.__lt__`` is subset semantics — only a partial order.
        sk = SortKey(columns=("v",))
        r = Record(primary_key=(0,), payload={"v": {1, 2}}, sequence=0)
        with self.assertRaises(SortOrderViolation):
            sk.sort_tuple(r)

    def test_frozenset_value_rejected(self):
        sk = SortKey(columns=("v",))
        r = Record(primary_key=(0,), payload={"v": frozenset([1, 2])}, sequence=0)
        with self.assertRaises(SortOrderViolation):
            sk.sort_tuple(r)

    def test_extract_does_not_validate(self):
        # ``extract`` returns raw values for downstream use; validation
        # only happens when the wrapper has to produce an ordering.
        sk = SortKey(columns=("v",))
        r = Record(primary_key=(0,), payload={"v": {"a": 1}}, sequence=0)
        self.assertEqual(sk.extract(r), ({"a": 1},))

    def test_heterogeneous_list_does_not_raise(self):
        # Native list/tuple compare delegates to element ``<``, which would
        # raise TypeError between e.g. int and str. The wrapper recurses so
        # inner elements get the same type-bucket treatment.
        sk = SortKey(columns=("v",))
        a = Record(primary_key=(0,), payload={"v": [1, "x"]}, sequence=0)
        b = Record(primary_key=(1,), payload={"v": [1, 2]}, sequence=0)
        self.assertEqual(sk.compare(a, b), -sk.compare(b, a))
        self.assertNotEqual(sk.compare(a, b), 0)

    def test_nested_nan_in_list_preserves_antisymmetry(self):
        # NaN inside a list would let native tuple compare report both
        # directions as "not less" / "not equal". The recursive normalize
        # buckets nested NaNs the same way as top-level ones.
        sk = SortKey(columns=("v",))
        a = Record(primary_key=(0,), payload={"v": [float("nan"), 1.0]}, sequence=0)
        b = Record(primary_key=(1,), payload={"v": [1.0, 1.0]}, sequence=0)
        self.assertEqual(sk.compare(a, b), -sk.compare(b, a))

    def test_two_lists_with_nan_compare_equal(self):
        sk = SortKey(columns=("v",))
        a = Record(primary_key=(0,), payload={"v": [float("nan"), 1.0]}, sequence=0)
        b = Record(primary_key=(1,), payload={"v": [float("nan"), 1.0]}, sequence=0)
        self.assertEqual(sk.compare(a, b), 0)

    def test_unorderable_value_nested_in_list_is_rejected(self):
        sk = SortKey(columns=("v",))
        r = Record(primary_key=(0,), payload={"v": [1, frozenset([2])]}, sequence=0)
        with self.assertRaises(SortOrderViolation):
            sk.sort_tuple(r)

    def test_unorderable_value_nested_in_tuple_is_rejected(self):
        sk = SortKey(columns=("v",))
        r = Record(primary_key=(0,), payload={"v": (1, {"a": 1})}, sequence=0)
        with self.assertRaises(SortOrderViolation):
            sk.sort_tuple(r)

    def test_bare_object_value_rejected(self):
        # ``object()`` instances have no ``__lt__`` — comparing two of them
        # raises a raw ``TypeError`` mid-sort, so reject up front instead.
        sk = SortKey(columns=("v",))
        r = Record(primary_key=(0,), payload={"v": object()}, sequence=0)
        with self.assertRaises(SortOrderViolation):
            sk.sort_tuple(r)

    def test_range_value_rejected(self):
        # ``range`` defines no ordering; same failure mode as ``object()``.
        sk = SortKey(columns=("v",))
        r = Record(primary_key=(0,), payload={"v": range(3)}, sequence=0)
        with self.assertRaises(SortOrderViolation):
            sk.sort_tuple(r)

    def test_user_class_without_lt_rejected(self):
        class Opaque:
            pass

        sk = SortKey(columns=("v",))
        r = Record(primary_key=(0,), payload={"v": Opaque()}, sequence=0)
        with self.assertRaises(SortOrderViolation):
            sk.sort_tuple(r)

    def test_unorderable_value_nested_under_list_is_rejected(self):
        # Same defence applies inside list/tuple payloads.
        sk = SortKey(columns=("v",))
        r = Record(primary_key=(0,), payload={"v": [1, object()]}, sequence=0)
        with self.assertRaises(SortOrderViolation):
            sk.sort_tuple(r)

    def test_user_class_with_only_lt_rejected(self):
        # Regression: a class whose ``__lt__`` always returns ``False`` and
        # which inherits the default identity-based ``__eq__`` slips past a
        # naïve "does ``<`` raise?" probe. ``functools.total_ordering``
        # synthesises ``__gt__`` as ``not (a < b) and a != b``, so two
        # distinct instances satisfy *both* ``a > b`` and ``b > a`` and
        # ``compare`` returns ``+1`` in both directions — not a total
        # order. Reject the class up front instead.
        class HalfOrdered:
            def __lt__(self, other):
                return False

        sk = SortKey(columns=("v",))
        a = Record(primary_key=(0,), payload={"v": HalfOrdered()}, sequence=0)
        b = Record(primary_key=(1,), payload={"v": HalfOrdered()}, sequence=0)
        with self.assertRaises(SortOrderViolation):
            sk.sort_tuple(a)
        with self.assertRaises(SortOrderViolation):
            sk.compare(a, b)

    def test_user_class_with_lt_but_default_eq_rejected(self):
        # Even a "reasonable" ``__lt__`` is unsafe when paired with the
        # default identity ``__eq__``: two equal-by-content instances tie
        # under ``<`` but compare unequal under ``==``, so the synthesised
        # ``__gt__`` again misfires. Require both to be overridden.
        class OnlyLt:
            def __init__(self, n):
                self.n = n

            def __lt__(self, other):
                return self.n < other.n

        sk = SortKey(columns=("v",))
        r = Record(primary_key=(0,), payload={"v": OnlyLt(1)}, sequence=0)
        with self.assertRaises(SortOrderViolation):
            sk.sort_tuple(r)

    def test_naive_and_aware_datetime_compare_raises_sort_order_violation(self):
        # Regression: ``_normalize`` only probed ``value < value``, which
        # both a naive and a timezone-aware ``datetime`` pass individually,
        # so each call to ``sort_tuple`` succeeded. But pairwise the two
        # raise ``TypeError: can't compare offset-naive and offset-aware
        # datetimes`` in CPython, which leaked out of ``compare()`` /
        # ``sorted(..., key=sort_tuple)`` instead of becoming the
        # documented ordering-violation error.
        import datetime

        sk = SortKey(columns=("ts",))
        naive = datetime.datetime(2024, 1, 1)
        aware = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        a = Record(primary_key=(0,), payload={"ts": naive}, sequence=0)
        b = Record(primary_key=(1,), payload={"ts": aware}, sequence=0)
        # Each value individually still normalises (self-compare succeeds).
        sk.sort_tuple(a)
        sk.sort_tuple(b)
        # But the cross-instance comparison must surface as the documented
        # ordering-violation error in both directions and through ``sorted``.
        with self.assertRaises(SortOrderViolation):
            sk.compare(a, b)
        with self.assertRaises(SortOrderViolation):
            sk.compare(b, a)
        with self.assertRaises(SortOrderViolation):
            sorted([a, b], key=sk.sort_tuple)

    def test_distinct_classes_with_same_qualname_dont_collide(self):
        # Regression: bucketing by ``__qualname__`` alone let two distinct
        # classes that share a qualname (factory-produced classes both
        # named ``make_class.<locals>.C``) fall through to a raw
        # cross-class ``<``. With each class's ``__lt__`` returning
        # ``False`` for "other" instances, both ``compare(a, b)`` and
        # ``compare(b, a)`` came back as ``+1`` — not a total order.
        from functools import total_ordering

        def make_class():
            @total_ordering
            class C:
                def __init__(self, n):
                    self.n = n

                def __eq__(self, other):
                    return isinstance(other, type(self)) and self.n == other.n

                def __lt__(self, other):
                    return isinstance(other, type(self)) and self.n < other.n

                def __hash__(self):
                    return hash(self.n)

            return C

        A = make_class()
        B = make_class()
        # Sanity: distinct classes that share __qualname__.
        self.assertEqual(A.__qualname__, B.__qualname__)
        self.assertIsNot(A, B)

        sk = SortKey(columns=("v",))
        a = Record(primary_key=(0,), payload={"v": A(1)}, sequence=0)
        b = Record(primary_key=(1,), payload={"v": B(1)}, sequence=0)
        ab = sk.compare(a, b)
        ba = sk.compare(b, a)
        # Strict antisymmetry and trichotomy: distinct classes are never
        # equal under this wrapper, and the two directions must be opposite.
        self.assertNotEqual(ab, 0)
        self.assertEqual(ab, -ba)

    def test_decimal_nan_value_rejected(self):
        # Regression: ``_normalize`` previously caught only ``TypeError``
        # from the ``value < value`` probe, but ``Decimal('NaN')`` raises
        # ``decimal.InvalidOperation`` (an ``ArithmeticError``, not a
        # ``TypeError``). The probe leaked that out of ``sort_tuple``
        # instead of converting it to the documented ordering-violation
        # error, so callers got an exception outside the
        # :class:`HudiError` hierarchy.
        from decimal import Decimal

        sk = SortKey(columns=("v",))
        r = Record(primary_key=(0,), payload={"v": Decimal("NaN")}, sequence=0)
        with self.assertRaises(SortOrderViolation):
            sk.sort_tuple(r)
        with self.assertRaises(SortOrderViolation):
            sk.compare(r, r)

    def test_decimal_signaling_nan_value_rejected(self):
        # ``Decimal('sNaN')`` raises ``InvalidOperation`` even more
        # eagerly than quiet NaN. Make sure the same broadened catch
        # converts it into the documented ordering-violation error.
        from decimal import Decimal

        sk = SortKey(columns=("v",))
        r = Record(primary_key=(0,), payload={"v": Decimal("sNaN")}, sequence=0)
        with self.assertRaises(SortOrderViolation):
            sk.sort_tuple(r)

    def test_user_class_with_broken_lt_falls_back_to_total_order(self):
        # Regression: a class can override BOTH ``__lt__`` and ``__eq__``
        # (passing the strict signature check and the per-value ``<``
        # probe) and still ship a semantically broken ``__lt__`` — the
        # canonical case is ``__lt__`` always returning ``False`` paired
        # with an identity-based ``__eq__``. Two distinct instances then
        # satisfy neither ``a < b`` nor ``b < a`` nor ``a == b``, and a
        # naive comparator returns ``+1`` in both directions, violating
        # the issue's total-order contract. ``_FallbackOrdered`` wraps
        # the value with an ``id()``-based tiebreaker that fires only
        # when the user's own ``<`` and ``==`` both fail to decide, so
        # the comparator stays antisymmetric and trichotomous.
        class BrokenLt:
            def __lt__(self, other):
                return False

            def __eq__(self, other):
                return self is other

            def __hash__(self):
                return id(self)

        sk = SortKey(columns=("v",))
        a = Record(primary_key=(0,), payload={"v": BrokenLt()}, sequence=0)
        b = Record(primary_key=(1,), payload={"v": BrokenLt()}, sequence=0)
        ab = sk.compare(a, b)
        ba = sk.compare(b, a)
        # Antisymmetry: the two directions must be exact opposites, and
        # distinct instances must never silently tie under a broken ``<``.
        self.assertNotEqual(ab, 0)
        self.assertEqual(ab, -ba)
        # Self-compare still returns 0 — the user's identity ``__eq__``
        # decides before the fallback fires.
        self.assertEqual(sk.compare(a, a), 0)
        # ``sorted`` must produce a stable, total order over a population
        # of such broken-but-overridden values.
        records = [
            Record(primary_key=(i,), payload={"v": BrokenLt()}, sequence=0)
            for i in range(5)
        ]
        ordered = sorted(records, key=sk.sort_tuple)
        self.assertEqual(len(ordered), 5)
        # Two passes over the same input must yield the same order.
        self.assertEqual(
            [id(r) for r in ordered],
            [id(r) for r in sorted(records, key=sk.sort_tuple)],
        )

    def test_user_class_with_lt_is_accepted(self):
        # Sanity check: a value that does implement ``__lt__`` still works.
        from functools import total_ordering

        @total_ordering
        class Boxed:
            def __init__(self, n):
                self.n = n
            def __eq__(self, other):
                return isinstance(other, Boxed) and self.n == other.n
            def __lt__(self, other):
                return isinstance(other, Boxed) and self.n < other.n
            def __hash__(self):
                return hash(self.n)

        sk = SortKey(columns=("v",))
        a = Record(primary_key=(0,), payload={"v": Boxed(1)}, sequence=0)
        b = Record(primary_key=(1,), payload={"v": Boxed(2)}, sequence=0)
        self.assertEqual(sk.compare(a, b), -1)
        self.assertEqual(sk.compare(b, a), 1)
        self.assertEqual(sk.compare(a, a), 0)


class SortKeyFingerprintTests(unittest.TestCase):
    def test_fingerprint_is_fixed_size(self):
        fp = SortKey(columns=("a",)).fingerprint()
        self.assertEqual(len(fp), FINGERPRINT_BYTES)

    def test_fingerprint_is_deterministic(self):
        a = SortKey(columns=("a", "b")).fingerprint()
        b = SortKey(columns=("a", "b")).fingerprint()
        self.assertEqual(a, b)

    def test_fingerprint_distinguishes_columns(self):
        a = SortKey(columns=("a", "b")).fingerprint()
        b = SortKey(columns=("b", "a")).fingerprint()
        self.assertNotEqual(a, b)

    def test_fingerprint_distinguishes_direction(self):
        a = SortKey(columns=("a",), descending=(False,)).fingerprint()
        b = SortKey(columns=("a",), descending=(True,)).fingerprint()
        self.assertNotEqual(a, b)

    def test_fingerprint_is_injective_across_separator_collisions(self):
        # Regression: a previous encoding concatenated columns with raw
        # NUL bytes and no length prefix, so a single column whose name
        # embedded the separator could collide with a multi-column spec.
        a = SortKey(columns=("a", "b")).fingerprint()
        b = SortKey(columns=("a\x00\x00b",)).fingerprint()
        self.assertNotEqual(a, b)

    def test_fingerprint_distinguishes_split_point(self):
        # ``ab`` as one column must not hash the same as ``a`` + ``b``.
        a = SortKey(columns=("ab",)).fingerprint()
        b = SortKey(columns=("a", "b")).fingerprint()
        self.assertNotEqual(a, b)

    def test_fingerprint_distinguishes_direction_byte_in_name(self):
        # A name that ends with the direction-flag byte must not collide
        # with the same name written under the opposite direction.
        a = SortKey(columns=("a\x01",), descending=(False,)).fingerprint()
        b = SortKey(columns=("a",), descending=(True,)).fingerprint()
        self.assertNotEqual(a, b)


if __name__ == "__main__":
    unittest.main()
