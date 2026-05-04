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

import unittest

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


if __name__ == "__main__":
    unittest.main()
