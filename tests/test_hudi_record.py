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

from hudi.fileformat import FileHeader, FileKind, HEADER_SIZE
from hudi.errors import CorruptFileError
from hudi.record import Record
from hudi.sortkey import FINGERPRINT_BYTES, SortKey


class RecordEqualityTests(unittest.TestCase):
    def test_equal_records_compare_equal(self):
        a = Record(primary_key=("k1",), payload={"x": 1}, sequence=7)
        b = Record(primary_key=("k1",), payload={"x": 1}, sequence=7)
        self.assertEqual(a, b)

    def test_different_sequence_not_equal(self):
        a = Record(primary_key=("k1",), payload={"x": 1}, sequence=1)
        b = Record(primary_key=("k1",), payload={"x": 1}, sequence=2)
        self.assertNotEqual(a, b)

    def test_primary_key_normalised_to_tuple(self):
        # Lists and tuples with the same contents must produce equal records.
        a = Record(primary_key=["k1", 2], payload={})
        b = Record(primary_key=("k1", 2), payload={})
        self.assertEqual(a.primary_key, ("k1", 2))
        self.assertEqual(a, b)


class RecordImmutabilityTests(unittest.TestCase):
    def test_dataclass_is_frozen(self):
        r = Record(primary_key=("k",), payload={"x": 1})
        with self.assertRaises(Exception):
            r.sequence = 99  # type: ignore[misc]

    def test_payload_is_read_only_view(self):
        r = Record(primary_key=("k",), payload={"x": 1})
        with self.assertRaises(TypeError):
            r.payload["x"] = 2  # type: ignore[index]

    def test_external_payload_mutation_does_not_leak_in(self):
        # Mutating the source dict after construction must not affect the
        # record's stored payload.
        src = {"x": 1}
        r = Record(primary_key=("k",), payload=src)
        src["x"] = 999
        self.assertEqual(r.payload["x"], 1)

    def test_nested_list_in_payload_is_frozen(self):
        # Regression: shallow freezing left mutable values reachable
        # through the read-only payload view, so a caller could still
        # mutate the record's state via the inner list and silently
        # change its equality / sort behavior.
        src = [1, 2, 3]
        r = Record(primary_key=("k",), payload={"x": src})
        # The list is converted to a tuple on the way in.
        self.assertEqual(r.payload["x"], (1, 2, 3))
        # The original list staying mutable must not affect the record.
        src.append(99)
        self.assertEqual(r.payload["x"], (1, 2, 3))
        # The stored value itself is no longer mutable.
        with self.assertRaises(AttributeError):
            r.payload["x"].append(99)  # type: ignore[attr-defined]

    def test_nested_dict_in_payload_is_frozen(self):
        inner = {"a": 1}
        r = Record(primary_key=("k",), payload={"x": inner})
        # External mutation must not leak in.
        inner["a"] = 999
        self.assertEqual(r.payload["x"]["a"], 1)
        # And the stored nested mapping is itself read-only.
        with self.assertRaises(TypeError):
            r.payload["x"]["a"] = 2  # type: ignore[index]

    def test_nested_set_in_payload_is_frozen(self):
        src = {1, 2, 3}
        r = Record(primary_key=("k",), payload={"x": src})
        self.assertEqual(r.payload["x"], frozenset({1, 2, 3}))
        # frozenset has no ``add`` method.
        with self.assertRaises(AttributeError):
            r.payload["x"].add(99)  # type: ignore[attr-defined]

    def test_nested_bytearray_in_payload_is_frozen(self):
        src = bytearray(b"abc")
        r = Record(primary_key=("k",), payload={"x": src})
        # Stored as bytes; mutating the source must not affect the record.
        src[0] = ord("Z")
        self.assertEqual(r.payload["x"], b"abc")

    def test_deeply_nested_mutation_does_not_leak_in(self):
        # Inner dict-inside-list-inside-dict is also frozen all the way down.
        inner = {"k": [1, 2]}
        r = Record(primary_key=("k",), payload={"x": [inner]})
        inner["k"].append(99)
        # Snapshot is unaffected by the post-construction mutation, and
        # every level along the way is the immutable equivalent of the
        # original container.
        outer = r.payload["x"]
        self.assertIsInstance(outer, tuple)
        self.assertEqual(len(outer), 1)
        nested = outer[0]
        self.assertEqual(dict(nested), {"k": (1, 2)})
        with self.assertRaises(TypeError):
            nested["k"] = "mutated"  # type: ignore[index]

    def test_primary_key_inner_list_is_frozen(self):
        # Mutable values inside the primary key would otherwise change
        # the record's identity after construction.
        inner = [1, 2]
        r = Record(primary_key=(inner,), payload={})
        self.assertEqual(r.primary_key, ((1, 2),))
        inner.append(99)
        self.assertEqual(r.primary_key, ((1, 2),))


class RecordSupersedesTests(unittest.TestCase):
    def test_higher_sequence_supersedes_same_key(self):
        old = Record(primary_key=("k",), payload={}, sequence=1)
        new = Record(primary_key=("k",), payload={}, sequence=2)
        self.assertTrue(new.supersedes(old))
        self.assertFalse(old.supersedes(new))

    def test_different_keys_never_supersede(self):
        a = Record(primary_key=("k1",), payload={}, sequence=10)
        b = Record(primary_key=("k2",), payload={}, sequence=1)
        self.assertFalse(a.supersedes(b))
        self.assertFalse(b.supersedes(a))


class FileHeaderRoundTripTests(unittest.TestCase):
    def test_serialise_deserialise_round_trip(self):
        sk = SortKey(columns=("id", "ts"))
        original = FileHeader(
            kind=FileKind.BASE_FILE,
            sort_key_fingerprint=sk.fingerprint(),
            record_count=42,
        )
        raw = original.to_bytes()
        self.assertEqual(len(raw), HEADER_SIZE)
        restored = FileHeader.from_bytes(raw)
        self.assertEqual(restored, original)

    def test_log_block_round_trip(self):
        fp = b"\x00" * FINGERPRINT_BYTES
        h = FileHeader(kind=FileKind.LOG_BLOCK, sort_key_fingerprint=fp, record_count=0)
        self.assertEqual(FileHeader.from_bytes(h.to_bytes()), h)

    def test_bad_magic_raises_corrupt(self):
        fp = b"\x00" * FINGERPRINT_BYTES
        good = FileHeader(
            kind=FileKind.BASE_FILE, sort_key_fingerprint=fp, record_count=1
        ).to_bytes()
        bad = b"XXXX" + good[4:]
        with self.assertRaises(CorruptFileError):
            FileHeader.from_bytes(bad)

    def test_truncated_header_raises_corrupt(self):
        with self.assertRaises(CorruptFileError):
            FileHeader.from_bytes(b"HUDI")

    def test_unknown_kind_raises_corrupt(self):
        fp = b"\x00" * FINGERPRINT_BYTES
        good = FileHeader(
            kind=FileKind.BASE_FILE, sort_key_fingerprint=fp, record_count=1
        ).to_bytes()
        # Replace the kind byte (offset 6) with an unsupported value.
        broken = good[:6] + bytes([99]) + good[7:]
        with self.assertRaises(CorruptFileError):
            FileHeader.from_bytes(broken)

    def test_bad_fingerprint_length_rejected_at_construction(self):
        with self.assertRaises(ValueError):
            FileHeader(
                kind=FileKind.BASE_FILE,
                sort_key_fingerprint=b"\x00" * (FINGERPRINT_BYTES - 1),
                record_count=0,
            )


if __name__ == "__main__":
    unittest.main()
