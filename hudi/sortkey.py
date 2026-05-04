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
"""Composite sort/primary-key spec used to order records on disk."""

import hashlib
from dataclasses import dataclass, field
from functools import total_ordering
from typing import Any, Tuple

from hudi.record import Record

FINGERPRINT_BYTES = 16


@total_ordering
class _OrderableValue:
    """Wraps a column value so comparisons never raise.

    ``None`` always sorts first; otherwise values are compared first by
    their type's qualified name, then by value. This gives a deterministic
    total order even across heterogeneous column types.
    """

    __slots__ = ("_is_none", "_type", "_value")

    def __init__(self, value: Any) -> None:
        self._is_none = value is None
        self._type = "" if self._is_none else type(value).__qualname__
        self._value = value

    def _key(self) -> Tuple[int, str, Any]:
        # (none-flag, type-name, value) — tuples compare lexicographically.
        return (0 if self._is_none else 1, self._type, self._value)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _OrderableValue):
            return NotImplemented
        return self._key() == other._key()

    def __lt__(self, other: "_OrderableValue") -> bool:
        if not isinstance(other, _OrderableValue):
            return NotImplemented
        a, b = self._key(), other._key()
        if a[:2] != b[:2]:
            return a[:2] < b[:2]
        # Same type bucket: fall back to native comparison.
        return self._value < other._value


@dataclass(frozen=True)
class SortKey:
    """Names the columns (and direction) that form the composite key.

    The same SortKey instance is used to (a) extract a key tuple from any
    record, (b) compare two records in total order, and (c) generate a
    short fingerprint that identifies this layout in file headers.
    """

    columns: Tuple[str, ...]
    descending: Tuple[bool, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        cols = tuple(self.columns)
        if not cols:
            raise ValueError("SortKey requires at least one column")
        desc = tuple(self.descending) if self.descending else (False,) * len(cols)
        if len(desc) != len(cols):
            raise ValueError("descending must have the same length as columns")
        object.__setattr__(self, "columns", cols)
        object.__setattr__(self, "descending", desc)

    def extract(self, record: Record) -> Tuple[Any, ...]:
        """Pull the key columns out of ``record.payload`` in declared order."""
        return tuple(record.payload.get(col) for col in self.columns)

    def sort_tuple(self, record: Record) -> Tuple[Any, ...]:
        """A tuple suitable as ``key=`` to :func:`sorted`.

        Descending columns are wrapped in a reversing helper so that a
        single ascending sort over the tuple yields the desired order.
        """
        out = []
        for col, desc in zip(self.columns, self.descending):
            wrapped = _OrderableValue(record.payload.get(col))
            out.append(_Reversed(wrapped) if desc else wrapped)
        return tuple(out)

    def compare(self, a: Record, b: Record) -> int:
        """Total-order comparator: -1 / 0 / +1."""
        ka, kb = self.sort_tuple(a), self.sort_tuple(b)
        if ka < kb:
            return -1
        if ka > kb:
            return 1
        return 0

    def fingerprint(self) -> bytes:
        """16-byte digest identifying this key layout.

        Embedded in file headers so a reader can detect when a file was
        written under a different sort spec.
        """
        h = hashlib.blake2b(digest_size=FINGERPRINT_BYTES)
        for col, desc in zip(self.columns, self.descending):
            h.update(col.encode("utf-8"))
            h.update(b"\x01" if desc else b"\x00")
            h.update(b"\x00")  # column separator
        return h.digest()


@total_ordering
class _Reversed:
    """Inverts ordering of the wrapped key (for descending columns)."""

    __slots__ = ("_inner",)

    def __init__(self, inner: _OrderableValue) -> None:
        self._inner = inner

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _Reversed):
            return NotImplemented
        return self._inner == other._inner

    def __lt__(self, other: "_Reversed") -> bool:
        if not isinstance(other, _Reversed):
            return NotImplemented
        return other._inner < self._inner
