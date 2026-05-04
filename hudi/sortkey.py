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
import math
from dataclasses import dataclass, field
from functools import total_ordering
from typing import Any, Tuple

from hudi.errors import SortOrderViolation
from hudi.record import Record

FINGERPRINT_BYTES = 16

# Types that either raise on ``<`` (complex, dict) or only implement a
# partial order (set, frozenset use subset semantics, so {1} and {2} are
# mutually-incomparable). Any of these as a top-level key value would
# break the total-order contract, so we reject them up front.
_UNORDERABLE_TYPES: Tuple[type, ...] = (complex, dict, set, frozenset)


@total_ordering
class _OrderableValue:
    """Wraps a column value so the result of comparisons is a true total order.

    Each value is normalised to a ``(bucket, type_name, nan_tag, payload)``
    4-tuple. Comparing two such tuples with Python's native ``<`` then yields
    a true total order because:

    1. ``None`` always sorts first (bucket 0).
    2. Within bucket 1 we group by the value's type qualname so heterogeneous
       columns (e.g. mixing ``int`` and ``str``) get a deterministic order
       instead of raising ``TypeError``.
    3. Within a single type bucket we additionally tag NaN floats so they
       sort after every real float and compare equal to each other —
       Python's native ``float('nan') < x`` and ``x < float('nan')`` are
       both False, which would otherwise let ``compare(a, b)`` and
       ``compare(b, a)`` both return +1.
    4. ``list`` / ``tuple`` payloads are recursively normalised so a nested
       NaN or heterogeneous element can't smuggle a partial order back in
       at the inner-comparison step.
    5. Other values whose native ``<`` is not a true total order
       (``complex``, ``dict``, ``set``, ``frozenset``) are rejected with
       :class:`SortOrderViolation`, even when they appear nested inside a
       list or tuple.
    6. Values whose type has no working ``<`` at all (``object()``,
       ``range()``, user classes without ``__lt__``) are likewise rejected
       — otherwise the raw ``TypeError`` would only surface mid-sort.
    """

    __slots__ = ("_key_tuple",)

    def __init__(self, value: Any) -> None:
        self._key_tuple: Tuple[Any, ...] = self._normalize(value)

    @classmethod
    def _normalize(cls, value: Any) -> Tuple[Any, ...]:
        if isinstance(value, _UNORDERABLE_TYPES):
            raise SortOrderViolation(
                f"value of type {type(value).__qualname__!r} cannot be used as "
                "a sort-key column: it has no usable total order"
            )
        if value is None:
            return (0, "", 0, ())
        if isinstance(value, float) and math.isnan(value):
            # All NaNs compare equal to each other and sort after real floats.
            return (1, "float", 1, 0.0)
        if isinstance(value, (list, tuple)):
            # Recurse so heterogeneous / NaN inner elements can't break the
            # total order via Python's native element-wise tuple compare.
            normalized = tuple(cls._normalize(v) for v in value)
            return (1, type(value).__qualname__, 0, normalized)
        # Probe ``<`` against itself: raw ``object()``, ``range()``, or any
        # user class without ``__lt__`` would otherwise pass through as an
        # opaque payload and raise ``TypeError`` only when the sort happens
        # to compare two such values within the same type bucket.
        try:
            value < value  # noqa: B015 - probing __lt__ for total-order support
        except TypeError:
            raise SortOrderViolation(
                f"value of type {type(value).__qualname__!r} cannot be used as "
                "a sort-key column: it has no usable total order"
            ) from None
        return (1, type(value).__qualname__, 0, value)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _OrderableValue):
            return NotImplemented
        return self._key_tuple == other._key_tuple

    def __lt__(self, other: "_OrderableValue") -> bool:
        if not isinstance(other, _OrderableValue):
            return NotImplemented
        return self._key_tuple < other._key_tuple


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

        Raises :class:`SortOrderViolation` if any key column holds a value
        whose type has no total order (``complex``, ``dict``, ``set``,
        ``frozenset``).
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

        Each column is length-prefixed and the total column count is hashed
        first, so distinct ``(columns, descending)`` pairs always produce
        distinct byte streams. A naive ``col || sep`` encoding would let a
        column whose name contains the separator byte collide with a
        multi-column spec (e.g. ``("a", "b")`` vs ``("a\\x00\\x00b",)``).
        """
        h = hashlib.blake2b(digest_size=FINGERPRINT_BYTES)
        h.update(len(self.columns).to_bytes(4, "big"))
        for col, desc in zip(self.columns, self.descending):
            col_bytes = col.encode("utf-8")
            h.update(len(col_bytes).to_bytes(4, "big"))
            h.update(col_bytes)
            h.update(b"\x01" if desc else b"\x00")
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
