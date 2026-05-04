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

    Each value is normalised to a
    ``(bucket, type_name, nan_tag, type_id, payload)`` 5-tuple. Comparing
    two such tuples with Python's native ``<`` then yields a true total
    order because:

    1. ``None`` always sorts first (bucket 0).
    2. Within bucket 1 we group by the value's type qualname so heterogeneous
       columns (e.g. mixing ``int`` and ``str``) get a deterministic order
       instead of raising ``TypeError``.
    3. Within a single type bucket we additionally tag NaN floats so they
       sort after every real float and compare equal to each other —
       Python's native ``float('nan') < x`` and ``x < float('nan')`` are
       both False, which would otherwise let ``compare(a, b)`` and
       ``compare(b, a)`` both return +1.
    4. The class identity (``id(type(value))``) is included as a further
       tiebreaker so two distinct user classes that happen to share the
       same ``__qualname__`` (e.g. classes built by a factory function,
       both ending up as ``make_class.<locals>.C``) bucket separately
       instead of falling through to a raw cross-class ``<``. Without
       this, two such classes can pass each per-value probe and then
       silently violate trichotomy at compare time, with both
       ``compare(a, b)`` and ``compare(b, a)`` returning ``+1``.
    5. ``list`` / ``tuple`` payloads are recursively normalised so a nested
       NaN or heterogeneous element can't smuggle a partial order back in
       at the inner-comparison step.
    6. Other values whose native ``<`` is not a true total order
       (``complex``, ``dict``, ``set``, ``frozenset``) are rejected with
       :class:`SortOrderViolation`, even when they appear nested inside a
       list or tuple.
    7. Values whose type has no working ``<`` at all (``object()``,
       ``range()``, user classes without ``__lt__``) are likewise rejected
       — otherwise the raw ``TypeError`` would only surface mid-sort.
    8. User classes must override both ``__lt__`` AND ``__eq__``. A class
       that defines only ``__lt__`` (e.g. one that always returns
       ``False``) would otherwise pass the ``<`` probe and then silently
       break trichotomy: with the default identity ``__eq__``, two
       distinct instances satisfy neither ``a < b`` nor ``a == b``, so
       ``functools.total_ordering``'s synthesised ``__gt__`` (defined as
       ``not (a < b) and a != b``) reports both ``a > b`` and ``b > a``
       as true and ``compare`` returns ``+1`` in both directions.
    9. As a final safety net, both the per-value probe and ``__lt__``
       catch *any* exception raised by the underlying ``<`` (not just
       ``TypeError``) and re-raise it as :class:`SortOrderViolation`.
       Some types pass the per-value ``value < value`` probe but raise
       when compared across instances — naive vs. timezone-aware
       ``datetime.datetime`` is the canonical case
       (``TypeError: can't compare offset-naive and offset-aware
       datetimes``). Other types raise non-``TypeError`` exceptions
       even on self-comparison: ``decimal.Decimal('NaN') <
       decimal.Decimal('NaN')`` raises ``decimal.InvalidOperation``,
       which is *not* a ``TypeError``. Catching ``Exception`` keeps the
       failure inside the documented :class:`SortOrderViolation`
       contract instead of letting an arbitrary error escape mid-sort.
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
            return (0, "", 0, 0, ())
        if isinstance(value, float) and math.isnan(value):
            # All NaNs compare equal to each other and sort after real floats.
            return (1, "float", 1, id(float), 0.0)
        if isinstance(value, (list, tuple)):
            # Recurse so heterogeneous / NaN inner elements can't break the
            # total order via Python's native element-wise tuple compare.
            normalized = tuple(cls._normalize(v) for v in value)
            return (1, type(value).__qualname__, 0, id(type(value)), normalized)
        val_cls = type(value)
        # Require the class to explicitly override both ``__lt__`` AND
        # ``__eq__``. A class that defines only ``__lt__`` (returning
        # ``False`` always, say) would otherwise pass the ``<`` probe
        # below and then quietly produce a non-total comparator: with
        # the default identity ``__eq__`` two distinct instances are
        # neither ``<`` nor ``==``, and ``@total_ordering``'s
        # synthesised ``__gt__`` then reports both directions as
        # ``True``, so ``compare`` returns ``+1`` both ways.
        if val_cls.__lt__ is object.__lt__ or val_cls.__eq__ is object.__eq__:
            raise SortOrderViolation(
                f"value of type {val_cls.__qualname__!r} cannot be used as "
                "a sort-key column: its class must explicitly define both "
                "__lt__ and __eq__ to provide a usable total order"
            )
        # Probe ``<`` against itself: a class that defines ``__lt__`` but
        # whose implementation still routes back to ``object`` (returning
        # ``NotImplemented``) would otherwise raise ``TypeError`` only
        # when the sort happens to compare two such values. We catch
        # ``Exception`` rather than just ``TypeError`` because some types
        # signal "no usable order" with a different exception class —
        # e.g. ``Decimal('NaN') < Decimal('NaN')`` raises
        # ``decimal.InvalidOperation``, which is an ``ArithmeticError``,
        # not a ``TypeError``. Anything other than a clean ``True`` /
        # ``False`` here means the value can't sit in a total order.
        try:
            value < value  # noqa: B015 - probing __lt__ for total-order support
        except Exception:
            raise SortOrderViolation(
                f"value of type {val_cls.__qualname__!r} cannot be used as "
                "a sort-key column: it has no usable total order"
            ) from None
        # ``id(val_cls)`` disambiguates distinct classes that happen to
        # share ``__qualname__`` (factory-produced classes, redefined
        # classes, etc.) so their values never reach a cross-class ``<``.
        return (1, val_cls.__qualname__, 0, id(val_cls), value)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _OrderableValue):
            return NotImplemented
        return self._key_tuple == other._key_tuple

    def __lt__(self, other: "_OrderableValue") -> bool:
        if not isinstance(other, _OrderableValue):
            return NotImplemented
        try:
            return self._key_tuple < other._key_tuple
        except Exception as exc:
            # Two values whose types each pass the per-value ``<`` probe
            # can still raise when compared to each other: naive vs.
            # timezone-aware ``datetime.datetime`` is the canonical
            # offender (``TypeError``). Other types signal an
            # un-orderable comparison with a non-``TypeError`` exception
            # — ``decimal.InvalidOperation`` from ``Decimal('NaN')`` is
            # the case that prompted broadening this catch. Surface any
            # such failure as the documented ordering-violation error
            # rather than letting it escape mid-sort.
            raise SortOrderViolation(
                "values cannot be ordered against each other: "
                f"{exc}"
            ) from exc


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
