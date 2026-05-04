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
"""The atomic ``Record`` value used everywhere in the prototype."""

from collections.abc import Mapping as _MappingABC
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Mapping, Tuple


def _freeze(value: Any) -> Any:
    """Recursively replace mutable containers with immutable equivalents.

    Without this, a record's ``primary_key`` or ``payload`` could still be
    mutated after construction by reaching through a nested ``list``,
    ``dict``, ``set``, or ``bytearray`` — which would silently change the
    record's equality and sort-key behavior.
    """
    # Any Mapping (plain ``dict``, ``MappingProxyType``, ``UserDict``, ...)
    # is snapshot-copied: a ``MappingProxyType`` is only a read-only *view*
    # of a still-mutable backing dict, so we must not trust it as-is.
    if isinstance(value, _MappingABC):
        return MappingProxyType({k: _freeze(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(v) for v in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze(v) for v in value)
    if isinstance(value, bytearray):
        return bytes(value)
    return value


@dataclass(frozen=True)
class Record:
    """An immutable row.

    ``primary_key`` is the tuple of values that identifies the row across
    versions. ``payload`` is the column-name -> value map (wrapped in a
    read-only view so the frozen contract is not just nominal).
    ``sequence`` is a monotonically increasing version stamp; on duplicate
    keys, the highest ``sequence`` wins.

    Both ``primary_key`` and ``payload`` are *deeply* frozen on
    construction: nested lists become tuples, nested dicts become
    read-only mapping proxies, sets become frozensets, etc. Without
    this, a caller holding a reference to an inner container could
    mutate the record's state after construction and silently change
    its equality / sort-key behavior.
    """

    primary_key: Tuple[Any, ...]
    payload: Mapping[str, Any] = field(default_factory=dict)
    sequence: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "primary_key", tuple(_freeze(v) for v in self.primary_key)
        )
        object.__setattr__(
            self,
            "payload",
            MappingProxyType({k: _freeze(v) for k, v in dict(self.payload).items()}),
        )

    def supersedes(self, other: "Record") -> bool:
        """True if ``self`` is a newer version of the same key than ``other``."""
        return self.primary_key == other.primary_key and self.sequence > other.sequence
