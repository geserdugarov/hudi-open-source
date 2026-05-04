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

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Mapping, Tuple


@dataclass(frozen=True)
class Record:
    """An immutable row.

    ``primary_key`` is the tuple of values that identifies the row across
    versions. ``payload`` is the column-name -> value map (wrapped in a
    read-only view so the frozen contract is not just nominal).
    ``sequence`` is a monotonically increasing version stamp; on duplicate
    keys, the highest ``sequence`` wins.
    """

    primary_key: Tuple[Any, ...]
    payload: Mapping[str, Any] = field(default_factory=dict)
    sequence: int = 0

    def __post_init__(self) -> None:
        # Normalise the key to a tuple and freeze the payload so callers
        # can't mutate a Record's state after construction.
        object.__setattr__(self, "primary_key", tuple(self.primary_key))
        object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))

    def supersedes(self, other: "Record") -> bool:
        """True if ``self`` is a newer version of the same key than ``other``."""
        return self.primary_key == other.primary_key and self.sequence > other.sequence
