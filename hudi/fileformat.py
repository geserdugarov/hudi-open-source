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
"""On-disk file kinds and the fixed header that precedes every Hudi file."""

import struct
from dataclasses import dataclass
from enum import IntEnum

from hudi.errors import CorruptFileError
from hudi.sortkey import FINGERPRINT_BYTES

MAGIC = b"HUDI"
FORMAT_VERSION = 1

# Big-endian: 4-byte magic, uint16 version, uint8 kind, uint8 reserved,
# uint64 record_count, 16-byte sort-key fingerprint. Total: 32 bytes.
_HEADER_STRUCT = struct.Struct(">4sHBBQ16s")
HEADER_SIZE = _HEADER_STRUCT.size


class FileKind(IntEnum):
    """Discriminator for the two on-disk file kinds in MoR layout."""

    BASE_FILE = 1
    LOG_BLOCK = 2


@dataclass(frozen=True)
class FileHeader:
    """Fixed-size preamble at offset 0 of every base file and log block."""

    kind: FileKind
    sort_key_fingerprint: bytes
    record_count: int
    version: int = FORMAT_VERSION

    def __post_init__(self) -> None:
        if len(self.sort_key_fingerprint) != FINGERPRINT_BYTES:
            raise ValueError(
                f"sort_key_fingerprint must be {FINGERPRINT_BYTES} bytes, "
                f"got {len(self.sort_key_fingerprint)}"
            )
        if self.record_count < 0:
            raise ValueError("record_count must be non-negative")

    def to_bytes(self) -> bytes:
        return _HEADER_STRUCT.pack(
            MAGIC,
            self.version,
            int(self.kind),
            0,  # reserved
            self.record_count,
            self.sort_key_fingerprint,
        )

    @classmethod
    def from_bytes(cls, raw: bytes) -> "FileHeader":
        if len(raw) < HEADER_SIZE:
            raise CorruptFileError(
                f"header truncated: got {len(raw)} bytes, need {HEADER_SIZE}"
            )
        magic, version, kind, _reserved, count, fp = _HEADER_STRUCT.unpack(
            raw[:HEADER_SIZE]
        )
        if magic != MAGIC:
            raise CorruptFileError(f"bad magic: {magic!r}")
        if version != FORMAT_VERSION:
            raise CorruptFileError(
                f"unsupported file format version {version} (this build supports "
                f"{FORMAT_VERSION})"
            )
        try:
            kind_enum = FileKind(kind)
        except ValueError as e:
            raise CorruptFileError(f"unknown file kind {kind}") from e
        return cls(
            kind=kind_enum,
            sort_key_fingerprint=fp,
            record_count=count,
            version=version,
        )
