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
"""Foundation data model for the Python Hudi prototype.

Exports the core types every higher layer (writers, readers, compaction,
log-merging) builds on: ``Record``, ``SortKey``, the file-format header,
and the exception hierarchy.
"""

from hudi.errors import CorruptFileError, HudiError, SortOrderViolation
from hudi.fileformat import FileHeader, FileKind
from hudi.record import Record
from hudi.sortkey import SortKey

__all__ = [
    "CorruptFileError",
    "FileHeader",
    "FileKind",
    "HudiError",
    "Record",
    "SortKey",
    "SortOrderViolation",
]
