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
"""Exception hierarchy for the Hudi Python prototype."""


class HudiError(Exception):
    """Root of all Hudi-prototype errors. Catch this to net every failure."""


class CorruptFileError(HudiError):
    """Raised when an on-disk file has a bad header, truncated body, or
    fails an integrity check (magic mismatch, version skew, fingerprint
    mismatch, short read)."""


class SortOrderViolation(HudiError):
    """Raised when records are observed out of the declared SortKey order
    (e.g. while writing a base file or validating a log block)."""
