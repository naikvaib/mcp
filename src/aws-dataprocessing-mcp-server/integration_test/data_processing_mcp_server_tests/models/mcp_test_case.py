# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from .validators import Validator
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List


@dataclass
class MCPTestCase:
    """Test case definition for MCP server tests."""

    test_name: str
    tool_name: str
    input_params: Dict[str, Any]
    dependencies: List[str] = field(default_factory=list)
    validators: List[Validator] = field(default_factory=list)
    clean_ups: List[Callable] = field(default_factory=list)
    aws_resources: List[Callable[[], None]] = field(default_factory=list)
