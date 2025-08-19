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

import json
import re
from typing import Any


def extract_path(response: dict, path: str) -> Any:
    """Extract value from nested dict using path notation."""
    tokens = re.findall(r'\w+|\[\d+\]', path)
    value = response
    for token in tokens:
        if re.fullmatch(r'\[\d+\]', token):
            index = int(token[1:-1])
            if not isinstance(value, list):
                raise ValueError(f'Expected list at {token}, got {type(value)}')
            value = value[index]
        else:
            if isinstance(value, str):
                try:
                    value = json.loads(value)
                except Exception:
                    raise ValueError(f"Expected JSON string at '{token}' but failed to parse.")
            if not isinstance(value, dict) or token not in value:
                raise ValueError(f"Cannot find key '{token}' in {value}")
            value = value[token]
    return value
