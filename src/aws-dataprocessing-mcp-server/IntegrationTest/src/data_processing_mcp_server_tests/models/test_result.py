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

"""Test result classes used across the testing framework."""

from ..core.validators import ValidationResult
from .test_case import MCPTestCase
from typing import List, Optional


class TestResult:
    """Class representing the result of a test execution."""

    def __init__(
        self,
        test_case: MCPTestCase,
        success: bool,
        validation_results: List[ValidationResult] = None,
        error: Optional[str] = None,
        execution_time: float = 0.0,
    ):
        """Initialize a test result.

        Args:
            test_case: The test case that was executed
            success: Whether the test was successful
            validation_results: List of validation results
            error: Error message (if any)
            execution_time: Time taken to execute the test (in seconds)
        """
        self.test_case = test_case
        self.success = success
        self.validation_results = validation_results or []
        self.error = error
        self.execution_time = execution_time
