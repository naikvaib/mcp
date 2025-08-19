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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class ValidationResult:
    """Result of a validation operation."""

    success: bool
    error_message: Optional[str] = None


class Validator(ABC):
    """Base abstract validator class."""

    def __init__(self):
        """Initialize Validator."""
        self.validator_type = 'base'


class TextValidator(Validator):
    """Validator for checking text in MCP tool responses."""

    def __init__(self):
        """Initialize TextValidator.

        Args:
            expected_string: String to look for in the response
            field_path: Optional dot-notation path to check (e.g., 'job.name')
        """
        self.validator_type = 'text'

    @abstractmethod
    def validate(self, actual_response: Dict[str, Any]) -> ValidationResult:
        """Validate the response contains expected text.

        Args:
            actual_response: The response from the MCP tool call

        Returns:
            ValidationResult: Result of the validation
        """
        pass


class BotoValidator(Validator):
    """Validator for checking AWS resources using boto3."""

    def __init__(self, boto_client: Any):
        """Initialize BotoValidator.

        Args:
            boto_client: Boto3 client for a specific AWS service (e.g., Glue, EMR)
            operation: Boto3 operation name (e.g., 'get_job', 'get_tags')
            operation_input_params: Parameters to call the operation
            expected_keys: Optional list of keys to check in the response
            validate_absence: If True, checks that the keys are not present in the response
        """
        super().__init__()
        self.validator_type = 'boto'

        if not boto_client:
            raise ValueError('boto_client must be provided')
        self.boto_client = boto_client

    @abstractmethod
    def validate(self, tool_params: Dict[str, Any]) -> ValidationResult:
        """Validate AWS resources using boto3.

        Args:
            tool_params: Parameters for the boto3 call

        Returns:
            ValidationResult: Result of the validation
        """
        pass
