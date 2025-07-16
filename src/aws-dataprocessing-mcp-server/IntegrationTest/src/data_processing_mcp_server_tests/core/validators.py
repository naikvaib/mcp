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

"""Simple validation system for test framework."""

import json
import os
from ..core.aws_setup import AWSSetup
from botocore.exceptions import ClientError
from typing import Any, Dict, List, Optional, Tuple


class ValidationResult:
    """Result of a validation operation with success status, message, and details.

    Used to track validation results throughout the test framework.
    """

    def __init__(self, success: bool, message: str = '', details: Dict[str, Any] = None):
        """Initialize a validation result.

        Args:
            success: Whether the validation succeeded
            message: A human-readable description of the result
            details: Additional structured information about the result
        """
        self.success = success
        self.message = message
        self.details = details or {}


def get_aws_setup() -> AWSSetup:
    """Get a configured AWSSetup instance using GitHub Actions environment credentials."""
    aws_region = os.environ.get('AWS_REGION', 'us-west-1')
    return AWSSetup(profile_name=None, region=aws_region)


def extract_nested_error_message(
    test_case, expected_msg: Optional[str] = None
) -> Tuple[bool, str]:
    """Extract error message from nested response content.

    Returns:
        Tuple of (found_error, message)
    """
    if 'result' in test_case.response and 'content' in test_case.response['result']:
        for item in test_case.response['result']['content']:
            if item.get('type') == 'text' and 'text' in item:
                try:
                    inner_json = json.loads(item['text'])
                    if inner_json.get('isError'):
                        # Extract error message from inner content
                        error_msg = ''
                        if 'content' in inner_json:
                            for content_item in inner_json['content']:
                                if content_item.get('type') == 'text':
                                    error_msg = content_item.get('text', '')

                        # Check if we found the expected message
                        if (
                            expected_msg is None
                            or expected_msg in error_msg
                            or expected_msg in item['text']
                        ):
                            return True, error_msg or item['text']
                except Exception:
                    # Try direct text search if JSON parsing fails
                    if expected_msg is not None and expected_msg in item['text']:
                        return True, item['text']

    return False, ''


# Example built-in validator functions
def validate_response_iserror(test_case) -> ValidationResult:
    """Validate all isError fields are False in nested response.

    For negative test cases, we expect isError in the inner result, so this validation
    is modified to check for that specific pattern.
    """
    # Special handling for negative test cases
    if test_case.test_name.lower().startswith('negative'):
        found_error, _ = extract_nested_error_message(test_case)
        if found_error:
            return ValidationResult(True, 'Found expected nested error for negative test case')
        return ValidationResult(
            False, 'Expected nested error not found in response for negative test case'
        )

    # For normal test cases, check that no isError fields are true
    def check_iserror(obj, path=''):
        if isinstance(obj, dict):
            if 'isError' in obj and obj['isError']:
                return f'isError=True at {path}'
            for key, value in obj.items():
                result = check_iserror(value, f'{path}.{key}' if path else key)
                if result:
                    return result
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                result = check_iserror(item, f'{path}[{i}]')
                if result:
                    return result
        elif isinstance(obj, str):
            try:
                parsed = json.loads(obj)
                return check_iserror(parsed, f'{path}(parsed)')
            except (ValueError, TypeError):
                pass
        return None

    error_path = check_iserror(test_case.response)
    if error_path:
        return ValidationResult(False, f'Found error: {error_path}')
    return ValidationResult(True, 'All isError fields are False')


# Example validator functions - users write their own
def validate_glue_job_exists(test_case) -> ValidationResult:
    """Validate Glue job exists in AWS."""
    job_name = test_case.input_params.get('job_name')
    if not job_name:
        return ValidationResult(False, 'job_name not found in test case')

    try:
        aws_setup = get_aws_setup()
        exists, job_details = aws_setup.check_job_exists(job_name)
        if exists:
            return ValidationResult(True, f'Glue job {job_name} exists')
        return ValidationResult(False, f'Glue job {job_name} does not exist')
    except ClientError as e:
        if 'ExpiredToken' in str(e):
            return ValidationResult(
                False, 'AWS credentials expired. Run: python refresh_credentials.py kathryncoding'
            )
        return ValidationResult(False, f'Error checking job: {str(e)}')
    except Exception as e:
        return ValidationResult(False, f'Error checking job: {str(e)}')


def get_operation_validators(operation: str):
    """Get appropriate validators for each operation."""
    validators = {
        'create-job': [
            validate_response_iserror,
            validate_glue_job_exists,
            validate_mcp_managed_tags,
            validate_job_name_matches,
            validate_job_params_match,
        ],
        'update-job': [validate_response_iserror, validate_update_job_operation],
        'delete-job': [validate_response_iserror, validate_glue_job_deleted],
    }
    return validators.get(operation, [validate_response_iserror])


def validate_glue_job_version_4(test_case) -> ValidationResult:
    """Validate Glue job is version 4.0."""
    job_name = test_case.input_params.get('job_name')
    if not job_name:
        return ValidationResult(False, 'job_name not found in test case')

    try:
        aws_setup = get_aws_setup()
        exists, job_details = aws_setup.check_job_exists(job_name)

        if not exists:
            return ValidationResult(False, f'Job {job_name} does not exist')

        actual_version = job_details.get('GlueVersion')
        if actual_version == '4.0':
            return ValidationResult(True, f'Job {job_name} is version 4.0')
        return ValidationResult(False, f'Job {job_name} version is {actual_version}, expected 4.0')
    except Exception as e:
        return ValidationResult(False, f'Error checking job version: {str(e)}')


def check_mcp_tags(job_name: str) -> Tuple[bool, str, Dict[str, str]]:
    """Check if job has proper MCP managed tags.

    Returns:
        Tuple of (has_valid_tags, error_message, tags)
    """
    try:
        aws_setup = get_aws_setup()
        tags = aws_setup.get_glue_job_tags(job_name)

        # Check for required MCP managed tag keys
        required_tags = ['CreatedAt', 'ManagedBy', 'ResourceType']
        missing_tags = [tag for tag in required_tags if tag not in tags]

        if missing_tags:
            return (
                False,
                f'Missing MCP tags: {missing_tags}. Found tags: {list(tags.keys())}',
                tags,
            )

        # Validate ManagedBy value
        if tags.get('ManagedBy') != 'DataprocessingMcpServer':
            return (
                False,
                f"ManagedBy should be 'DataprocessingMcpServer', got '{tags.get('ManagedBy')}'",
                tags,
            )

        return True, '', tags
    except Exception as e:
        return False, str(e), {}


def validate_mcp_managed_tags(test_case) -> ValidationResult:
    """Validate Glue job has MCP server managed tags."""
    job_name = test_case.input_params.get('job_name')
    if not job_name:
        return ValidationResult(False, 'job_name not found in test case')

    has_valid_tags, error_msg, tags = check_mcp_tags(job_name)

    if has_valid_tags:
        return ValidationResult(True, f'Job {job_name} has valid MCP managed tags')
    else:
        return ValidationResult(False, f'Error checking MCP tags: {error_msg}')


def validate_job_name_matches(test_case) -> ValidationResult:
    """Validate the created job has the same name as specified in the input params."""
    job_name = test_case.input_params.get('job_name')
    if not job_name:
        return ValidationResult(False, 'job_name not found in test case')

    try:
        aws_setup = get_aws_setup()
        exists, job_details = aws_setup.check_job_exists(job_name)

        if not exists:
            return ValidationResult(False, f'Job {job_name} does not exist')

        if job_details.get('Name') != job_name:
            return ValidationResult(
                False, f'Job name mismatch: expected {job_name}, got {job_details.get("Name")}'
            )

        return ValidationResult(True, f'Job name matches: {job_name}')
    except Exception as e:
        return ValidationResult(False, f'Error validating job name: {str(e)}')


def check_job_parameters(job_details: Dict, job_definition: Dict) -> List[str]:
    """Compare job details with job definition and return errors.

    Returns:
        List of validation error messages
    """
    validation_errors = []

    # Check basic fields
    for field in ['GlueVersion', 'Role', 'Description', 'MaxRetries']:
        if field in job_definition and job_definition.get(field) != job_details.get(field):
            validation_errors.append(
                f'{field} mismatch: expected {job_definition.get(field)}, got {job_details.get(field)}'
            )

    # Check Command details
    if 'Command' in job_definition:
        expected_cmd = job_definition.get('Command', {})
        actual_cmd = job_details.get('Command', {})

        for cmd_field in ['Name', 'ScriptLocation']:
            if cmd_field in expected_cmd and expected_cmd.get(cmd_field) != actual_cmd.get(
                cmd_field
            ):
                validation_errors.append(
                    f'Command.{cmd_field} mismatch: expected {expected_cmd.get(cmd_field)}, got {actual_cmd.get(cmd_field)}'
                )

    return validation_errors


def validate_job_params_match(test_case) -> ValidationResult:
    """Validate the created job has the same parameters as specified in the input params."""
    job_name = test_case.input_params.get('job_name')
    job_definition = test_case.input_params.get('job_definition', {})

    if not job_name or not job_definition:
        return ValidationResult(False, 'job_name or job_definition not found in test case')

    try:
        aws_setup = get_aws_setup()
        exists, job_details = aws_setup.check_job_exists(job_name)

        if not exists:
            return ValidationResult(False, f'Job {job_name} does not exist')

        validation_errors = check_job_parameters(job_details, job_definition)

        if validation_errors:
            return ValidationResult(
                False, f'Job parameter validation failed: {"; ".join(validation_errors)}'
            )

        return ValidationResult(True, f'Job parameters match for job {job_name}')
    except Exception as e:
        return ValidationResult(False, f'Error validating job parameters: {str(e)}')


def validate_update_job_operation(test_case) -> ValidationResult:
    """Validate update job operation based on whether it's a normal or negative test."""
    job_name = test_case.input_params.get('job_name')
    if not job_name:
        return ValidationResult(False, 'job_name not found in test case')

    # For negative test cases, we only need to check if the server correctly denied the update
    if test_case.test_name.lower().startswith('negative'):
        expected_msg = f'Cannot update job {job_name} - it is not managed by the MCP server (missing required tags)'
        found_error, error_msg = extract_nested_error_message(test_case, expected_msg)

        if found_error:
            return ValidationResult(
                True, 'MCP server correctly rejected update with expected error message'
            )
        else:
            return ValidationResult(
                False, f"Expected error message '{expected_msg}' not found in response"
            )

    # For normal test cases, validate that the job was updated correctly
    job_definition = test_case.input_params.get('job_definition', {})

    if not job_definition:
        return ValidationResult(False, 'job_definition not found in test case')

    try:
        aws_setup = get_aws_setup()
        exists, job_details = aws_setup.check_job_exists(job_name)

        if not exists:
            return ValidationResult(False, f'Job {job_name} does not exist')

        # Use the shared parameter checking logic
        validation_errors = check_job_parameters(job_details, job_definition)

        if validation_errors:
            return ValidationResult(
                False, f'Job update validation failed: {"; ".join(validation_errors)}'
            )

        return ValidationResult(
            True, f'Job {job_name} was updated successfully with the specified parameters'
        )
    except Exception as e:
        return ValidationResult(False, f'Error validating job update: {str(e)}')


def validate_job_params_updated(test_case) -> ValidationResult:
    """Legacy validator - kept for backward compatibility but no longer used automatically.

    Use validate_update_job_operation instead.
    """
    return validate_update_job_operation(test_case)


def validate_glue_job_deleted(test_case) -> ValidationResult:
    """Validate Glue job is deleted from AWS or properly denied deletion for negative test cases."""
    job_name = test_case.input_params.get('job_name')
    if not job_name:
        return ValidationResult(False, 'job_name not found in test case')

    # For negative test cases, we need to check for the specific error message
    if test_case.test_name.lower().startswith('negative'):
        expected_msg = f'Cannot delete job {job_name} - it is not managed by the MCP server (missing required tags)'
        found_error, _ = extract_nested_error_message(test_case, expected_msg)

        if found_error:
            return ValidationResult(
                True, 'MCP server correctly rejected deletion with expected error message'
            )
        else:
            return ValidationResult(
                False, f"Expected error message '{expected_msg}' not found in response"
            )

    try:
        aws_setup = get_aws_setup()
        exists, _ = aws_setup.check_job_exists(job_name)
        if exists:
            return ValidationResult(False, f'Glue job {job_name} still exists after deletion')
        return ValidationResult(True, f'Glue job {job_name} successfully deleted')
    except Exception as e:
        return ValidationResult(False, f'Error checking job deletion: {str(e)}')


def always_fail_validator(test_case) -> ValidationResult:
    """Always return a failing validation result.

    This validator is designed for testing negative scenarios and error handling.
    It always returns False regardless of the test case input.

    Args:
        test_case: The test case to validate

    Returns:
        A ValidationResult with success=False
    """
    return ValidationResult(False, 'This test is designed to fail.')
