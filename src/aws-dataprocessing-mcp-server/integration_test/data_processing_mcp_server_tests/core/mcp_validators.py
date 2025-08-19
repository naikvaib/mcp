import botocore.exceptions
from typing import Dict, List, Any, Optional
import boto3
import json
import re
from data_processing_mcp_server_tests.models.validators import TextValidator, BotoValidator, ValidationResult
from data_processing_mcp_server_tests.utils.utils import PARAMETER_MAPPING, OPERATION_ARN_MAP, OPERATION_PREFIX_MAP, SKIP_TAG_CHECK_OPERATIONS  
from data_processing_mcp_server_tests.utils.injection import extract_path
from data_processing_mcp_server_tests.utils.logger import get_logger

logger = get_logger(__name__)

class ContainsTextValidator(TextValidator):
    def __init__(self, expected_string: str, expected_count: Optional[int] = None, bucket_count: Optional[int] = None):
        super().__init__()
        self.expected_string = expected_string
        self.expected_count = expected_count
        self.bucket_count = bucket_count

    def validate(self, actual_response: Dict[str, any]) -> ValidationResult:
        """ Validate the response from the tool execution.
        Args:
            actual_response: The response dictionary from the tool execution.
        Returns:
            ValidationResult: Result of the validation.
        """
        # 1. Validate the text content
        logger.info(f"[ContainsTextValidator] Validating response: {actual_response}")
        content_items = actual_response.get("result", {}).get("content", [])
        if not content_items:
            return ValidationResult(False, "No content in response")

        first_text = content_items[0].get("text", "")

        try:
            parsed_json = json.loads(first_text)
        except json.JSONDecodeError:
            if self.expected_string in first_text:
                return ValidationResult(True, "Error message contains expected string")
            return ValidationResult(False, f"Expected string '{self.expected_string}' not found, and not valid JSON")

        embedded_text = parsed_json.get("content", [{}])[0].get("text", "")
        if self.expected_string not in embedded_text:
            return ValidationResult(False, f"Expected string '{self.expected_string}' not found in response")

        # 2. Validate the count field if expected_count or bucket_count is provided
        if self.expected_count is not None:
            actual_count = parsed_json.get("count", None)
            if actual_count != self.expected_count:
                return ValidationResult(False, f"Count mismatch: expected {self.expected_count}, got {actual_count}")
        if self.bucket_count is not None:
            actual_bucket_count = parsed_json.get("bucket_count", None)
            if actual_bucket_count != self.bucket_count:
                return ValidationResult(False, f"Bucket count mismatch: expected {self.bucket_count}, got {actual_bucket_count}")
        return ValidationResult(True, "Text and count match")

class AWSBotoValidator(BotoValidator):
    """Validator that checks AWS resources using boto3.
    Args:
        boto_client: Boto3 client for the specific AWS service (e.g., Glue, S3)
        operation: The boto3 operation to call (e.g., "get_job", "get_database")
        operation_input_params: Parameters to call the boto3 operation (can use snake_case like 'job_name')
        expected_keys: List of fields to validate (e.g., 'job_definition' or nested keys)
        validate_absence: If True, expect the resource to NOT exist
    Returns:
        ValidationResult: Result of the validation.
    """

    def __init__(self, boto_client=None, 
                 operation: str = "get_job",
                 operation_input_params: Optional[Dict[str, Any]] = None,
                 expected_keys: Optional[List[str]] = None,
                 validate_absence: bool = False,
                 injectable_params: Optional[Dict[str, str]] = None):
        """
        Args:
            boto_client: Boto3 AWS client
            operation: The boto3 operation to call (e.g., "get_job")
            operation_input_params: Parameters to call the boto3 operation (can use snake_case like 'job_name')
            expected_keys: List of fields to validate (e.g., 'job_definition' or nested keys)
            validate_absence: If True, expect the resource to NOT exist
            injectable_params: Parameters that can be injected from other responses, using {{dependency_name.key}} syntax
        """

        self.boto_client = boto_client
        super().__init__(boto_client=boto_client)
        self.operation = operation
        self.operation_input_params = self._normalize_params(operation_input_params or {})
        self.expected_keys = expected_keys or []
        self.validate_absence = validate_absence
        self.injectable_params = injectable_params or {}

    def _normalize_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Convert snake_case params like job_name to boto3 expected camelCase like JobName"""

        normalized = {}
        for k, v in params.items():
            normalized[PARAMETER_MAPPING.get(k, k)] = v  # Use the imported mapping
        return normalized

    def _flatten_dict(self, d: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
        """Flatten nested dictionaries into dot-separated keys."""
        items = {}
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(self._flatten_dict(v, new_key, sep=sep))
            else:
                items[new_key] = v
        return items

    def _get_nested_value(self, data: Dict[str, Any], key_path: str):
        """Get nested value from a dictionary using dot-separated key path."""
        keys = key_path.split('.')
        for key in keys:
            if not isinstance(data, dict):
                return None
            # Try exact match first
            if key in data:
                data = data[key]
            else:
                # Try case-insensitive match
                matched_key = next((k for k in data.keys() if k.lower() == key.lower()), None)
                if matched_key:
                    data = data[matched_key]
                else:
                    return None
        return data


    def _construct_arn(self, tool_params: Dict[str, Any]) -> Optional[str]:
        """Construct the ARN for the AWS resource based on the service and parameters."""
        region = self.boto_client.meta.region_name
        account_id = boto3.client('sts').get_caller_identity().get('Account')
        service_name = self.boto_client.meta.service_model.service_name

        # Special handling for S3 (ARN not required)
        if service_name == "s3":
            return None  # Not used

        operation_arn = OPERATION_ARN_MAP.get(self.operation)
        if not operation_arn:
            raise ValueError(f"Unsupported operation '{self.operation}' for ARN construction")

        resource_type, param_key = operation_arn

        values = []
        for key in param_key.split("/"):
            val = tool_params.get(key)
            if val is None:
                raise ValueError(f"Missing resource identifier '{key}' for operation '{self.operation}'")
            if isinstance(val, list):
                val = "/".join(val)
            values.append(val)
        resource_id = "/".join(values)
        if not resource_id:
            raise ValueError(f"Missing resource identifier '{param_key}' for operation '{self.operation}'")

        return f"arn:aws:{service_name}:{region}:{account_id}:{resource_type}/{resource_id}"

    def _validate_expected_keys_by_operation(self, operation: str, tool_params: Dict[str, Any], response: Dict[str, Any]) -> List[str]:
        mismatches = []

        for key_path in self.expected_keys:
            input_key_prefix, response_key_prefix = OPERATION_PREFIX_MAP.get(operation, (None, None))

            if input_key_prefix is not None:
                sub_obj = tool_params if input_key_prefix == "" else tool_params.get(input_key_prefix, {})
                expected_key = PARAMETER_MAPPING.get(key_path, key_path)  # apply mapping
                expected_value = self._get_nested_value(sub_obj, expected_key)
            else:
                expected_value = self._get_nested_value(tool_params, key_path)

            if response_key_prefix is not None:
                actual_value = self._get_nested_value(response.get(response_key_prefix, {}), key_path)
            else:
                actual_value = self._get_nested_value(response, key_path)

            if expected_value != actual_value:
                if isinstance(actual_value, list):
                    if expected_value not in actual_value:
                        mismatches.append(f"Expected '{expected_value}' in list at '{key_path}', but got {actual_value}")
                else:
                    mismatches.append(f"Mismatch for '{key_path}': expected '{expected_value}', got '{actual_value}'")

        return mismatches

    def validate(self, tool_params: Dict[str, Any], response_map: Optional[Dict[str, Any]] = None) -> ValidationResult:
        """ Validate the actual AWS resource against the expected keys.
        Args:
            tool_params: Parameters passed to the tool execution, including operation and input params.
        Returns:
            ValidationResult: Result of the validation.
        """
        
        try:
            # First resolve injectable parameters if any
            if self.injectable_params:
                if response_map is None:
                    return ValidationResult(False, "response_map not provided")
                for k, v in self.injectable_params.items():
                    match = re.match(r"\{\{(.+?)\}\}", v)
                    if match:
                        path = match.group(1)
                        parts = path.split(".", 1)
                        dep_name = parts[0]
                        sub_path = parts[1] if len(parts) > 1 else ""
                        dep_response = response_map.get(dep_name)
                        if not dep_response:
                            return ValidationResult(False, f"Missing response for dependency: {dep_name}")
                        try:
                            resolved_value = extract_path(dep_response, sub_path)
                            normalized = self._normalize_params({k: resolved_value})
                            norm_key = list(normalized.keys())[0]

                            if k in self.operation_input_params:
                                logger.info(f"[Validator] Removing unnormalized key: {k}")
                                del self.operation_input_params[k]

                            self.operation_input_params[norm_key] = resolved_value

                        except Exception as e:
                            return ValidationResult(False, f"Failed to inject param '{k}': {e}")

            method = getattr(self.boto_client, self.operation, None)
            if not method:
                return ValidationResult(False, f"Operation '{self.operation}' not found in AWS client")
            
            response = method(**self.operation_input_params)
            if self.validate_absence:
                # For triggers, check if it's in DELETING state
                if self.operation == "get_trigger" and response.get("Trigger", {}).get("State") == "DELETING":
                    return ValidationResult(True, "Trigger is in DELETING state, considered as deleted")
                return ValidationResult(False, f"Expected resource to NOT exist, but it does: {response}")
            
            # Validate expected keys
            mismatches = self._validate_expected_keys_by_operation(self.operation, tool_params, response)

            #Check MCP tags
            if self.operation in SKIP_TAG_CHECK_OPERATIONS:
                logger.info(f"[Validator Info] Skipping tag validation for operation: {self.operation}")
            else:    
                tags_dict = {}
                if self.operation == "describe_cluster":
                    tag_list = response.get("Cluster", {}).get("Tags", [])
                    tags_dict = {tag["Key"]: tag["Value"] for tag in tag_list}
                else:
                    try:
                        arn = self._construct_arn(tool_params)
                        # Try both list_tags_for_resource and get_tags depending on what's available
                        if hasattr(self.boto_client, "list_tags_for_resource"):
                            tag_response = self.boto_client.list_tags_for_resource(ResourceARN=arn)
                            tag_list = tag_response.get("Tags", [])
                            # In Athena, tags are returned as a list of dicts
                            if isinstance(tag_list, list):
                                tags_dict = {tag["Key"]: tag["Value"] for tag in tag_list}
                            else:
                                tags_dict = tag_list  # fallback
                        elif hasattr(self.boto_client, "get_tags"):
                            tag_response = self.boto_client.get_tags(ResourceArn=arn)
                            tag_list = tag_response.get("Tags", [])
                            tags_dict = tag_list if isinstance(tag_list, dict) else {tag["Key"]: tag["Value"] for tag in tag_list}
                        else:
                            logger.warning(f"[Validator Warning] No tag API available for {self.operation}")

                    except Exception as e:
                        logger.warning(f"[Validator Warning] Failed to get tags for {self.operation}: {e}")

                for tag_key in ['CreatedAt', 'ManagedBy', 'ResourceType']:
                        if tags_dict.get(tag_key) is None:
                            mismatches.append(f"Missing tag: {tag_key}")

                # Validate ManagedBy value
                if tags_dict.get('ManagedBy') != 'DataprocessingMcpServer':
                    mismatches.append(f"ManagedBy should be 'DataprocessingMcpServer', got '{tags_dict.get('ManagedBy')}'")

            if mismatches:
                return ValidationResult(False, f"Validation failed: {'; '.join(mismatches)}")
            
            return ValidationResult(True, f"Validation successful for operation '{self.operation}'")
            
        except botocore.exceptions.ClientError as e:
            error_code = e.response["Error"].get("Code", "")
            if self.validate_absence and error_code in ["EntityNotFoundException", "InvalidRequestException", "ResourceNotFoundException"]:
                return ValidationResult(True, f"Resource correctly not found: {error_code}")
            return ValidationResult(False, f"ClientError during validation: {error_code} - {e.response['Error'].get('Message', str(e))}")
