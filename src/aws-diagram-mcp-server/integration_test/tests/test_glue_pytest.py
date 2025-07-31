import pytest
import os
import sys
from typing import Dict, List, Any, Optional, Tuple
import boto3
import json
from collections import defaultdict
import botocore.exceptions
from botocore.config import Config
import time
import re

from src.data_processing_mcp_server_tests.core.aws_setup import upload_script, create_non_mcp_job, create_athena_results_bucket
from src.data_processing_mcp_server_tests.core.test_executor import Executor
from src.data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase
from src.data_processing_mcp_server_tests.models.validators import TextValidator, BotoValidator, ValidationResult
from src.data_processing_mcp_server_tests.models.clean_up import CleanUper
from src.data_processing_mcp_server_tests.utils.injection import extract_path
from src.data_processing_mcp_server_tests.core.aws_setup import get_or_create_glue_role

# This is the demo modification



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
        print(f"[ContainsTextValidator] Validating response: {actual_response}")
        # 1. Validate the text content
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

        mapping = {
            "job_name": "JobName",
            "database_name": "DatabaseName",
            "job_definition": "JobDefinition",
            "table_name": "Name",  # For get_table operation
            "cluster_id": "ClusterId",
            "step_id": "StepId",
            "policy_name": "PolicyName",
        }

        normalized = {}
        for k, v in params.items():
            normalized[mapping.get(k, k)] = v
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
            if isinstance(data, dict) and key in data:
                data = data[key]
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
        
        OPERATION_ARN_MAP = {
            "get_job": ("job", "job_name"),
            "get_database": ("database", "database_name"),
            "get_table": ("table", "database_name/table_name"),  # Concatenates database_name and table_name
            "get_connection": ("connection", "connection_name"),
            "get_work_group": ("workgroup", "name"),
            "get_data_catalog": ("datacatalog", "name"),
            "get_partition": ("partition", "database_name/table_name/partition_values"),
            "get_session": ("session", "session_id"),
        }
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
        return f"arn:aws:{service_name}:{region}:{account_id}:{resource_type}/{resource_id}"
    
    def _validate_expected_keys_by_operation(self, operation: str, tool_params: Dict[str, Any], response: Dict[str, Any]) -> List[str]:
        mismatches = []

        for key_path in self.expected_keys:
            if key_path == "job_definition":
                expected_def = tool_params.get("job_definition", {})
                actual_def = response.get("Job", {})
                flat_expected = self._flatten_dict(expected_def)
                flat_actual = self._flatten_dict(actual_def)
                for sub_key, expected_value in flat_expected.items():
                    actual_value = flat_actual.get(sub_key)
                    if expected_value != actual_value:
                        mismatches.append(f"Mismatch for '{sub_key}': expected '{expected_value}', got '{actual_value}'")

            elif operation == "get_database":
                expected_value = self._get_nested_value(tool_params.get("database_definition", {}), key_path)
                actual_value = self._get_nested_value(response.get("Database", {}), key_path)
                if expected_value != actual_value:
                    mismatches.append(f"Mismatch for '{key_path}': expected '{expected_value}', got '{actual_value}'")

            elif operation == "get_table":
                expected_value = self._get_nested_value(tool_params.get("table_input", {}), key_path)
                actual_value = self._get_nested_value(response.get("Table", {}), key_path)
                if expected_value != actual_value:
                    mismatches.append(f"Mismatch for '{key_path}': expected '{expected_value}', got '{actual_value}'")

            elif operation == "list_role_policies":
                expected_value = tool_params.get("policy_name")
                actual_list = response.get("PolicyNames", [])
                if expected_value not in actual_list:
                    mismatches.append(f"Mismatch for 'policy_name': expected '{expected_value}' to be in {actual_list}")

            elif operation == "get_session":
                sub_key = key_path.split(".", 1)[1] if "." in key_path else key_path
                expected_value = self._get_nested_value(tool_params, sub_key)
                actual_value = self._get_nested_value(response, key_path)

            elif operation == "head_object":
                actual_value = self._get_nested_value(response, key_path)
                if actual_value is None:
                    mismatches.append(f"Missing expected key: {key_path}")

            else:
                expected_value = self._get_nested_value(tool_params, key_path)
                actual_value = self._get_nested_value(response, key_path)
                if expected_value != actual_value:
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
                                print(f"[Validator] Removing unnormalized key: {k}")
                                del self.operation_input_params[k]

                            self.operation_input_params[norm_key] = resolved_value

                        except Exception as e:
                            return ValidationResult(False, f"Failed to inject param '{k}': {e}")

            method = getattr(self.boto_client, self.operation, None)
            if not method:
                return ValidationResult(False, f"Operation '{self.operation}' not found in AWS client")
            
            response = method(**self.operation_input_params)
            if self.validate_absence:
                return ValidationResult(False, f"Expected resource to NOT exist, but it does: {response}")
            
            mismatches = self._validate_expected_keys_by_operation(self.operation, tool_params, response)

            #Check MCP tags
            skip_tag_check_operations = {"describe_step", "list_role_policies", "get_role_policy", "get_role", "get_partition", "list_instance_groups", "list_instance_fleets", "head_object"} # Step doesn't have MCP tags
            if self.operation in skip_tag_check_operations:
                print(f"[Validator Info] Skipping tag validation for operation: {self.operation}")
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
                            print(f"[Validator Warning] No tag API available for {self.operation}")

                    except Exception as e:
                        print(f"[Validator Warning] Failed to get tags for {self.operation}: {e}")

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

class DeleteAWSResources(CleanUper):
    """Generic cleaner that deletes any AWS resource based on a given delete API and parameters."""

    def __init__(self, delete_api: str, 
                 delete_params: Dict[str, Any]=None, 
                 boto_client=None,
                 resource_field: Optional[str] = None, target_param_key: Optional[str] = None,
                 param_is_list: bool = False):
        
        self.delete_api = delete_api
        self.delete_params = delete_params or {}
        self.boto_client = boto_client

        self.resource_field = resource_field
        self.target_param_key = target_param_key
        self.param_is_list = param_is_list

    def _convert_to_boto_keys(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Convert snake_case keys to camelCase for boto3 compatibility."""
        def is_snake_case(s: str) -> bool:
            return '_' in s and s.lower() == s

        def camel_case(s):
            parts = s.split('_')
            return ''.join(p.capitalize() for p in parts)

        converted = {}
        for k, v in params.items():
            if is_snake_case(k):
                converted[camel_case(k)] = v
            else:
                converted[k] = v  
        return converted

    def clean_up(self, _: Dict[str, Any], actual_response: Dict[str, Any]):
        if not self.boto_client:
            raise ValueError("No AWS boto client provided")

        if not hasattr(self.boto_client, self.delete_api):
            raise ValueError(f"AWS client does not support method: {self.delete_api}")
        
        # Prepare delete params (copy to avoid mutating original)
        delete_params = dict(self.delete_params)

        # If resource_field and target_param_key are provided, extract the resource ID from the response of MCP tool execution
        if self.resource_field and self.target_param_key:
            try:
                content_list = actual_response["result"]["content"]
                first_text = content_list[0]["text"]
                parsed = json.loads(first_text)
                resource_id = parsed.get(self.resource_field)
                if not resource_id:
                    raise ValueError(f"Cannot extract {self.resource_field} from response")

                if self.param_is_list:
                    delete_params[self.target_param_key] = [resource_id]
                else:
                    delete_params[self.target_param_key] = resource_id

                print(f"[Cleanup] Successfully parsed resource ID: {resource_id} to delete params")
            except Exception as e:
                print(f"[Cleanup Warning] Failed to parse resource ID: {e}")

        try:
            # Handle glue session wait before deletion
            if self.delete_api == "delete_session":
                session_id = delete_params.get("Id")
                if session_id:
                    for _ in range(60):  # wait max 60s
                        try:
                            describe_response = self.boto_client.get_session(Id=session_id)
                            state = describe_response.get("Session", {}).get("Status")
                            if state in ["READY", "FAILED", "TIMEOUT", "STOPPED"]:
                                time.sleep(5)  # wait a bit before deletion
                                break
                            time.sleep(1)
                        except Exception as e:
                            print(f"[Cleanup] Error checking session status: {e}")
                            break
                    else:
                        print(f"[Cleanup Warning] Timeout waiting for session {session_id} to be deletable")
            
            delete_fn = getattr(self.boto_client, self.delete_api)
            boto_params = self._convert_to_boto_keys(delete_params)  
            delete_fn(**boto_params)
        except Exception as e:
            print(f"[Cleanup Error] Failed to call {self.delete_api}: {e}")
            return
        
        # Handle job run cleanup waiting logic
        if self.delete_api == "batch_stop_job_run":
            job_name = delete_params.get("JobName")
            job_run_ids = delete_params.get("JobRunIds", [])
            if job_name and job_run_ids:
                for job_run_id in job_run_ids:
                    print(f"[Cleanup] Waiting for job run {job_run_id} to stop...")
                    for _ in range(60):  # maximum wait time of 60 seconds
                        try:
                            response = self.boto_client.get_job_run(JobName=job_name, RunId=job_run_id)
                            state = response["JobRun"]["JobRunState"]
                            print(f"[Cleanup] Current state of {job_run_id}: {state}")
                            if state in ["STOPPED", "FAILED", "SUCCEEDED", "TIMEOUT"]:
                                print(f"[Cleanup] Job run {job_run_id} has stopped.")
                                break
                            time.sleep(1)
                        except Exception as e:
                            print(f"[Cleanup] Error while checking job run status: {e}")
                            break
                time.sleep(30)  # Wait for 30 seconds to ensure the status is actually changed

@pytest.fixture(scope="session")
def aws_setup():
    """Set up AWS resources needed for testing."""
    # Setup AWS resources
    upload_script()
    create_non_mcp_job()
    create_athena_results_bucket()
    return True


def glue_database_test_cases(aws_clients) -> List[MCPTestCase]:
    return [

        MCPTestCase(
            test_name="create_database_basic",
            tool_name="manage_aws_glue_databases",
            input_params={
                "operation": "create-database",
                "database_name": "mcp_test_database",
                "description": "Test database created by MCP server"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully created database"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_database",
                    operation_input_params={"Name": "mcp_test_database"},
                    expected_keys=["description"]
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="delete_database",
                    delete_params={"Name": "mcp_test_database"},
                    boto_client=aws_clients["glue"]
                )
            ]
        ),
        MCPTestCase(
            test_name="get_database_basic",
            tool_name="manage_aws_glue_databases",
            input_params={
                "operation": "get-database",
                "database_name": "mcp_test_database"
            },
            dependencies=["create_database_basic"],
            validators=[
                ContainsTextValidator("Successfully retrieved database")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="create_database_already_exists",
            tool_name="manage_aws_glue_databases",
            input_params={
                "operation": "create-database",
                "database_name": "mcp_test_database",
                "description": "Test database created by MCP server"
            },
            dependencies=["create_database_basic"],
            validators=[
                ContainsTextValidator("already exists")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="create_database_missing_name",
            tool_name="manage_aws_glue_databases",
            input_params={
                "operation": "create-database",
                "description": "Test database created by MCP server"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("database_name is required for create-database operation")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="create_database_with_definition",
            tool_name="manage_aws_glue_databases",
            input_params={
                "operation": "create-database",
                "database_name": "mcp_test_database_with_definition",
                "description": "Test database with definition created by MCP server",
                "location_uri": "s3://mcp-test-script-s3/"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully created database"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_database",
                    operation_input_params={"Name": "mcp_test_database_with_definition"},
                    expected_keys=[
                        "Database.Description",
                        "Database.LocationUri"
                    ]
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="delete_database",
                    delete_params={"Name": "mcp_test_database_with_definition"},
                    boto_client=aws_clients["glue"]
                )
            ]
        ),
        MCPTestCase(
            test_name="get_database_not_exist",
            tool_name="manage_aws_glue_databases",
            input_params={
                "operation": "get-database",
                "database_name": "non_existent_database"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("not found")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="get_databases_all",
            tool_name="manage_aws_glue_databases",
            input_params={
                "operation": "list-databases",
                "max_results": 10
            },
            dependencies=["create_database_basic", "create_database_with_definition"],
            validators=[
                ContainsTextValidator("Successfully listed 2 databases", expected_count=2)
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="update_database_description",
            tool_name="manage_aws_glue_databases",
            input_params={
                "operation": "update-database",
                "database_name": "mcp_test_database",
                "description": "Updated description for MCP test database"
            },
            dependencies=["create_database_basic"],
            validators=[
                ContainsTextValidator("Successfully updated database"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_database",
                    operation_input_params={"Name": "mcp_test_database"},
                    expected_keys=["description"]
                )
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="update_database_missing_name",
            tool_name="manage_aws_glue_databases",
            input_params={
                "operation": "update-database",
                "description": "Updated description for MCP test database"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("database_name is required for update-database operation")
            ],
            clean_ups=[]    
        ),
        MCPTestCase(
            test_name="update_database_not_exist",
            tool_name="manage_aws_glue_databases",
            input_params={
                "operation": "update-database",
                "database_name": "non_existent_database",
                "description": "Updated description for MCP test database"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("not found")
            ],
            clean_ups=[]
        ),
    
        MCPTestCase(
            test_name="delete_database_basic",
            tool_name="manage_aws_glue_databases",
            input_params={
                "operation": "delete-database",
                "database_name": "mcp_test_database"
            },
            dependencies=["create_database_basic"], 
            validators=[
                ContainsTextValidator("Successfully deleted database"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_database",
                    operation_input_params={"Name": "mcp_test_database"},
                    validate_absence=True
                )
            ],
            clean_ups=[]  
        ),
        MCPTestCase(
            test_name="delete_database_not_exist",
            tool_name="manage_aws_glue_databases",
            input_params={
                "operation": "delete-database", 
                "database_name": "non_existent_database"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("not found")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="delete_database_missing_name",
            tool_name="manage_aws_glue_databases",
            input_params={
                "operation": "delete-database"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("database_name is required for delete-database operation")
            ],
            clean_ups=[]
        ),
    ]
def glue_table_test_cases(aws_clients) -> List[MCPTestCase]:
    return [
        MCPTestCase(
            test_name="create_table_basic",
            tool_name="manage_aws_glue_tables",
            input_params={
                "operation": "create-table",
                "database_name": "mcp_test_database",
                "table_name": "mcp_test_table",
                "table_input": {
                    "Description": "Test table",
                    "StorageDescriptor": {
                        "Columns": [{"Name": "id", "Type": "int"}],
                        "Location": "s3://mcp-test-bucket/",
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                            "Parameters": {"serialization.format": "1"}
                        }
                    }
                }
            },
            dependencies=["create_database_basic"],  
            validators=[
                ContainsTextValidator("Successfully created table"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_table",
                    operation_input_params={"database_name": "mcp_test_database", "table_name": "mcp_test_table"},
                    expected_keys=["Description"],
                )
            ],
            clean_ups=[] # No clean up here, as the database will be deleted in the database test case
        ),

        MCPTestCase(
            test_name="get_table_basic",
            tool_name="manage_aws_glue_tables",
            input_params={
                "operation": "get-table",
            "database_name": "mcp_test_database",
            "table_name": "mcp_test_table"
        },
        dependencies=["create_table_basic"],
        validators=[
            ContainsTextValidator("Successfully retrieved table")
        ],
        clean_ups=[]
    ),
            MCPTestCase(
            test_name="create_table_with_partition_keys",
            tool_name="manage_aws_glue_tables",
            input_params={
                "operation": "create-table",
                "database_name": "mcp_test_database",
                "table_name": "mcp_test_table_with_partition_keys",
                "table_input": {
                    "Description": "Test table",
                    "PartitionKeys": [ 
                        {"Name": "event_date", "Type": "string"}
                    ],
                    "StorageDescriptor": {
                        "Columns": [{"Name": "id", "Type": "int"}],
                        "Location": "s3://mcp-test-bucket/",
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                            "Parameters": {"serialization.format": "1"}
                        }
                    }
                }
            },
            dependencies=["create_database_basic"],  
            validators=[
                ContainsTextValidator("Successfully created table"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_table",
                    operation_input_params={"database_name": "mcp_test_database", "table_name": "mcp_test_table_with_partition_keys"},
                    expected_keys=["Description"],
                )
            ],
            clean_ups=[] # No clean up here, as the database will be deleted in the database test case
        ),
        
    MCPTestCase(
        test_name="create_table_with_description",
        tool_name="manage_aws_glue_tables",
        input_params={
            "operation": "create-table",
            "database_name": "mcp_test_database",
            "table_name": "mcp_test_table_with_description",
            "table_input": {
                "Description": "Test table with description"
            }
        },
        dependencies=["create_database_basic"],
        validators=[
            ContainsTextValidator("Successfully created table"),
            AWSBotoValidator(
                boto_client=aws_clients["glue"],
                operation="get_table",
                operation_input_params={"database_name": "mcp_test_database", "table_name": "mcp_test_table_with_description"},
                expected_keys=["Description"]
            )
        ],
        clean_ups=[]
    ),
    MCPTestCase(
        test_name="get_table_not_exist",
        tool_name="manage_aws_glue_tables",
        input_params={
            "operation": "get-table",
            "database_name": "mcp_test_database",
            "table_name": "non_existent_table"
        },
        dependencies=[],
        validators=[
            ContainsTextValidator("not found")
        ],
        clean_ups=[]
    ),
    MCPTestCase(
        test_name="create_table_missing_database",
        tool_name="manage_aws_glue_tables",
        input_params={
            "operation": "create-table",
            "table_name": "mcp_test_table_missing_db"
        },
        dependencies=[],
        validators=[
            ContainsTextValidator("database_name, table_input and table_name are required for create-table operation")
        ],
        clean_ups=[]
    ),
    MCPTestCase(
        test_name="create_table_missing_table_name",
        tool_name="manage_aws_glue_tables",
        input_params={
            "operation": "create-table",
            "database_name": "mcp_test_database"
        },
        dependencies=["create_database_basic"],
        validators=[
            ContainsTextValidator("database_name, table_input and table_name are required for create-table operation")
        ],
        clean_ups=[]
    ),
    MCPTestCase(
        test_name="update_table_description",
        tool_name="manage_aws_glue_tables",
        input_params={
            "operation": "update-table",
            "database_name": "mcp_test_database",
            "table_name": "mcp_test_table",
            "table_input": {
                "Description": "Updated table description"
                }
        },
        dependencies=["create_table_basic"],
        validators=[
            ContainsTextValidator("Successfully updated table"),
            AWSBotoValidator(
                boto_client=aws_clients["glue"],
                operation="get_table",
                operation_input_params={"database_name": "mcp_test_database", "table_name": "mcp_test_table"},
                expected_keys=["Description"]
            )
        ],
        clean_ups=[]
    ),

    MCPTestCase(
        test_name="list_tables_in_db",
        tool_name="manage_aws_glue_tables",
        input_params={
            "operation": "list-tables",
            "database_name": "mcp_test_database",
            "max_results": 10
        },
        dependencies=["create_table_basic", "create_table_with_description"],
        validators=[
            ContainsTextValidator("Successfully listed 2 tables in database mcp_test_database", expected_count=2)
        ],
        clean_ups=[]
    ),

    MCPTestCase(
        test_name="delete_table_basic",
        tool_name="manage_aws_glue_tables",
        input_params={
            "operation": "delete-table",
            "database_name": "mcp_test_database",
            "table_name": "mcp_test_table"
        },
        dependencies=["create_table_basic"],
        validators=[
            ContainsTextValidator("Successfully deleted table"),
            AWSBotoValidator(
                boto_client=aws_clients["glue"],
                operation="get_table",
                operation_input_params={"database_name": "mcp_test_database", "table_name": "mcp_test_table"},
                validate_absence=True
            )
        ],
        clean_ups=[]
    ),
    MCPTestCase(
        test_name="search_table_for_databases",
        tool_name="manage_aws_glue_tables",
        input_params={
            "operation": "search-tables",
            "database_name": "mcp_test_database",
            "search_text": "mcp_test_table_with_description"
        },
        dependencies=["create_table_with_description"],
        validators=[
            ContainsTextValidator("Search found", expected_count=1)
        ],
        clean_ups=[]
    )
    ]

def emr_cluster_test_cases(aws_clients) -> List[MCPTestCase]:
    return [
        MCPTestCase(
            test_name="create_emr_cluster_basic",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "create-cluster",
                "name": "mcp-test-emr-cluster",
                "release_label": "emr-6.3.0",
                "applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
                "instances": {
                    "InstanceGroups": [
                        {
                            "Name": "Master",
                            "InstanceRole": "MASTER",
                            "InstanceType": "m5.xlarge",
                            "InstanceCount": 1
                        },
                        {
                            "Name": "Core",
                            "InstanceRole": "CORE",
                            "InstanceType": "m5.xlarge",
                            "InstanceCount": 2
                        }
                    ],
                    "Ec2KeyName": "your-ec2-key-name",
                    "KeepJobFlowAliveWhenNoSteps": True
                },
                "service_role": "EMR_DefaultRole",
                "job_flow_role": "EMR_EC2_DefaultRole"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully created EMR cluster"),
                AWSBotoValidator(
                    boto_client=aws_clients["emr"],
                    operation="describe_cluster",
                    operation_input_params={},
                    expected_keys=["Name", "ReleaseLabel", "Instances"],
                    injectable_params={"cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}"}
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="terminate_job_flows",
                    boto_client=aws_clients["emr"],
                    resource_field="cluster_id",
                    target_param_key="JobFlowIds",
                    param_is_list=True,
                )   
            ]
        ),
        MCPTestCase(
            test_name="create_emr_cluster_missing_name",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "create-cluster",
                "release_label": "emr-6.3.0",
                "applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
                "instance_type": "m5.xlarge",
                "instance_count": 2,
                "ec2_key_name": "your-ec2-key-name"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("name, release_label, and instances are required for create-cluster operation")
            ],
            clean_ups=[]
        ),

        MCPTestCase(
            test_name="list_emr_clusters",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "list-clusters",
                "max_results": 10
            },
            dependencies=["create_emr_cluster_basic"],
            validators=[
                ContainsTextValidator("Successfully listed EMR clusters")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="describe_emr_cluster",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "describe-cluster",
                "cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}" # Use the cluster_id from the dependency's response
            },
            dependencies=["create_emr_cluster_basic"],
            validators=[
                ContainsTextValidator("Successfully described EMR cluster")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="describe_emr_cluster_missing_id",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "describe-cluster"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("cluster_id is required for describe-cluster operation")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="modify_emr_cluster",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "modify-cluster",
                "cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}", # Use the cluster_id from the dependency's response
                "name": "mcp-test-emr-cluster-modified",
                "step_concurrency_level": 1
            },
            dependencies=["create_emr_cluster_basic"],
            validators=[
                ContainsTextValidator("Successfully modified EMR cluster"),
                AWSBotoValidator(
                    boto_client=aws_clients["emr"],
                    operation="describe_cluster",
                    operation_input_params={},
                    expected_keys=["Name"],
                    injectable_params= {"cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}"}
                )
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="modify_emr_cluster_missing_id",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "modify-cluster",
                "name": "mcp-test-emr-cluster-modified"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("cluster_id is required for modify-cluster operation")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="modify_emr_cluster_attribute",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "modify-cluster-attributes",
                "cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}", 
                "attribute": "StepConcurrencyLevel",
                "value": 2,
                "auto_terminate": True
            },
            dependencies=["create_emr_cluster_basic"],
            validators=[
                ContainsTextValidator("Successfully modified attributes for EMR cluster"),
                AWSBotoValidator(
                    boto_client=aws_clients["emr"],
                    operation="describe_cluster",
                    operation_input_params={},  
                    expected_keys=["StepConcurrencyLevel"],
                    injectable_params={"cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}"}
                )
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="terminate_emr_cluster",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "terminate-clusters",
                "cluster_ids": ["{{create_emr_cluster_basic.result.content[0].text.cluster_id}}"]
            },
            dependencies=["create_emr_cluster_basic"],
            validators=[
                ContainsTextValidator("Successfully initiated termination for 1 MCP-managed EMR clusters")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="terminate_emr_cluster_missing_id",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "terminate-clusters"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("cluster_ids is required for terminate-clusters operation")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="create_cluster_with_fleet",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "create-cluster",
                "name": "mcp-test-emr-cluster-with-fleet",
                "release_label": "emr-6.3.0",
                "applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
                "instances": {
                    "InstanceFleets": [
                        {
                            "Name": "Master Fleet",
                            "InstanceFleetType": "MASTER",
                            "TargetOnDemandCapacity": 1,
                            "InstanceTypeConfigs": [
                                {
                                    "InstanceType": "m5.xlarge",
                                    "WeightedCapacity": 1
                                }
                            ]
                        },
                        {
                            "Name": "Core Fleet",
                            "InstanceFleetType": "CORE",
                            "TargetOnDemandCapacity": 2,
                            "InstanceTypeConfigs": [
                                {
                                    "InstanceType": "m5.xlarge",
                                    "WeightedCapacity": 1
                                }
                            ]
                        }
                    ],
                    "Ec2KeyName": "your-ec2-key-name",
                    "KeepJobFlowAliveWhenNoSteps": True
                },
                "service_role": "EMR_DefaultRole",
                "job_flow_role": "EMR_EC2_DefaultRole"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully created EMR cluster"),
                AWSBotoValidator(
                    boto_client=aws_clients["emr"],
                    operation="describe_cluster",
                    operation_input_params={},
                    expected_keys=["InstanceFleets"],
                    injectable_params={"cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}"}
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="terminate_job_flows",
                    boto_client=aws_clients["emr"],
                    resource_field="cluster_id",
                    target_param_key="JobFlowIds",
                    param_is_list=True,
                )   
            ]
        ),
    ]
def emr_step_test_cases(aws_clients) -> List[MCPTestCase]:
    return [
        MCPTestCase(
            test_name="add_step_to_emr_cluster",
            tool_name="manage_aws_emr_ec2_steps",
            input_params={
                "operation": "add-steps",
                "cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}",  # Use the cluster_id from the dependency's response
                "steps": [
                    {
                        "Name": "SparkExampleStep",
                        "ActionOnFailure": "CONTINUE",
                        "HadoopJarStep": {
                            "Jar": "command-runner.jar",
                            "Args": [
                                "spark-submit",
                                "--deploy-mode",
                                "cluster",
                                "s3://your-bucket/path/to/app.jar",
                                "--arg1", "val1"
                            ]
                        }
                    }
                ]
            },
            dependencies=["create_emr_cluster_basic"],
            validators=[
                ContainsTextValidator("Successfully added 1 steps to EMR cluster"),
                AWSBotoValidator(
                    boto_client=aws_clients["emr"],
                    operation="describe_step",
                    operation_input_params={},
                    expected_keys=["StepId"],
                    injectable_params={"cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}",
                                       "step_id": "{{add_step_to_emr_cluster.result.content[0].text.step_ids[0]}}"}
                )
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="add_step_missing_cluster_id",
            tool_name="manage_aws_emr_ec2_steps",
            input_params={
                "operation": "add-step",
                "steps": {
                    "Name": "MCP Test Step",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["spark-submit", "--deploy-mode", "cluster", "script.py"]
                    }
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Field required")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="describe_emr_step",
            tool_name="manage_aws_emr_ec2_steps",
            input_params={
                "operation": "describe-step",
                "cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}",  # Use the cluster_id from the dependency's response
                "step_id": "{{add_step_to_emr_cluster.result.content[0].text.step_ids[0]}}"  # Use the step_id from the dependency's response
            },
            dependencies=["add_step_to_emr_cluster"],
            validators=[
                ContainsTextValidator("Successfully described step")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="describe_emr_step_missing_ids",
            tool_name="manage_aws_emr_ec2_steps",
            input_params={
                "operation": "describe-step"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Field required")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="list_emr_steps",
            tool_name="manage_aws_emr_ec2_steps",
            input_params={  
                "operation": "list-steps",
                "cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}",  # Use the cluster_id from the dependency's response
                "max_results": 10
            },
            dependencies=["add_step_to_emr_cluster"],
            validators=[
                ContainsTextValidator("Successfully listed steps for EMR cluster", expected_count=1)
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="list_emr_steps_missing_cluster_id",
            tool_name="manage_aws_emr_ec2_steps",
            input_params={
                "operation": "list-steps",
                "max_results": 10
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Field required")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="cancel_emr_step",
            tool_name="manage_aws_emr_ec2_steps",
            input_params={
                "operation": "cancel-steps",
                "cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}",  # Use the cluster_id from the dependency's response
                "step_ids": ["{{add_step_to_emr_cluster.result.content[0].text.step_ids[0]}}"]  # Use the step_id from the dependency's response
            },
            dependencies=["add_step_to_emr_cluster"],
            validators=[
                ContainsTextValidator("Successfully initiated cancellation for 1 steps")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="cancel_emr_step_missing_ids",
            tool_name="manage_aws_emr_ec2_steps",
            input_params={
                "operation": "cancel-steps"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Field required")
            ],
            clean_ups=[]
        )

    ]

def emr_instance_test_cases(aws_clients) -> List[MCPTestCase]:
    return[
        MCPTestCase(
            test_name="add_instance_fleet_to_emr_cluster",
            tool_name="manage_aws_emr_ec2_instances",
            input_params={
                "operation": "add-instance-fleet",
                "cluster_id": "{{create_cluster_with_fleet.result.content[0].text.cluster_id}}",  # Use the cluster_id from the dependency's response
                "instance_fleet": {
                    "InstanceFleetType": "TASK",
                    "Name": "mcp-test-fleet",
                    "TargetOnDemandCapacity": 1,
                    "TargetSpotCapacity": 1,
                    "InstanceTypeConfigs": [
                    {
                        "InstanceType": "m5.xlarge",
                        "WeightedCapacity": 1,
                        "BidPriceAsPercentageOfOnDemandPrice": 80
                    }
                    ]
                }
            },
            dependencies=["create_cluster_with_fleet"],
            validators=[
                ContainsTextValidator("Successfully added instance fleet to EMR cluster"),
                AWSBotoValidator(
                    boto_client=aws_clients["emr"],
                    operation="list_instance_fleets",
                    operation_input_params={},
                    expected_keys=["InstanceFleetId"],
                    injectable_params={"cluster_id": "{{create_cluster_with_fleet.result.content[0].text.cluster_id}}"}
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="terminate_job_flows",
                    boto_client=aws_clients["emr"],
                    resource_field="cluster_id",
                    target_param_key="JobFlowIds",
                    param_is_list=True,
                )
            ]
        ),
        MCPTestCase(
            test_name="add_instance_fleet_missing_cluster_id",
            tool_name="manage_aws_emr_ec2_instances",
            input_params={
                "operation": "add-instance-fleet",
                "instance_fleet": {
                    "InstanceFleetType": "TASK",
                    "Name": "mcp-test-fleet",
                    "TargetOnDemandCapacity": 1,
                    "TargetSpotCapacity": 1,
                    "InstanceTypeConfigs": [
                        {
                            "InstanceType": "m5.xlarge",
                            "WeightedCapacity": 1,
                            "BidPriceAsPercentageOfOnDemandPrice": 80
                        }
                    ]
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("cluster_id and instance_fleet are required")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="add_instance_group_to_emr_cluster_basic",
            tool_name="manage_aws_emr_ec2_instances",
            input_params={
                "operation": "add-instance-groups",
                "cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}",  # Use the cluster_id from the dependency's response
                "instance_groups": [
                    {
                    "Name": "TestCoreGroup",
                    "InstanceRole": "TASK",
                    "InstanceType": "m1.small",
                    "InstanceCount": 2
                    }
                ]
            },
            dependencies=["create_emr_cluster_basic"],
            validators=[
                ContainsTextValidator("Successfully added instance groups to EMR cluster"),
                AWSBotoValidator(
                    boto_client=aws_clients["emr"],
                    operation="list_instance_groups",
                    operation_input_params={},
                    expected_keys=["InstanceGroupId"],
                    injectable_params={"cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}"}
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="terminate_job_flows",
                    boto_client=aws_clients["emr"],
                    resource_field="cluster_id",
                    target_param_key="JobFlowIds",
                    param_is_list=True,
                )
            ]
        ),
        MCPTestCase(
            test_name="modify_instance_fleet_missing_ids",
            tool_name="manage_aws_emr_ec2_instances",
            input_params={
                "operation": "modify-instance-fleet",
                "instance_fleet": {
                    "Name": "mcp-test-fleet-modified",
                    "TargetOnDemandCapacity": 2,
                    "TargetSpotCapacity": 2,
                    "InstanceTypeConfigs": [
                        {
                            "InstanceType": "m5.xlarge",
                            "WeightedCapacity": 1,
                            "BidPriceAsPercentageOfOnDemandPrice": 80
                        }
                    ]
                }   
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("cluster_id, instance_fleet_id, and instance_fleet_config are required")
            ],
            clean_ups=[]
        ),
        
        MCPTestCase(
            test_name="modify_instance_group_missing_ids",
            tool_name="manage_aws_emr_ec2_instances",
            input_params={
                "operation": "modify-instance-groups",
                "instance_group": {
                    "InstanceCount": 3,
                    "InstanceType": "m5.xlarge"
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("instance_group_configs is required for modify-instance-groups operation")
            ],
            clean_ups=[]
        ),

        MCPTestCase(
            test_name="list_instance_fleets",
            tool_name="manage_aws_emr_ec2_instances",
            input_params={
                "operation": "list-instance-fleets",
                "cluster_id": "{{create_cluster_with_fleet.result.content[0].text.cluster_id}}",  # Use the cluster_id from the dependency's response
                "max_results": 10
            },
            dependencies=["add_instance_fleet_to_emr_cluster"],
            validators=[
                ContainsTextValidator("Successfully listed instance fleets for EMR cluster", expected_count=3) # 1 master, 1 core, 1 task
            ],
            clean_ups=[]
        ),

        MCPTestCase(
            test_name="list_instance_groups",
            tool_name="manage_aws_emr_ec2_instances",
            input_params={
                "operation": "list-instances",
                "cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}", 
                "max_results": 10
            },
            dependencies=["add_instance_group_to_emr_cluster_basic"],
            validators=[
                ContainsTextValidator("uccessfully listed instances for EMR cluster ") 
            ],
            clean_ups=[]
        ),

        MCPTestCase(
            test_name="list_supported_instance_types",
            tool_name="manage_aws_emr_ec2_instances",
            input_params={
                "operation": "list-supported-instance-types",
                "release_label": "emr-6.3.0",
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully listed supported instance types for EMR")
            ],
            clean_ups=[]
        ),
    ]

def s3_test_cases(aws_clients) -> List[MCPTestCase]:
    return [
        MCPTestCase(
            test_name="list_s3_buckets_default_region",
            tool_name="list_s3_buckets",
            input_params={},
            dependencies=[],
            validators=[
                ContainsTextValidator("Looking for S3 buckets")
            ],
            clean_ups=[]
        ),  
        MCPTestCase(
            test_name="list_s3_buckets_with_region",
            tool_name="list_s3_buckets",
            input_params={
                "region": os.environ.get("AWS_REGION", "us-west-2")
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Looking for S3 buckets", bucket_count=0)
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="list_s3_buckets_with_invalid_region",
            tool_name="list_s3_buckets",
            input_params={
                "region": "invalid-region"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Error listing S3 buckets: An error occurred (InvalidRegion) when calling the ListBuckets operation: The region 'invalid-region' is invalid")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="upload_file_to_s3",
            tool_name="upload_to_s3",
            input_params={
                "bucket_name": "mcp-test-script-s3",
                "code_content": "print('Hello from MCP S3 test')",
                "s3_key": "mcp-test-upload-s3.py"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Python code uploaded successfully!"),
                AWSBotoValidator(
                    boto_client=aws_clients["s3"],
                    operation="head_object",
                    operation_input_params={
                        "Bucket": "mcp-test-script-s3",
                        "Key": "mcp-test-upload-s3.py"
                    },
                    expected_keys=["ContentLength", "LastModified"]
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="delete_object",
                    delete_params={"Bucket": "mcp-test-script-s3", "Key": "mcp-test-upload-s3.py"},
                    boto_client=aws_clients["s3"]
                )   
            ]
        ),
        MCPTestCase(
            test_name="upload_file_to_s3_missing_bucket",
            tool_name="upload_to_s3",
            input_params={
                "code_content": "print('Hello from MCP S3 test')",
                "s3_key": "mcp-test-upload-s3.py"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Field required")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="analyze_all_s3_usage",
            tool_name="analyze_s3_usage_for_data_processing",
            input_params={
                "bucket_name": None
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Analyzing bucket")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="analyze_s3_usage_with_bucket",
            tool_name="analyze_s3_usage_for_data_processing",
            input_params={
                "bucket_name": "mcp-test-script-s3"
            },
            dependencies=["upload_file_to_s3"],
            validators=[
                ContainsTextValidator("Analyzing bucket")
            ],
            clean_ups=[]
        )
    ]
def iam_test_cases(glue_role, aws_clients) -> List[MCPTestCase]:
    return [
        MCPTestCase(
            test_name="add_inline_policy_to_role",
            tool_name="add_inline_policy",
            input_params={
                "policy_name": "mcp-test-inline-policy",
                "role_name": "mcp-test-glue-role",
                "permissions": {
                    "Effect": "Allow",
                    "Action": [
                        "glue:*",
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket",
                        "iam:PassRole"
                    ],
                    "Resource": "*"
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("mcp-test-inline-policy"),
                AWSBotoValidator(
                    boto_client=aws_clients["iam"],
                    operation="list_role_policies",
                    operation_input_params={"RoleName": "mcp-test-glue-role"},
                    expected_keys=["policy_name"],
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="delete_role_policy",
                    delete_params={"RoleName": "mcp-test-glue-role", "PolicyName": "mcp-test-inline-policy"},
                    boto_client=aws_clients["iam"]
                )
            ]
        ),
        MCPTestCase(
            test_name="add_inline_policy_missing_role",
            tool_name="add_inline_policy",
            input_params={
                "policy_name": "mcp-test-inline-policy",
                "permissions": {
                    "Effect": "Allow",
                    "Action": ["glue:*"],
                    "Resource": "*"
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Field required")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="get_policies_for_role",
            tool_name="get_policies_for_role",
            input_params={
                "role_name": "mcp-test-glue-role"
            },
            dependencies=["add_inline_policy_to_role"],
            validators=[
                ContainsTextValidator("Successfully retrieved details for IAM role")
            ],  
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="get_policies_for_role_missing_role",
            tool_name="get_policies_for_role",
            input_params={},
            dependencies=[],
            validators=[
                ContainsTextValidator("Field required")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="create_data_processing_role",
            tool_name="create_data_processing_role",
            input_params={
                "role_name": "mcp-test-data-processing-role",
                "description": "Role for MCP data processing tests",
                "service_type": "glue"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully created IAM role"),
                AWSBotoValidator(
                    boto_client=aws_clients["iam"],
                    operation="get_role",
                    operation_input_params={"RoleName": "mcp-test-data-processing-role"},
                    expected_keys=["RoleName", "Description"]
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="delete_role",
                    delete_params={"RoleName": "mcp-test-data-processing-role"},
                    boto_client=aws_clients["iam"]
                )   
            ]   
        ),
        MCPTestCase(
            test_name="create_data_processing_role_missing_name",
            tool_name="create_data_processing_role",
            input_params={
                "description": "Role for MCP data processing tests",
                "service_type": "glue"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Field required")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="get_roles_for_service",
            tool_name="get_roles_for_service",
            input_params={
                "service_type": "glue"
            },  
            dependencies=["create_data_processing_role"],
            validators=[
                ContainsTextValidator("Successfully retrieved")
            ],  
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="get_roles_for_service_missing_type",
            tool_name="get_roles_for_service",
            input_params={},
            dependencies=[],
            validators=[
                ContainsTextValidator("Field required")
            ],
            clean_ups=[]
        )
    ]


def athena_named_query_test_cases(aws_clients) -> List[MCPTestCase]:
    return [
        MCPTestCase(
            test_name="create_athena_query",
            tool_name="manage_aws_athena_named_queries",
            input_params={
                "operation": "create-named-query",
                "name": "mcp_test_named_query",
                "description": "This is a test named query for MCP validation.",
                "database": "mcp_test_database",
                "query_string": "SELECT 1",
                "work_group": "primary"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully created named")
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="delete_named_query",
                    boto_client=aws_clients["athena"],
                    resource_field="named_query_id",        
                    target_param_key="NamedQueryId",        
                    param_is_list=False
                )
            ]
        ),
        MCPTestCase(
            test_name="get_athena_queries",
            tool_name="manage_aws_athena_named_queries",
            input_params={
                "operation": "list-named-queries",
                "max_results": 10
            },
            dependencies=["create_athena_query"],
            validators=[
                ContainsTextValidator("Successfully listed named queries")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="batch_get_athena_named_queries",
            tool_name="manage_aws_athena_named_queries",
            input_params={
                "operation": "batch-get-named-query",
                "named_query_ids": ["{{create_athena_query.result.content[0].text.named_query_id}}"]
            },
            dependencies=["create_athena_query"],
            validators=[
                ContainsTextValidator("Successfully retrieved named queries")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="get_athena_named_query",
            tool_name="manage_aws_athena_named_queries",
            input_params={
                "operation": "get-named-query",
                "named_query_id": "{{create_athena_query.result.content[0].text.named_query_id}}"
            },
            dependencies=["create_athena_query"],
            validators=[
                ContainsTextValidator("Successfully retrieved named query")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="get_athena_named_query_missing_id",
            tool_name="manage_aws_athena_named_queries",
            input_params={
                "operation": "get-named-query"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("named_query_id is required for get-named-query operation")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="update_athena_named_query",
            tool_name="manage_aws_athena_named_queries",
            input_params={
                "operation": "update-named-query",  
                "named_query_id": "{{create_athena_query.result.content[0].text.named_query_id}}",
                "name": "mcp_test_named_query_updated",
                "description": "This is an updated test named query for MCP validation.",
                "query_string": "SELECT 2",
                "work_group": "primary"
            },
            dependencies=["create_athena_query"],
            validators=[
                ContainsTextValidator("Successfully updated named query")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="update_athena_named_query_missing_id",
            tool_name="manage_aws_athena_named_queries",
            input_params={
                "operation": "update-named-query",
                "name": "mcp_test_named_query_updated",
                "description": "This is an updated test named query for MCP validation.",
                "query_string": "SELECT 2",
                "work_group": "primary"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("named_query_id is required for update-named-query operation")
            ],
            clean_ups=[]
        ),  
        MCPTestCase(
            test_name="delete_athena_named_query",
            tool_name="manage_aws_athena_named_queries",
            input_params={
                "operation": "delete-named-query",
                "named_query_id": "{{create_athena_query.result.content[0].text.named_query_id}}"
            },
            dependencies=["create_athena_query"],
            validators=[
                ContainsTextValidator("Successfully deleted named query")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="delete_athena_named_query_missing_id",
            tool_name="manage_aws_athena_named_queries",
            input_params={
                "operation": "delete-named-query"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("named_query_id is required for delete-named-query operation")
            ],
            clean_ups=[]
        )
    ]

def load_all_grouped_test_cases(glue_role, aws_clients) -> List[Tuple[str, List[MCPTestCase]]]:
    """Create test cases with proper role ARN."""
    all_test_cases = (
         glue_database_test_cases(aws_clients)
        + glue_table_test_cases(aws_clients)
         + athena_named_query_test_cases(aws_clients)
         + emr_cluster_test_cases(aws_clients)
        + emr_step_test_cases(aws_clients)
        + emr_instance_test_cases(aws_clients)
        + s3_test_cases(aws_clients)
        + iam_test_cases(glue_role, aws_clients)

    )
    def classify_tool_name(tool_name: str) -> str:
        lower_name = tool_name.lower()
        if "glue" in lower_name:
            return "glue"
        elif "emr" in lower_name:
            return "emr"
        elif "athena" in lower_name:
            return "athena"
        elif "s3" in lower_name:
            return "s3"
        else:
            return "iam"

    grouped = defaultdict(list)
    for case in all_test_cases:
        group_name = classify_tool_name(case.tool_name)
        grouped[group_name].append(case)

    return list(grouped.items())

@pytest.fixture(scope="session")
def test_executor(test_cases, mcp_env):
    """Create and execute test executor."""
    executor = Executor(test_cases, mcp_env)
    execution_results = executor.execute_tests()
    return execution_results

@pytest.fixture(scope="session")
def test_results(test_executor):
    """Extract test results from executor."""
    return test_executor['success_map']

@pytest.fixture(scope="session")
def test_details(test_executor):
    """Extract test details from executor."""
    return {result['test_name']: result for result in test_executor['results']}


@pytest.fixture(scope="session")
def aws_clients():
    config = Config(user_agent_extra='awslabs/mcp/aws-dataprocessing-mcp-server-test-framework/')
    session = boto3.Session(
        profile_name=os.environ.get("AWS_PROFILE"),
        region_name=os.environ.get("AWS_REGION")
    )
    services = ['glue', 'iam', 'emr', 's3', 'athena', 'sts']
    return {service: session.client(service, config=config) for service in services}

@pytest.fixture(scope="session")
def glue_role():
    return get_or_create_glue_role()

@pytest.fixture(scope="session")
def test_case_groups(glue_role, aws_clients):
    return load_all_grouped_test_cases(glue_role, aws_clients)

@pytest.fixture
def group(request, test_case_groups):
    return test_case_groups[request.param]

def pytest_generate_tests(metafunc):
    if "group" in metafunc.fixturenames:
        from inspect import getmodule
        module = getmodule(metafunc.function)
        test_case_groups = module.test_case_groups.__wrapped__(
            glue_role=module.glue_role.__wrapped__(),
            aws_clients=module.aws_clients.__wrapped__()
        )
        indices = list(range(len(test_case_groups)))
        ids = [name for name, _ in test_case_groups]
        metafunc.parametrize("group", indices, indirect=True, ids=ids)

def test_tool_group(group, mcp_env):
    """Execute a group of test cases and validate results."""
    tool_name, test_cases = group
    executor = Executor(test_cases, mcp_env)
    execution_results = executor.execute_tests()

    failed_messages = []

    for result in execution_results["results"]:
        test_name = result["test_name"]
        validations = result.get("validations", [])

        for v in validations:
            if not v.success:
                msg = f"[{tool_name}] {test_name} failed: {v.error_message}"
                failed_messages.append(msg)

    if failed_messages:
        full_error_message = "\n".join(failed_messages)
        pytest.fail(f"Some test cases failed:\n{full_error_message}")
