from typing import List

from data_processing_mcp_server_tests.core.mcp_validators import ContainsTextValidator, AWSBotoValidator
from data_processing_mcp_server_tests.core.mcp_cleanup import DeleteAWSResources
from data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase

def glue_encryption_test_cases(aws_clients) -> List[MCPTestCase]:
    return [
        MCPTestCase(
            test_name="put_glue_encryption_settings",
            tool_name="manage_aws_glue_encryption",
            input_params={
                "operation": "put-catalog-encryption-settings",
                "encryption_at_rest": { 
                    "CatalogEncryptionMode": "SSE-KMS",
                    "SseAwsKmsKeyId": "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012"
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully updated Data Catalog encryption settings"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_data_catalog_encryption_settings",
                    operation_input_params={},
                    expected_keys=["encryption_at_rest"]
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="delete_data_catalog_encryption_settings",
                    boto_client=aws_clients["glue"]
                )
            ]
        ),
        MCPTestCase(
            test_name="get_glue_encryption_settings",
            tool_name="manage_aws_glue_encryption",
            input_params={
                "operation": "get-catalog-encryption-settings"
            },
            dependencies=["put_glue_encryption_settings"],
            validators=[
                ContainsTextValidator("Successfully retrieved Data Catalog encryption settings")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="put_glue_encryption_settings_missing_params",
            tool_name="manage_aws_glue_encryption",
            input_params={
                "operation": "put-catalog-encryption-settings"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Either encryption_at_rest or connection_password_encryption is required for put-catalog-encryption-settings operation")
            ],
            clean_ups=[]
        )
    ]
