from typing import List

from data_processing_mcp_server_tests.core.mcp_validators import ContainsTextValidator, AWSBotoValidator
from data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase
from data_processing_mcp_server_tests.core.mcp_cleanup import DeleteAWSResources

def glue_security_configurations_test_cases(aws_clients) -> List[MCPTestCase]:
    return [
        MCPTestCase(
            test_name="create_security_configuration_basic",
            tool_name="manage_aws_glue_security_configurations",
            input_params={
                "operation": "create-security-configuration",
                "config_name": "mcp_test_security_config",
                "encryption_configuration": {
                        "S3Encryption": [
                            {
                            "S3EncryptionMode": "SSE-KMS",
                            "KmsKeyArn": "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012"
                        }
                    ]
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully created security configuration"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_security_configuration",
                    operation_input_params={"Name": "mcp_test_security_config"},
                    expected_keys=["Name", "EncryptionConfiguration"]
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="delete_security_configuration",
                    delete_params={"Name": "mcp_test_security_config"},
                    boto_client=aws_clients["glue"]
                )
            ]
        ),
        MCPTestCase(
            test_name="get_security_configuration_basic",
            tool_name="manage_aws_glue_security_configurations",
            input_params={
                "operation": "get-security-configuration",
                "config_name": "mcp_test_security_config"
            },
            dependencies=["create_security_configuration_basic"],
            validators=[
                ContainsTextValidator("Successfully retrieved security configuration")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="create_security_configuration_missing_name",
            tool_name="manage_aws_glue_security_configurations",
            input_params={
                "operation": "create-security-configuration",
                "encryption_configuration": {
                    "S3Encryption": [
                        {
                            "S3EncryptionMode": "SSE-KMS",
                            "KmsKeyArn": "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012"
                        }
                    ]
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Field required")
            ],
            clean_ups=[]
        ),

        MCPTestCase(
            test_name="get_security_configuration_not_exist",
            tool_name="manage_aws_glue_security_configurations",
            input_params={
                "operation": "get-security-configuration",
                "config_name": "non_existent_config"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("An error occurred (EntityNotFoundException) ")
            ],
            clean_ups=[]
        ),

        MCPTestCase(
            test_name="delete_security_configuration",
            tool_name="manage_aws_glue_security_configurations",
            input_params={
                "operation": "delete-security-configuration",
                "config_name": "mcp_test_security_config"
            },
            dependencies=["create_security_configuration_basic"],
            validators=[
                ContainsTextValidator("Successfully deleted security configuration"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_security_configuration",
                    operation_input_params={"Name": "mcp_test_security_config"},
                    validate_absence=True
                )
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="delete_security_configuration_not_exist",
            tool_name="manage_aws_glue_security_configurations",
            input_params={
                "operation": "delete-security-configuration",
                "config_name": "non_existent_config"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Security configuration non_existent_config not found")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="delete_security_configuration_missing_name",
            tool_name="manage_aws_glue_security_configurations",
            input_params={
                "operation": "delete-security-configuration"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Field required")
            ],
            clean_ups=[]
        )
    ]
