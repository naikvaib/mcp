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

from data_processing_mcp_server_tests.core.mcp_cleanup import DeleteAWSResources
from data_processing_mcp_server_tests.core.mcp_validators import (
    AWSBotoValidator,
    ContainsTextValidator,
)
from data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase
from typing import List


def glue_security_configurations_test_cases(aws_clients) -> List[MCPTestCase]:
    """Glue security configuration test cases."""
    return [
        MCPTestCase(
            test_name='create_security_configuration_basic',
            tool_name='manage_aws_glue_security_configurations',
            input_params={
                'operation': 'create-security-configuration',
                'config_name': 'mcp_test_security_config',
                'encryption_configuration': {
                    'S3Encryption': [
                        {
                            'S3EncryptionMode': 'SSE-KMS',
                            'KmsKeyArn': 'arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012',
                        }
                    ]
                },
            },
            dependencies=[],
            validators=[
                ContainsTextValidator('Successfully created security configuration'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_security_configuration',
                    operation_input_params={'Name': 'mcp_test_security_config'},
                    expected_keys=['Name', 'EncryptionConfiguration'],
                ),
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api='delete_security_configuration',
                    delete_params={'Name': 'mcp_test_security_config'},
                    boto_client=aws_clients['glue'],
                )
            ],
        ),
        MCPTestCase(
            test_name='get_security_configuration_basic',
            tool_name='manage_aws_glue_security_configurations',
            input_params={
                'operation': 'get-security-configuration',
                'config_name': 'mcp_test_security_config',
            },
            dependencies=['create_security_configuration_basic'],
            validators=[ContainsTextValidator('Successfully retrieved security configuration')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='create_security_configuration_missing_name',
            tool_name='manage_aws_glue_security_configurations',
            input_params={
                'operation': 'create-security-configuration',
                'encryption_configuration': {
                    'S3Encryption': [
                        {
                            'S3EncryptionMode': 'SSE-KMS',
                            'KmsKeyArn': 'arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012',
                        }
                    ]
                },
            },
            dependencies=[],
            validators=[ContainsTextValidator('Field required')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_security_configuration_not_exist',
            tool_name='manage_aws_glue_security_configurations',
            input_params={
                'operation': 'get-security-configuration',
                'config_name': 'non_existent_config',
            },
            dependencies=[],
            validators=[ContainsTextValidator('An error occurred (EntityNotFoundException) ')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_security_configuration',
            tool_name='manage_aws_glue_security_configurations',
            input_params={
                'operation': 'delete-security-configuration',
                'config_name': 'mcp_test_security_config',
            },
            dependencies=['create_security_configuration_basic'],
            validators=[
                ContainsTextValidator('Successfully deleted security configuration'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_security_configuration',
                    operation_input_params={'Name': 'mcp_test_security_config'},
                    validate_absence=True,
                ),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_security_configuration_not_exist',
            tool_name='manage_aws_glue_security_configurations',
            input_params={
                'operation': 'delete-security-configuration',
                'config_name': 'non_existent_config',
            },
            dependencies=[],
            validators=[
                ContainsTextValidator('Security configuration non_existent_config not found')
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_security_configuration_missing_name',
            tool_name='manage_aws_glue_security_configurations',
            input_params={'operation': 'delete-security-configuration'},
            dependencies=[],
            validators=[ContainsTextValidator('Field required')],
            clean_ups=[],
        ),
    ]
