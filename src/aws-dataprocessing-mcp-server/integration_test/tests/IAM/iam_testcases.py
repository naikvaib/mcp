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


def iam_test_cases(glue_role, aws_clients) -> List[MCPTestCase]:
    """IAM test cases."""
    return [
        MCPTestCase(
            test_name='add_inline_policy_to_role',
            tool_name='add_inline_policy',
            input_params={
                'policy_name': 'mcp-test-inline-policy',
                'role_name': 'mcp-test-glue-role',
                'permissions': {
                    'Effect': 'Allow',
                    'Action': [
                        'glue:*',
                        's3:GetObject',
                        's3:PutObject',
                        's3:DeleteObject',
                        's3:ListBucket',
                        'iam:PassRole',
                    ],
                    'Resource': '*',
                },
            },
            dependencies=[],
            validators=[
                ContainsTextValidator('mcp-test-inline-policy'),
                AWSBotoValidator(
                    boto_client=aws_clients['iam'],
                    operation='list_role_policies',
                    operation_input_params={'RoleName': 'mcp-test-glue-role'},
                    expected_keys=['policy_name'],
                ),
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api='delete_role_policy',
                    delete_params={
                        'RoleName': 'mcp-test-glue-role',
                        'PolicyName': 'mcp-test-inline-policy',
                    },
                    boto_client=aws_clients['iam'],
                )
            ],
        ),
        MCPTestCase(
            test_name='add_inline_policy_missing_role',
            tool_name='add_inline_policy',
            input_params={
                'policy_name': 'mcp-test-inline-policy',
                'permissions': {'Effect': 'Allow', 'Action': ['glue:*'], 'Resource': '*'},
            },
            dependencies=[],
            validators=[ContainsTextValidator('Field required')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_policies_for_role',
            tool_name='get_policies_for_role',
            input_params={'role_name': 'mcp-test-glue-role'},
            dependencies=['add_inline_policy_to_role'],
            validators=[ContainsTextValidator('Successfully retrieved details for IAM role')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_policies_for_role_missing_role',
            tool_name='get_policies_for_role',
            input_params={},
            dependencies=[],
            validators=[ContainsTextValidator('Field required')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='create_data_processing_role',
            tool_name='create_data_processing_role',
            input_params={
                'role_name': 'mcp-test-data-processing-role',
                'description': 'Role for MCP data processing tests',
                'service_type': 'glue',
            },
            dependencies=[],
            validators=[
                ContainsTextValidator('Successfully created IAM role'),
                AWSBotoValidator(
                    boto_client=aws_clients['iam'],
                    operation='get_role',
                    operation_input_params={'RoleName': 'mcp-test-data-processing-role'},
                    expected_keys=['role_name', 'Description'],
                ),
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api='delete_role',
                    delete_params={'RoleName': 'mcp-test-data-processing-role'},
                    boto_client=aws_clients['iam'],
                )
            ],
        ),
        MCPTestCase(
            test_name='create_data_processing_role_missing_name',
            tool_name='create_data_processing_role',
            input_params={
                'description': 'Role for MCP data processing tests',
                'service_type': 'glue',
            },
            dependencies=[],
            validators=[ContainsTextValidator('Field required')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_roles_for_service',
            tool_name='get_roles_for_service',
            input_params={'service_type': 'glue'},
            dependencies=['create_data_processing_role'],
            validators=[ContainsTextValidator('Successfully retrieved')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_roles_for_service_missing_type',
            tool_name='get_roles_for_service',
            input_params={},
            dependencies=[],
            validators=[ContainsTextValidator('Field required')],
            clean_ups=[],
        ),
    ]
