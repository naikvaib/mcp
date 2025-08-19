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


def athena_workgroup_test_cases(s3_bucket, aws_clients) -> List[MCPTestCase]:
    """Athena workgroup test cases."""
    return [
        MCPTestCase(
            test_name='create_athena_workgroup',
            tool_name='manage_aws_athena_workgroups',
            input_params={
                'operation': 'create-work-group',
                'name': 'mcp_test_workgroup',
                'description': 'This is a test workgroup for MCP validation.',
                'configuration': {
                    'ResultConfiguration': {'OutputLocation': f's3://{s3_bucket}/athena-results/'}
                },
            },
            dependencies=[],
            validators=[
                ContainsTextValidator('Successfully created Athena workgroup'),
                AWSBotoValidator(
                    boto_client=aws_clients['athena'],
                    operation='get_work_group',
                    operation_input_params={'WorkGroup': 'mcp_test_workgroup'},
                    expected_keys=['Name', 'Description'],
                ),
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api='delete_work_group',
                    boto_client=aws_clients['athena'],
                    delete_params={'WorkGroup': 'mcp_test_workgroup'},
                )
            ],
        ),
        MCPTestCase(
            test_name='get_athena_workgroup',
            tool_name='manage_aws_athena_workgroups',
            input_params={'operation': 'get-work-group', 'name': 'mcp_test_workgroup'},
            dependencies=['create_athena_workgroup'],
            validators=[ContainsTextValidator('Successfully retrieved workgroup')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_athena_workgroup_missing_name',
            tool_name='manage_aws_athena_workgroups',
            input_params={'operation': 'get-work-group'},
            dependencies=[],
            validators=[ContainsTextValidator('name is required for get-work-group operation')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='list_athena_workgroups',
            tool_name='manage_aws_athena_workgroups',
            input_params={'operation': 'list-work-groups', 'max_results': 10},
            dependencies=['create_athena_workgroup'],
            validators=[ContainsTextValidator('Successfully listed workgroups')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='update_athena_workgroup',
            tool_name='manage_aws_athena_workgroups',
            input_params={
                'operation': 'update-work-group',
                'name': 'mcp_test_workgroup',
                'description': 'This is an updated test workgroup for MCP validation.',
                'configuration': {
                    'ResultConfigurationUpdates': {
                        'OutputLocation': f's3://{s3_bucket}/athena-results/'
                    }
                },
            },
            dependencies=['create_athena_workgroup'],
            validators=[
                ContainsTextValidator('Successfully updated workgroup'),
                AWSBotoValidator(
                    boto_client=aws_clients['athena'],
                    operation='get_work_group',
                    operation_input_params={'WorkGroup': 'mcp_test_workgroup'},
                    expected_keys=['Name', 'Description'],
                ),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='update_athena_workgroup_missing_name',
            tool_name='manage_aws_athena_workgroups',
            input_params={
                'operation': 'update-work-group',
                'description': 'This is an updated test workgroup for MCP validation.',
                'configuration': {
                    'ResultConfigurationUpdates': {
                        'OutputLocation': f's3://{s3_bucket}/athena-results/'
                    }
                },
            },
            dependencies=[],
            validators=[ContainsTextValidator('name is required for update-work-group operation')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_athena_workgroup',
            tool_name='manage_aws_athena_workgroups',
            input_params={'operation': 'delete-work-group', 'name': 'mcp_test_workgroup'},
            dependencies=['create_athena_workgroup'],
            validators=[
                ContainsTextValidator('Successfully deleted MCP-managed Athena workgroup')
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_athena_workgroup_missing_name',
            tool_name='manage_aws_athena_workgroups',
            input_params={'operation': 'delete-work-group'},
            dependencies=[],
            validators=[ContainsTextValidator('name is required for delete-work-group operation')],
            clean_ups=[],
        ),
    ]
