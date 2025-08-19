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


def athena_data_catalog_test_cases(aws_clients) -> List[MCPTestCase]:
    """Athena data catalog test cases."""
    return [
        MCPTestCase(
            test_name='create_athena_data_catalog',
            tool_name='manage_aws_athena_data_catalogs',
            input_params={
                'operation': 'create-data-catalog',
                'name': 'mcp_test_data_catalog',
                'description': 'This is a test data catalog for MCP validation.',
                'type': 'LAMBDA',
                'parameters': {
                    'metadata-function': 'arn:aws:lambda:us-west-2:123456789012:function:mcp_test_lambda_function',
                    'record-function': 'arn:aws:lambda:us-west-2:123456789012:function:mcp_test_lambda_function',
                },
            },
            dependencies=[],
            validators=[
                ContainsTextValidator('Successfully created data catalog'),
                AWSBotoValidator(
                    boto_client=aws_clients['athena'],
                    operation='get_data_catalog',
                    operation_input_params={'Name': 'mcp_test_data_catalog'},
                    expected_keys=['Name', 'Description'],
                ),
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api='delete_data_catalog',
                    boto_client=aws_clients['athena'],
                    resource_field='name',
                    target_param_key='Name',
                    param_is_list=False,
                )
            ],
        ),
        MCPTestCase(
            test_name='get_athena_data_catalog',
            tool_name='manage_aws_athena_data_catalogs',
            input_params={
                'operation': 'get-data-catalog',
                'name': 'mcp_test_data_catalog',
                'type': 'LAMBDA',
            },
            dependencies=['create_athena_data_catalog'],
            validators=[ContainsTextValidator('Successfully retrieved data catalog')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_athena_data_catalog_missing_name',
            tool_name='manage_aws_athena_data_catalogs',
            input_params={'operation': 'get-data-catalog'},
            dependencies=[],
            validators=[ContainsTextValidator('name is required for get-data-catalog operation')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='list_athena_data_catalogs',
            tool_name='manage_aws_athena_data_catalogs',
            input_params={'operation': 'list-data-catalogs', 'max_results': 10},
            dependencies=['create_athena_data_catalog'],
            validators=[
                ContainsTextValidator(
                    'Successfully listed data catalogs', expected_count=2
                )  # 1 glue catalog, 1 lambda catalog
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='update_athena_data_catalog',
            tool_name='manage_aws_athena_data_catalogs',
            input_params={
                'operation': 'update-data-catalog',
                'name': 'mcp_test_data_catalog',
                'type': 'LAMBDA',
                'description': 'This is an updated test data catalog for MCP validation.',
                'parameters': {
                    'metadata-function': 'arn:aws:lambda:us-west-2:123456789012:function:mcp_test_lambda_function_updated',
                    'record-function': 'arn:aws:lambda:us-west-2:123456789012:function:mcp_test_lambda_function_updated',
                },
            },
            dependencies=['create_athena_data_catalog'],
            validators=[
                ContainsTextValidator('Successfully updated data catalog'),
                AWSBotoValidator(
                    boto_client=aws_clients['athena'],
                    operation='get_data_catalog',
                    operation_input_params={'Name': 'mcp_test_data_catalog'},
                    expected_keys=['Name', 'Description'],
                ),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='update_athena_data_catalog_missing_name',
            tool_name='manage_aws_athena_data_catalogs',
            input_params={
                'operation': 'update-data-catalog',
                'description': 'This is an updated test data catalog for MCP validation.',
                'parameters': {
                    'metadata-function': 'arn:aws:lambda:us-west-2:123456789012:function:mcp_test_lambda_function_updated',
                    'record-function': 'arn:aws:lambda:us-west-2:123456789012:function:mcp_test_lambda_function_updated',
                },
            },
            dependencies=[],
            validators=[
                ContainsTextValidator('name is required for update-data-catalog operation')
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_athena_data_catalog',
            tool_name='manage_aws_athena_data_catalogs',
            input_params={'operation': 'delete-data-catalog', 'name': 'mcp_test_data_catalog'},
            dependencies=['create_athena_data_catalog'],
            validators=[ContainsTextValidator('Successfully deleted data catalog')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_athena_data_catalog_missing_name',
            tool_name='manage_aws_athena_data_catalogs',
            input_params={'operation': 'delete-data-catalog'},
            dependencies=[],
            validators=[
                ContainsTextValidator('name is required for delete-data-catalog operation')
            ],
            clean_ups=[],
        ),
    ]
