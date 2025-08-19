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


def glue_database_test_cases(s3_bucket, aws_clients) -> List[MCPTestCase]:
    """Glue database test cases."""
    return [
        MCPTestCase(
            test_name='create_database_basic',
            tool_name='manage_aws_glue_databases',
            input_params={
                'operation': 'create-database',
                'database_name': 'mcp_test_database',
                'description': 'Test database created by MCP server',
            },
            dependencies=[],
            validators=[
                ContainsTextValidator('Successfully created database'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_database',
                    operation_input_params={'Name': 'mcp_test_database'},
                    expected_keys=['description'],
                ),
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api='delete_database',
                    delete_params={'Name': 'mcp_test_database'},
                    boto_client=aws_clients['glue'],
                )
            ],
        ),
        MCPTestCase(
            test_name='get_database_basic',
            tool_name='manage_aws_glue_databases',
            input_params={'operation': 'get-database', 'database_name': 'mcp_test_database'},
            dependencies=['create_database_basic'],
            validators=[ContainsTextValidator('Successfully retrieved database')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='create_database_already_exists',
            tool_name='manage_aws_glue_databases',
            input_params={
                'operation': 'create-database',
                'database_name': 'mcp_test_database',
                'description': 'Test database created by MCP server',
            },
            dependencies=['create_database_basic'],
            validators=[ContainsTextValidator('already exists')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='create_database_missing_name',
            tool_name='manage_aws_glue_databases',
            input_params={
                'operation': 'create-database',
                'description': 'Test database created by MCP server',
            },
            dependencies=[],
            validators=[
                ContainsTextValidator('database_name is required for create-database operation')
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='create_database_with_definition',
            tool_name='manage_aws_glue_databases',
            input_params={
                'operation': 'create-database',
                'database_name': 'mcp_test_database_with_definition',
                'description': 'Test database with definition created by MCP server',
                'location_uri': f's3://{s3_bucket}/glue_database_location/mcp_test_database_with_definition/',
            },
            dependencies=[],
            validators=[
                ContainsTextValidator('Successfully created database'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_database',
                    operation_input_params={'Name': 'mcp_test_database_with_definition'},
                    expected_keys=['description', 'location_uri'],
                ),
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api='delete_database',
                    delete_params={'Name': 'mcp_test_database_with_definition'},
                    boto_client=aws_clients['glue'],
                )
            ],
        ),
        MCPTestCase(
            test_name='get_database_not_exist',
            tool_name='manage_aws_glue_databases',
            input_params={'operation': 'get-database', 'database_name': 'non_existent_database'},
            dependencies=[],
            validators=[ContainsTextValidator('not found')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_databases_all',
            tool_name='manage_aws_glue_databases',
            input_params={'operation': 'list-databases', 'max_results': 10},
            dependencies=['create_database_basic', 'create_database_with_definition'],
            validators=[ContainsTextValidator('Successfully listed', expected_count=2)],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='update_database_description',
            tool_name='manage_aws_glue_databases',
            input_params={
                'operation': 'update-database',
                'database_name': 'mcp_test_database',
                'description': 'Updated description for MCP test database',
            },
            dependencies=['create_database_basic'],
            validators=[
                ContainsTextValidator('Successfully updated database'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_database',
                    operation_input_params={'Name': 'mcp_test_database'},
                    expected_keys=['description'],
                ),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='update_database_missing_name',
            tool_name='manage_aws_glue_databases',
            input_params={
                'operation': 'update-database',
                'description': 'Updated description for MCP test database',
            },
            dependencies=[],
            validators=[
                ContainsTextValidator('database_name is required for update-database operation')
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='update_database_not_exist',
            tool_name='manage_aws_glue_databases',
            input_params={
                'operation': 'update-database',
                'database_name': 'non_existent_database',
                'description': 'Updated description for MCP test database',
            },
            dependencies=[],
            validators=[ContainsTextValidator('not found')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_database_basic',
            tool_name='manage_aws_glue_databases',
            input_params={'operation': 'delete-database', 'database_name': 'mcp_test_database'},
            dependencies=['create_database_basic'],
            validators=[
                ContainsTextValidator('Successfully deleted database'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_database',
                    operation_input_params={'Name': 'mcp_test_database'},
                    validate_absence=True,
                ),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_database_not_exist',
            tool_name='manage_aws_glue_databases',
            input_params={
                'operation': 'delete-database',
                'database_name': 'non_existent_database',
            },
            dependencies=[],
            validators=[ContainsTextValidator('not found')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_database_missing_name',
            tool_name='manage_aws_glue_databases',
            input_params={'operation': 'delete-database'},
            dependencies=[],
            validators=[
                ContainsTextValidator('database_name is required for delete-database operation')
            ],
            clean_ups=[],
        ),
    ]
