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


def glue_crawlers_test_cases(s3_bucket, glue_role, aws_clients) -> List[MCPTestCase]:
    """Glue crawler test cases."""
    return [
        MCPTestCase(
            test_name='create_glue_crawler_basic',
            tool_name='manage_aws_glue_crawlers',
            input_params={
                'operation': 'create-crawler',
                'crawler_name': 'mcp_test_crawler',
                'crawler_definition': {
                    'Role': glue_role,
                    'DatabaseName': 'mcp_test_database',
                    'Targets': {'S3Targets': [{'Path': f's3://{s3_bucket}/crawlers/'}]},
                    'TablePrefix': 'mcp_',
                    'Description': 'Test crawler created by MCP',
                },
            },
            dependencies=[],
            validators=[
                ContainsTextValidator('Successfully created Glue crawler'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_crawler',
                    operation_input_params={'Name': 'mcp_test_crawler'},
                    expected_keys=['crawler_name', 'crawler_definition.DatabaseName'],
                ),
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api='delete_crawler',
                    delete_params={'Name': 'mcp_test_crawler'},
                    boto_client=aws_clients['glue'],
                )
            ],
        ),
        MCPTestCase(
            test_name='get_glue_crawler_basic',
            tool_name='manage_aws_glue_crawlers',
            input_params={'operation': 'get-crawler', 'crawler_name': 'mcp_test_crawler'},
            dependencies=['create_glue_crawler_basic'],
            validators=[ContainsTextValidator('Successfully retrieved crawler')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='create_glue_crawler_missing_name',
            tool_name='manage_aws_glue_crawlers',
            input_params={
                'operation': 'create-crawler',
                'crawler_definition': {
                    'Role': glue_role,
                    'DatabaseName': 'mcp_test_database',
                    'Targets': {'S3Targets': [{'Path': f's3://{s3_bucket}/crawlers/'}]},
                    'TablePrefix': 'mcp_',
                    'Description': 'Test crawler created by MCP',
                },
            },
            dependencies=[],
            validators=[
                ContainsTextValidator(
                    'crawler_name and crawler_definition are required for create-crawler operation'
                )
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='create_glue_crawler_second',
            tool_name='manage_aws_glue_crawlers',
            input_params={
                'operation': 'create-crawler',
                'crawler_name': 'mcp_test_crawler_second',
                'crawler_definition': {
                    'Role': glue_role,
                    'DatabaseName': 'mcp_test_database',
                    'Targets': {'S3Targets': [{'Path': f's3://{s3_bucket}/crawlers/'}]},
                    'TablePrefix': 'mcp_',
                    'Description': 'Test crawler created by MCP',
                },
            },
            dependencies=[],
            validators=[
                ContainsTextValidator('Successfully created Glue crawler'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_crawler',
                    operation_input_params={'Name': 'mcp_test_crawler_second'},
                    expected_keys=['crawler_name', 'crawler_definition.DatabaseName'],
                ),
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api='delete_crawler',
                    delete_params={'Name': 'mcp_test_crawler_second'},
                    boto_client=aws_clients['glue'],
                )
            ],
        ),
        MCPTestCase(
            test_name='get_glue_crawlers',
            tool_name='manage_aws_glue_crawlers',
            input_params={'operation': 'get-crawlers', 'max_results': 10},
            dependencies=[],
            validators=[
                ContainsTextValidator('Successfully retrieved'),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='batch_get_glue_crawlers',
            tool_name='manage_aws_glue_crawlers',
            input_params={
                'operation': 'batch-get-crawlers',
                'crawler_names': ['mcp_test_crawler', 'mcp_test_crawler_second'],
            },
            dependencies=['create_glue_crawler_basic', 'create_glue_crawler_second'],
            validators=[
                ContainsTextValidator('Successfully retrieved'),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='start_glue_crawler',
            tool_name='manage_aws_glue_crawlers',
            input_params={'operation': 'start-crawler', 'crawler_name': 'mcp_test_crawler'},
            dependencies=['create_glue_crawler_basic'],
            validators=[
                ContainsTextValidator('Successfully started crawler'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_crawler',
                    operation_input_params={'Name': 'mcp_test_crawler'},
                    expected_keys=['crawler_name'],
                ),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='start_glue_crawler_missing_name',
            tool_name='manage_aws_glue_crawlers',
            input_params={'operation': 'start-crawler'},
            dependencies=[],
            validators=[
                ContainsTextValidator('crawler_name is required for start-crawler operation')
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='stop_glue_crawler',
            tool_name='manage_aws_glue_crawlers',
            input_params={'operation': 'stop-crawler', 'crawler_name': 'mcp_test_crawler'},
            dependencies=['create_glue_crawler_basic'],
            validators=[
                ContainsTextValidator('Successfully stopped crawler'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_crawler',
                    operation_input_params={'Name': 'mcp_test_crawler'},
                    expected_keys=['crawler_name'],
                ),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='stop_glue_crawler_missing_name',
            tool_name='manage_aws_glue_crawlers',
            input_params={'operation': 'stop-crawler'},
            dependencies=[],
            validators=[
                ContainsTextValidator('crawler_name is required for stop-crawler operation')
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_glue_crawler',
            tool_name='manage_aws_glue_crawlers',
            input_params={
                'operation': 'delete-crawler',
                'crawler_name': 'mcp_test_crawler_second',
            },
            dependencies=['create_glue_crawler_second'],
            validators=[
                ContainsTextValidator('Successfully deleted MCP-managed Glue crawler'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_crawler',
                    operation_input_params={'Name': 'mcp_test_crawler_second'},
                    validate_absence=True,
                ),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_glue_crawler_not_exist',
            tool_name='manage_aws_glue_crawlers',
            input_params={'operation': 'delete-crawler', 'crawler_name': 'non_existent_crawler'},
            dependencies=[],
            validators=[ContainsTextValidator('Cannot delete crawler non_existent_crawler')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_glue_crawler_missing_name',
            tool_name='manage_aws_glue_crawlers',
            input_params={'operation': 'delete-crawler'},
            dependencies=[],
            validators=[
                ContainsTextValidator('crawler_name is required for delete-crawler operation')
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='update_glue_crawler',
            tool_name='manage_aws_glue_crawlers',
            input_params={
                'operation': 'update-crawler',
                'crawler_name': 'mcp_test_crawler',
                'crawler_definition': {
                    'Role': glue_role,
                    'DatabaseName': 'mcp_test_database_updated',
                    'Targets': {'S3Targets': [{'Path': f's3://{s3_bucket}/crawlers-updated/'}]},
                    'TablePrefix': 'mcp_updated_',
                    'Description': 'Updated test crawler created by MCP',
                },
            },
            dependencies=['create_glue_crawler_basic'],
            validators=[
                ContainsTextValidator('Successfully updated crawler'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_crawler',
                    operation_input_params={'Name': 'mcp_test_crawler'},
                    expected_keys=[
                        'crawler_name',
                        'crawler_definition.DatabaseName',
                        'crawler_definition.Targets',
                    ],
                ),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='update_glue_crawler_missing_name',
            tool_name='manage_aws_glue_crawlers',
            input_params={
                'operation': 'update-crawler',
                'crawler_definition': {
                    'Role': glue_role,
                    'DatabaseName': 'mcp_test_database_updated',
                    'Targets': {'S3Targets': [{'Path': f's3://{s3_bucket}/crawlers-updated/'}]},
                    'TablePrefix': 'mcp_updated_',
                    'Description': 'Updated test crawler created by MCP',
                },
            },
            dependencies=[],
            validators=[
                ContainsTextValidator(
                    'crawler_name and crawler_definition are required for update-crawler operation'
                )
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='list_glue_crawlers',
            tool_name='manage_aws_glue_crawlers',
            input_params={'operation': 'list-crawlers', 'max_results': 10},
            dependencies=[],
            validators=[ContainsTextValidator('Successfully listed crawlers')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='create_crawler_with_schedule',
            tool_name='manage_aws_glue_crawlers',
            input_params={
                'operation': 'create-crawler',
                'crawler_name': 'mcp_test_crawler_with_schedule',
                'crawler_definition': {
                    'Role': glue_role,
                    'DatabaseName': 'mcp_test_database',
                    'Targets': {'S3Targets': [{'Path': f's3://{s3_bucket}/crawlers/'}]},
                    'TablePrefix': 'mcp_',
                    'Description': 'Test crawler with schedule created by MCP',
                    'Schedule': 'cron(0 12 * * ? *)',
                },
            },
            dependencies=[],
            validators=[
                ContainsTextValidator('Successfully created Glue crawler'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_crawler',
                    operation_input_params={'Name': 'mcp_test_crawler_with_schedule'},
                    expected_keys=[
                        'crawler_name',
                        'crawler_definition.DatabaseName',
                        'crawler_definition.Schedule',
                    ],
                ),
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api='delete_crawler',
                    delete_params={'Name': 'mcp_test_crawler_with_schedule'},
                    boto_client=aws_clients['glue'],
                )
            ],
        ),
    ]
