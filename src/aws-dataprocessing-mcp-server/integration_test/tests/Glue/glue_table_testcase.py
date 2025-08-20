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

from data_processing_mcp_server_tests.core.mcp_validators import (
    AWSBotoValidator,
    ContainsTextValidator,
)
from data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase
from typing import List


def glue_table_test_cases(s3_bucket, aws_clients) -> List[MCPTestCase]:
    """Glue table test cases."""
    return [
        MCPTestCase(
            test_name='create_table_basic',
            tool_name='manage_aws_glue_tables',
            input_params={
                'operation': 'create-table',
                'database_name': 'mcp_test_database',
                'table_name': 'mcp_test_table',
                'table_input': {
                    'Description': 'Test table',
                    'StorageDescriptor': {
                        'Columns': [{'Name': 'id', 'Type': 'int'}],
                        'Location': f's3://{s3_bucket}/',
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                            'Parameters': {'serialization.format': '1'},
                        },
                    },
                },
            },
            dependencies=['create_database_basic'],
            validators=[
                ContainsTextValidator('Successfully created table'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_table',
                    operation_input_params={
                        'database_name': 'mcp_test_database',
                        'table_name': 'mcp_test_table',
                    },
                    expected_keys=[
                        'Description',
                        'StorageDescriptor.Columns',
                        'StorageDescriptor.Location',
                        'StorageDescriptor.InputFormat',
                        'StorageDescriptor.OutputFormat',
                        'StorageDescriptor.SerdeInfo',
                    ],
                ),
            ],
            clean_ups=[],  # No clean up here, as the database will be deleted in the database test case
        ),
        MCPTestCase(
            test_name='get_table_basic',
            tool_name='manage_aws_glue_tables',
            input_params={
                'operation': 'get-table',
                'database_name': 'mcp_test_database',
                'table_name': 'mcp_test_table',
            },
            dependencies=['create_table_basic'],
            validators=[ContainsTextValidator('Successfully retrieved table')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='create_table_with_partition_keys',
            tool_name='manage_aws_glue_tables',
            input_params={
                'operation': 'create-table',
                'database_name': 'mcp_test_database',
                'table_name': 'mcp_test_table_with_partition_keys',
                'table_input': {
                    'Description': 'Test table',
                    'PartitionKeys': [{'Name': 'event_date', 'Type': 'string'}],
                    'StorageDescriptor': {
                        'Columns': [{'Name': 'id', 'Type': 'int'}],
                        'Location': f's3://{s3_bucket}/',
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                            'Parameters': {'serialization.format': '1'},
                        },
                    },
                },
            },
            dependencies=['create_database_basic'],
            validators=[
                ContainsTextValidator('Successfully created table'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_table',
                    operation_input_params={
                        'database_name': 'mcp_test_database',
                        'table_name': 'mcp_test_table_with_partition_keys',
                    },
                    expected_keys=['PartitionKeys'],
                ),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='create_table_with_description',
            tool_name='manage_aws_glue_tables',
            input_params={
                'operation': 'create-table',
                'database_name': 'mcp_test_database',
                'table_name': 'mcp_test_table_with_description',
                'table_input': {'Description': 'Test table with description'},
            },
            dependencies=['create_database_basic'],
            validators=[
                ContainsTextValidator('Successfully created table'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_table',
                    operation_input_params={
                        'database_name': 'mcp_test_database',
                        'table_name': 'mcp_test_table_with_description',
                    },
                    expected_keys=['Description'],
                ),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_table_not_exist',
            tool_name='manage_aws_glue_tables',
            input_params={
                'operation': 'get-table',
                'database_name': 'mcp_test_database',
                'table_name': 'non_existent_table',
            },
            dependencies=[],
            validators=[ContainsTextValidator('Entity Not Found')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='create_table_missing_database',
            tool_name='manage_aws_glue_tables',
            input_params={'operation': 'create-table', 'table_name': 'mcp_test_table_missing_db'},
            dependencies=[],
            validators=[ContainsTextValidator('Field required')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='create_table_missing_table_name',
            tool_name='manage_aws_glue_tables',
            input_params={'operation': 'create-table', 'database_name': 'mcp_test_database'},
            dependencies=['create_database_basic'],
            validators=[
                ContainsTextValidator(
                    'database_name, table_input and table_name are required for create-table operation'
                )
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='update_table_description',
            tool_name='manage_aws_glue_tables',
            input_params={
                'operation': 'update-table',
                'database_name': 'mcp_test_database',
                'table_name': 'mcp_test_table',
                'table_input': {'Description': 'Updated table description'},
            },
            dependencies=['create_table_basic'],
            validators=[
                ContainsTextValidator('Successfully updated table'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_table',
                    operation_input_params={
                        'database_name': 'mcp_test_database',
                        'table_name': 'mcp_test_table',
                    },
                    expected_keys=['Description'],
                ),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='update_table_not_exist',
            tool_name='manage_aws_glue_tables',
            input_params={
                'operation': 'update-table',
                'database_name': 'mcp_test_database',
                'table_name': 'non_existent_table',
                'table_input': {'Description': 'This table does not exist'},
            },
            dependencies=[],
            validators=[ContainsTextValidator('not found')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='update_table_missing_database',
            tool_name='manage_aws_glue_tables',
            input_params={'operation': 'update-table', 'table_name': 'mcp_test_table_missing_db'},
            dependencies=[],
            validators=[ContainsTextValidator('Field required')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='list_tables_in_db',
            tool_name='manage_aws_glue_tables',
            input_params={
                'operation': 'list-tables',
                'database_name': 'mcp_test_database',
                'max_results': 10,
            },
            dependencies=['create_table_basic', 'create_table_with_description'],
            validators=[
                ContainsTextValidator(
                    'Successfully listed 2 tables in database mcp_test_database', expected_count=2
                )
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_table_basic',
            tool_name='manage_aws_glue_tables',
            input_params={
                'operation': 'delete-table',
                'database_name': 'mcp_test_database',
                'table_name': 'mcp_test_table',
            },
            dependencies=['create_table_basic'],
            validators=[
                ContainsTextValidator('Successfully deleted table'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_table',
                    operation_input_params={
                        'database_name': 'mcp_test_database',
                        'table_name': 'mcp_test_table',
                    },
                    validate_absence=True,
                ),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_table_not_exist',
            tool_name='manage_aws_glue_tables',
            input_params={
                'operation': 'delete-table',
                'database_name': 'mcp_test_database',
                'table_name': 'non_existent_table',
            },
            dependencies=[],
            validators=[ContainsTextValidator('not found')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_table_missing_database',
            tool_name='manage_aws_glue_tables',
            input_params={'operation': 'delete-table', 'table_name': 'mcp_test_table_missing_db'},
            dependencies=[],
            validators=[ContainsTextValidator('Field required')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='search_table_for_databases',
            tool_name='manage_aws_glue_tables',
            input_params={
                'operation': 'search-tables',
                'database_name': 'mcp_test_database',
                'search_text': 'mcp_test_table_with_description',
            },
            dependencies=['create_table_with_description'],
            validators=[ContainsTextValidator('Search found', expected_count=1)],
            clean_ups=[],
        ),
    ]
