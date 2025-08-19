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


def athena_databse_table_test_cases(s3_bucket, aws_clients) -> List[MCPTestCase]:
    """Athena database table test cases."""
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
                    expected_keys=['Description'],
                ),
            ],
            clean_ups=[],  # No clean up here, as the database will be deleted in the database test case
        ),
        MCPTestCase(
            test_name='get_athena_database',
            tool_name='manage_aws_athena_databases_and_tables',
            input_params={
                'operation': 'get-database',
                'database_name': 'mcp_test_database',
                'catalog_name': 'AwsDataCatalog',
            },
            dependencies=['create_database_basic'],
            validators=[ContainsTextValidator('Successfully retrieved database')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_athena_database_missing_name',
            tool_name='manage_aws_athena_databases_and_tables',
            input_params={'operation': 'get-database'},
            dependencies=[],
            validators=[ContainsTextValidator('Field required')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_table_metadata',
            tool_name='manage_aws_athena_databases_and_tables',
            input_params={
                'operation': 'get-table-metadata',
                'database_name': 'mcp_test_database',
                'table_name': 'mcp_test_table',
                'catalog_name': 'AwsDataCatalog',
            },
            dependencies=['create_table_basic'],
            validators=[ContainsTextValidator('Successfully retrieved metadata')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_table_metadata_missing_params',
            tool_name='manage_aws_athena_databases_and_tables',
            input_params={'operation': 'get-table-metadata'},
            dependencies=[],
            validators=[ContainsTextValidator('Field required')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='list_athena_databases',
            tool_name='manage_aws_athena_databases_and_tables',
            input_params={
                'operation': 'list-databases',
                'catalog_name': 'AwsDataCatalog',
                'max_results': 10,
            },
            dependencies=[],
            validators=[ContainsTextValidator('Successfully listed databases')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='list_athena_table_metadata',
            tool_name='manage_aws_athena_databases_and_tables',
            input_params={
                'operation': 'list-table-metadata',
                'database_name': 'mcp_test_database',
                'max_results': 10,
                'catalog_name': 'AwsDataCatalog',
            },
            dependencies=['create_database_basic'],
            validators=[ContainsTextValidator('Successfully listed table metadata')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='list_athena_table_metadata_missing_params',
            tool_name='manage_aws_athena_databases_and_tables',
            input_params={'operation': 'list-table-metadata'},
            dependencies=[],
            validators=[ContainsTextValidator('Field required')],
            clean_ups=[],
        ),
    ]
