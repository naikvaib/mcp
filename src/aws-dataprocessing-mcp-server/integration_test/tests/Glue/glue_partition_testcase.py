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


def glue_partitions_test_cases(s3_bucket, aws_clients) -> List[MCPTestCase]:
    """Glue partition test cases."""
    return [
        MCPTestCase(
            test_name='create_partition_basic',
            tool_name='manage_aws_glue_partitions',
            input_params={
                'operation': 'create-partition',
                'database_name': 'mcp_test_database',
                'table_name': 'mcp_test_table_with_partition_keys',
                'partition_values': ['2023-10-01'],
                'partition_input': {
                    'StorageDescriptor': {
                        'Columns': [{'Name': 'id', 'Type': 'int'}],
                        'Location': f's3://{s3_bucket}/partitions/2023-10-01/',
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                            'Parameters': {'serialization.format': '1'},
                        },
                    }
                },
            },
            dependencies=['create_table_with_partition_keys'],
            validators=[
                ContainsTextValidator('Successfully created partition'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_partition',
                    operation_input_params={
                        'DatabaseName': 'mcp_test_database',
                        'TableName': 'mcp_test_table_with_partition_keys',
                        'PartitionValues': ['2023-10-01'],
                    },
                    expected_keys=['Values', 'StorageDescriptor'],
                ),
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api='delete_partition',
                    delete_params={
                        'DatabaseName': 'mcp_test_database',
                        'TableName': 'mcp_test_table_with_partition_keys',
                        'PartitionValues': ['2023-10-01'],
                    },
                    boto_client=aws_clients['glue'],
                )
            ],
        ),
        MCPTestCase(
            test_name='get_partition_basic',
            tool_name='manage_aws_glue_partitions',
            input_params={
                'operation': 'get-partition',
                'database_name': 'mcp_test_database',
                'table_name': 'mcp_test_table_with_partition_keys',
                'partition_values': ['2023-10-01'],
            },
            dependencies=['create_partition_basic'],
            validators=[ContainsTextValidator('Successfully retrieved partition')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='create_partition_missing_database',
            tool_name='manage_aws_glue_partitions',
            input_params={
                'operation': 'create-partition',
                'table_name': 'mcp_test_table_with_partition_keys',
                'partition_values': ['2023-10-01'],
                'partition_input': {
                    'StorageDescriptor': {
                        'Columns': [{'Name': 'id', 'Type': 'int'}],
                        'Location': f's3://{s3_bucket}/partitions/2023-10-01/',
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                            'Parameters': {'serialization.format': '1'},
                        },
                    }
                },
            },
            dependencies=[],
            validators=[ContainsTextValidator('Field required ')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='create_partition_missing_table',
            tool_name='manage_aws_glue_partitions',
            input_params={
                'operation': 'create-partition',
                'database_name': 'mcp_test_database',
                'partition_values': ['2023-10-01'],
                'partition_input': {
                    'StorageDescriptor': {
                        'Columns': [{'Name': 'id', 'Type': 'int'}],
                        'Location': f's3://{s3_bucket}/partitions/2023-10-01/',
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                            'Parameters': {'serialization.format': '1'},
                        },
                    }
                },
            },
            dependencies=[],
            validators=[ContainsTextValidator('Field required ')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='create_partition_missing_values',
            tool_name='manage_aws_glue_partitions',
            input_params={
                'operation': 'create-partition',
                'database_name': 'mcp_test_database',
                'table_name': 'mcp_test_table_with_partition_keys',
                'partition_input': {
                    'StorageDescriptor': {
                        'Columns': [{'Name': 'id', 'Type': 'int'}],
                        'Location': f's3://{s3_bucket}/partitions/2023-10-01/',
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                            'Parameters': {'serialization.format': '1'},
                        },
                    }
                },
            },
            dependencies=[],
            validators=[
                ContainsTextValidator(
                    'partition_values and partition_input are required for create-partition operation'
                )
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_partition_not_exist',
            tool_name='manage_aws_glue_partitions',
            input_params={
                'operation': 'get-partition',
                'database_name': 'mcp_test_database',
                'table_name': 'mcp_test_table_with_partition_keys',
                'partition_values': ['2023-10-02'],
            },
            dependencies=[],
            validators=[ContainsTextValidator('not found')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='list_partitions_basic',
            tool_name='manage_aws_glue_partitions',
            input_params={
                'operation': 'list-partitions',
                'database_name': 'mcp_test_database',
                'table_name': 'mcp_test_table_with_partition_keys',
                'max_results': 10,
            },
            dependencies=['create_partition_basic'],
            validators=[ContainsTextValidator('Successfully listed', expected_count=1)],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='update_partition_description',
            tool_name='manage_aws_glue_partitions',
            input_params={
                'operation': 'update-partition',
                'database_name': 'mcp_test_database',
                'table_name': 'mcp_test_table_with_partition_keys',
                'partition_values': ['2023-10-01'],
                'partition_input': {
                    'StorageDescriptor': {
                        'Columns': [{'Name': 'id', 'Type': 'int'}],
                        'Location': f's3://{s3_bucket}/partitions/2023-10-01/',
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                            'Parameters': {'serialization.format': '1'},
                        },
                    }
                },
            },
            dependencies=['create_partition_basic'],
            validators=[
                ContainsTextValidator('Successfully updated partition'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_partition',
                    operation_input_params={
                        'DatabaseName': 'mcp_test_database',
                        'TableName': 'mcp_test_table_with_partition_keys',
                        'PartitionValues': ['2023-10-01'],
                    },
                    expected_keys=['Values', 'StorageDescriptor'],
                ),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='update_partition_missing_database',
            tool_name='manage_aws_glue_partitions',
            input_params={
                'operation': 'update-partition',
                'table_name': 'mcp_test_table_with_partition_keys',
                'partition_values': ['2023-10-01'],
                'partition_input': {
                    'StorageDescriptor': {
                        'Columns': [{'Name': 'id', 'Type': 'int'}],
                        'Location': f's3://{s3_bucket}/partitions/2023-10-01/',
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                            'Parameters': {'serialization.format': '1'},
                        },
                    }
                },
            },
            dependencies=[],
            validators=[ContainsTextValidator('Field required ')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='update_partition_missing_table',
            tool_name='manage_aws_glue_partitions',
            input_params={
                'operation': 'update-partition',
                'database_name': 'mcp_test_database',
                'partition_values': ['2023-10-01'],
                'partition_input': {
                    'StorageDescriptor': {
                        'Columns': [{'Name': 'id', 'Type': 'int'}],
                        'Location': f's3://{s3_bucket}/partitions/2023-10-01/',
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                            'Parameters': {'serialization.format': '1'},
                        },
                    }
                },
            },
            dependencies=[],
            validators=[ContainsTextValidator('Field required ')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='update_partition_missing_values',
            tool_name='manage_aws_glue_partitions',
            input_params={
                'operation': 'update-partition',
                'database_name': 'mcp_test_database',
                'table_name': 'mcp_test_table_with_partition_keys',
                'partition_input': {
                    'StorageDescriptor': {
                        'Columns': [{'Name': 'id', 'Type': 'int'}],
                        'Location': f's3://{s3_bucket}/partitions/2023-10-01/',
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                            'Parameters': {'serialization.format': '1'},
                        },
                    }
                },
            },
            dependencies=[],
            validators=[
                ContainsTextValidator(
                    'partition_values and partition_input are required for update-partition operation'
                )
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_partition_basic',
            tool_name='manage_aws_glue_partitions',
            input_params={
                'operation': 'delete-partition',
                'database_name': 'mcp_test_database',
                'table_name': 'mcp_test_table_with_partition_keys',
                'partition_values': ['2023-10-01'],
            },
            dependencies=['create_partition_basic'],
            validators=[
                ContainsTextValidator('Successfully deleted partition'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_partition',
                    operation_input_params={
                        'DatabaseName': 'mcp_test_database',
                        'TableName': 'mcp_test_table_with_partition_keys',
                        'PartitionValues': ['2023-10-01'],
                    },
                    validate_absence=True,
                ),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_partition_not_exist',
            tool_name='manage_aws_glue_partitions',
            input_params={
                'operation': 'delete-partition',
                'database_name': 'mcp_test_database',
                'table_name': 'mcp_test_table_with_partition_keys',
                'partition_values': ['2023-10-02'],
            },
            dependencies=[],
            validators=[ContainsTextValidator('not found')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_partition_missing_database',
            tool_name='manage_aws_glue_partitions',
            input_params={
                'operation': 'delete-partition',
                'table_name': 'mcp_test_table_with_partition_keys',
                'partition_values': ['2023-10-01'],
            },
            dependencies=[],
            validators=[ContainsTextValidator('Field required ')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_partition_missing_table',
            tool_name='manage_aws_glue_partitions',
            input_params={
                'operation': 'delete-partition',
                'database_name': 'mcp_test_database',
                'partition_values': ['2023-10-01'],
            },
            dependencies=[],
            validators=[ContainsTextValidator('Field required ')],
            clean_ups=[],
        ),
    ]
