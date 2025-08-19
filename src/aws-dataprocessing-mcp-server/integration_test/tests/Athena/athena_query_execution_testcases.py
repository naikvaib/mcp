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
    ContainsTextValidator,
)
from data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase
from typing import List


def athena_query_execution_test_cases(s3_bucket, aws_clients) -> List[MCPTestCase]:
    """Athena query execution test cases."""
    return [
        MCPTestCase(
            test_name='start_athena_query_execution',
            tool_name='manage_aws_athena_query_executions',
            input_params={
                'operation': 'start-query-execution',
                'query_string': 'SELECT 1',
                'work_group': 'primary',
                'result_configuration': {'OutputLocation': f's3://{s3_bucket}/athena-results/'},
            },
            dependencies=['create_athena_query'],
            validators=[ContainsTextValidator('Successfully started query execution')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='batch_get_athena_query_execution',
            tool_name='manage_aws_athena_query_executions',
            input_params={
                'operation': 'batch-get-query-execution',
                'query_execution_ids': [
                    '{{start_athena_query_execution.result.content[0].text.query_execution_id}}'
                ],
            },
            dependencies=['start_athena_query_execution'],
            validators=[ContainsTextValidator('Successfully retrieved query executions')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_athena_query_execution',
            tool_name='manage_aws_athena_query_executions',
            input_params={
                'operation': 'get-query-execution',
                'query_execution_id': '{{start_athena_query_execution.result.content[0].text.query_execution_id}}',
            },
            dependencies=['start_athena_query_execution'],
            validators=[ContainsTextValidator('Successfully retrieved query execution')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_athena_query_execution_missing_id',
            tool_name='manage_aws_athena_query_executions',
            input_params={'operation': 'get-query-execution'},
            dependencies=[],
            validators=[
                ContainsTextValidator(
                    'query_execution_id is required for get-query-execution operation'
                )
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_athena_query_results',
            tool_name='manage_aws_athena_query_executions',
            input_params={
                'operation': 'get-query-results',
                'query_execution_id': '{{start_athena_query_execution.result.content[0].text.query_execution_id}}',
            },
            dependencies=['start_athena_query_execution'],
            validators=[ContainsTextValidator('Query has not yet finished.')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_athena_query_results_missing_id',
            tool_name='manage_aws_athena_query_executions',
            input_params={
                'operation': 'get-query-results',
            },
            dependencies=[],
            validators=[
                ContainsTextValidator(
                    'query_execution_id is required for get-query-results operation'
                )
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_athena_query_runtime_statistics',
            tool_name='manage_aws_athena_query_executions',
            input_params={
                'operation': 'get-query-runtime-statistics',
                'query_execution_id': '{{start_athena_query_execution.result.content[0].text.query_execution_id}}',
            },
            dependencies=['start_athena_query_execution'],
            validators=[ContainsTextValidator('Query has not yet finished.')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_athena_query_runtime_statistics_missing_id',
            tool_name='manage_aws_athena_query_executions',
            input_params={'operation': 'get-query-runtime-statistics'},
            dependencies=[],
            validators=[
                ContainsTextValidator(
                    'query_execution_id is required for get-query-runtime-statistics operation'
                )
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='list_athena_query_executions',
            tool_name='manage_aws_athena_query_executions',
            input_params={'operation': 'list-query-executions', 'max_results': 10},
            dependencies=['start_athena_query_execution'],
            validators=[ContainsTextValidator('Successfully listed query executions')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='stop_athena_query_execution',
            tool_name='manage_aws_athena_query_executions',
            input_params={
                'operation': 'stop-query-execution',
                'query_execution_id': '{{start_athena_query_execution.result.content[0].text.query_execution_id}}',
            },
            dependencies=['start_athena_query_execution'],
            validators=[ContainsTextValidator('Successfully stopped query execution')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='stop_athena_query_execution_missing_id',
            tool_name='manage_aws_athena_query_executions',
            input_params={'operation': 'stop-query-execution'},
            dependencies=[],
            validators=[
                ContainsTextValidator(
                    'query_execution_id is required for stop-query-execution operation'
                )
            ],
            clean_ups=[],
        ),
    ]
