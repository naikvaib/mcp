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


def glue_sessions_test_cases(glue_role, aws_clients) -> List[MCPTestCase]:
    """Glue session test cases."""
    return [
        MCPTestCase(
            test_name='create_glue_session_basic',
            tool_name='manage_aws_glue_sessions',
            input_params={
                'operation': 'create-session',
                'session_id': 'mcp_test_glue_session',
                'description': 'Test session created by MCP',
                'role': glue_role,
                'command': {'Name': 'glueetl'},
            },
            dependencies=[],
            validators=[
                ContainsTextValidator('Successfully created session'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_session',
                    operation_input_params={'Id': 'mcp_test_glue_session'},
                    expected_keys=['description', 'command'],
                ),
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api='delete_session',
                    delete_params={'Id': 'mcp_test_glue_session'},
                    boto_client=aws_clients['glue'],
                )
            ],
        ),
        MCPTestCase(
            test_name='get_glue_session_basic',
            tool_name='manage_aws_glue_sessions',
            input_params={'operation': 'get-session', 'session_id': 'mcp_test_glue_session'},
            dependencies=['create_glue_session_basic'],
            validators=[ContainsTextValidator('Successfully retrieved session')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='create_glue_session_missing_id',
            tool_name='manage_aws_glue_sessions',
            input_params={
                'operation': 'create-session',
                'description': 'Test session created by MCP',
                'role': glue_role,
                'command': {'Name': 'glueetl'},
            },
            dependencies=[],
            validators=[
                ContainsTextValidator('role and command are required for create-session operation')
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='create_glue_session_missing_role',
            tool_name='manage_aws_glue_sessions',
            input_params={
                'operation': 'create-session',
                'session_id': 'mcp_test_glue_session',
                'description': 'Test session created by MCP',
                'command': {'Name': 'glueetl'},
            },
            dependencies=[],
            validators=[
                ContainsTextValidator('role and command are required for create-session operation')
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='list_glue_sessions',
            tool_name='manage_aws_glue_sessions',
            input_params={'operation': 'list-sessions', 'max_results': 10},
            dependencies=[],
            validators=[ContainsTextValidator('Successfully listed sessions')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_glue_session_not_exist',
            tool_name='manage_aws_glue_sessions',
            input_params={'operation': 'get-session', 'session_id': 'non_existent_session'},
            dependencies=[],
            validators=[ContainsTextValidator('not found')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='stop_glue_session_not_exist',
            tool_name='manage_aws_glue_sessions',
            input_params={'operation': 'stop-session', 'session_id': 'non_existent_session'},
            dependencies=[],
            validators=[ContainsTextValidator('not found')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='stop_glue_session_missing_id',
            tool_name='manage_aws_glue_sessions',
            input_params={'operation': 'stop-session'},
            dependencies=[],
            validators=[
                ContainsTextValidator('session_id is required for stop-session operation')
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_glue_session_not_exist',
            tool_name='manage_aws_glue_sessions',
            input_params={'operation': 'delete-session', 'session_id': 'non_existent_session'},
            dependencies=[],
            validators=[ContainsTextValidator('not found')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_glue_session_missing_id',
            tool_name='manage_aws_glue_sessions',
            input_params={'operation': 'delete-session'},
            dependencies=[],
            validators=[
                ContainsTextValidator('session_id is required for delete-session operation')
            ],
            clean_ups=[],
        ),
    ]
