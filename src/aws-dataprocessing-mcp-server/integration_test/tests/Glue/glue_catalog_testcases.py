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
    ContainsTextValidator,
)
from data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase
from typing import List


def glue_catalogs_test_cases(aws_clients) -> List[MCPTestCase]:
    """Glue catalog test cases."""
    return [
        MCPTestCase(
            test_name='create_glue_catalog_basic',
            tool_name='manage_aws_glue_catalog',
            input_params={
                'operation': 'create-catalog',
                'catalog_id': 'mcp-test-catalog',
                'catalog_input': {
                    'Description': 'Test catalog created by MCP',
                    'Parameters': {'env': 'test', 'team': 'dataprocessing'},
                },
            },
            dependencies=[],
            validators=[
                ContainsTextValidator('Successfully created catalog'),
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api='delete_data_catalog',
                    delete_params={'Name': 'mcp-test-catalog'},
                    boto_client=aws_clients['glue'],
                )
            ],
        ),
        MCPTestCase(
            test_name='list_glue_catalogs',
            tool_name='manage_aws_glue_catalog',
            input_params={'operation': 'list-catalogs', 'max_results': 10},
            dependencies=[],
            validators=[ContainsTextValidator('Successfully listed')],
            clean_ups=[],
        ),
    ]
