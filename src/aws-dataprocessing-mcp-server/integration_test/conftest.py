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

import os
import pytest
from data_processing_mcp_server_tests.core.mcp_client import MCPClient
from data_processing_mcp_server_tests.core.mcp_server import MCPServerManager


@pytest.fixture(scope='session')
def mcp_env():
    """Create MCP server environment for testing."""
    server_path = os.environ.get('MCP_SERVER_PATH')
    if not server_path:
        pytest.fail('MCP_SERVER_PATH environment variable must be set')

    server = MCPServerManager(
        server_path,
        aws_profile=os.environ.get('AWS_PROFILE'),
        aws_region=os.environ.get('AWS_REGION'),
        server_args='--allow-write',
    )
    server.start()
    client = MCPClient(server)
    client.initialize()
    yield client
    server.stop()
