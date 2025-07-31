import pytest
import os
import sys
import boto3


from src.data_processing_mcp_server_tests.core.mcp_server import MCPServerManager
from src.data_processing_mcp_server_tests.core.mcp_client import MCPClient


@pytest.fixture(scope="session")
def mcp_env():
    server_path = os.environ.get('MCP_SERVER_PATH')
    if not server_path:
        pytest.fail("MCP_SERVER_PATH environment variable must be set")
    
    server = MCPServerManager(
        server_path,
        aws_profile=os.environ.get('AWS_PROFILE'),
        aws_region=os.environ.get("AWS_REGION"),
        server_args="--allow-write"
    )
    server.start()
    client = MCPClient(server)
    client.initialize()
    yield client
    server.stop()
