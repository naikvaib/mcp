import pytest
import os
import sys
import boto3

# Add current directory to Python path to make src imports work
sys.path.insert(0, os.path.dirname(__file__))
from data_processing_mcp_server_tests.core.mcp_server import MCPServerManager
from data_processing_mcp_server_tests.core.mcp_client import MCPClient


@pytest.fixture(scope="session")
def mcp_env():
    server = MCPServerManager(
        "/home/lfqing/workplace/mcp/src/aws-dataprocessing-mcp-server/awslabs/aws_dataprocessing_mcp_server",
        aws_profile=os.environ.get('AWS_PROFILE'),
        aws_region=os.environ.get("AWS_REGION"),
        server_args="--allow-write"
    )
    server.start()
    client = MCPClient(server)
    client.initialize()
    yield client
    server.stop()
