from typing import List

from data_processing_mcp_server_tests.core.mcp_validators import ContainsTextValidator, AWSBotoValidator
from data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase
from data_processing_mcp_server_tests.core.mcp_cleanup import DeleteAWSResources

def glue_catalogs_test_cases(aws_clients) -> List[MCPTestCase]:
    return [
        MCPTestCase(
            test_name="create_glue_catalog_basic",
            tool_name="manage_aws_glue_catalog",
            input_params={
                "operation": "create-catalog",
                "catalog_id": "mcp-test-catalog",  
                "catalog_input": {
                    "Description": "Test catalog created by MCP",
                    "Parameters": {
                        "env": "test",
                        "team": "dataprocessing"
                    }
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully created catalog"),
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="delete_data_catalog",  
                    delete_params={"Name": "mcp-test-catalog"},
                    boto_client=aws_clients["glue"]
                )
            ]
        ),
        MCPTestCase(
            test_name="list_glue_catalogs",
            tool_name="manage_aws_glue_catalog",
            input_params={
                "operation": "list-catalogs",
                "max_results": 10
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully listed")
            ],
            clean_ups=[]
        )
    ]
