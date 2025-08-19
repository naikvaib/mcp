from typing import List

from data_processing_mcp_server_tests.core.mcp_validators import ContainsTextValidator, AWSBotoValidator
from data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase
from data_processing_mcp_server_tests.core.mcp_cleanup import DeleteAWSResources

def athena_named_query_test_cases(aws_clients) -> List[MCPTestCase]:
    return [
        MCPTestCase(
            test_name="create_athena_query",
            tool_name="manage_aws_athena_named_queries",
            input_params={
                "operation": "create-named-query",
                "name": "mcp_test_named_query",
                "description": "This is a test named query for MCP validation.",
                "database": "mcp_test_database",
                "query_string": "SELECT 1",
                "work_group": "primary"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully created named")
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="delete_named_query",
                    boto_client=aws_clients["athena"],
                    resource_field="named_query_id",        
                    target_param_key="NamedQueryId",        
                    param_is_list=False
                )
            ]
        ),
        MCPTestCase(
            test_name="get_athena_queries",
            tool_name="manage_aws_athena_named_queries",
            input_params={
                "operation": "list-named-queries",
                "max_results": 10
            },
            dependencies=["create_athena_query"],
            validators=[
                ContainsTextValidator("Successfully listed named queries")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="batch_get_athena_named_queries",
            tool_name="manage_aws_athena_named_queries",
            input_params={
                "operation": "batch-get-named-query",
                "named_query_ids": ["{{create_athena_query.result.content[0].text.named_query_id}}"]
            },
            dependencies=["create_athena_query"],
            validators=[
                ContainsTextValidator("Successfully retrieved named queries")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="get_athena_named_query",
            tool_name="manage_aws_athena_named_queries",
            input_params={
                "operation": "get-named-query",
                "named_query_id": "{{create_athena_query.result.content[0].text.named_query_id}}"
            },
            dependencies=["create_athena_query"],
            validators=[
                ContainsTextValidator("Successfully retrieved named query")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="get_athena_named_query_missing_id",
            tool_name="manage_aws_athena_named_queries",
            input_params={
                "operation": "get-named-query"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("named_query_id is required for get-named-query operation")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="update_athena_named_query",
            tool_name="manage_aws_athena_named_queries",
            input_params={
                "operation": "update-named-query",  
                "named_query_id": "{{create_athena_query.result.content[0].text.named_query_id}}",
                "name": "mcp_test_named_query_updated",
                "description": "This is an updated test named query for MCP validation.",
                "query_string": "SELECT 2",
                "work_group": "primary"
            },
            dependencies=["create_athena_query"],
            validators=[
                ContainsTextValidator("Successfully updated named query")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="update_athena_named_query_missing_id",
            tool_name="manage_aws_athena_named_queries",
            input_params={
                "operation": "update-named-query",
                "name": "mcp_test_named_query_updated",
                "description": "This is an updated test named query for MCP validation.",
                "query_string": "SELECT 2",
                "work_group": "primary"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("named_query_id is required for update-named-query operation")
            ],
            clean_ups=[]
        ),  
        MCPTestCase(
            test_name="delete_athena_named_query",
            tool_name="manage_aws_athena_named_queries",
            input_params={
                "operation": "delete-named-query",
                "named_query_id": "{{create_athena_query.result.content[0].text.named_query_id}}"
            },
            dependencies=["create_athena_query"],
            validators=[
                ContainsTextValidator("Successfully deleted named query")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="delete_athena_named_query_missing_id",
            tool_name="manage_aws_athena_named_queries",
            input_params={
                "operation": "delete-named-query"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("named_query_id is required for delete-named-query operation")
            ],
            clean_ups=[]
        )
    ]
