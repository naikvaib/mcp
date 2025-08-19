from typing import List

from data_processing_mcp_server_tests.core.mcp_validators import ContainsTextValidator, AWSBotoValidator
from data_processing_mcp_server_tests.core.mcp_cleanup import DeleteAWSResources
from data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase

def glue_connections_test_cases(aws_clients) -> List[MCPTestCase]:
    return [
        MCPTestCase(
            test_name="create_connection_basic",
            tool_name="manage_aws_glue_connections",
            input_params={
                "operation": "create-connection",
                "connection_name": "mcp_test_connection",
                "connection_input": {
                    "ConnectionType": "JDBC",
                    "ConnectionProperties": {
                        "JDBC_CONNECTION_URL": "jdbc:mysql://example.com:3306/testdb",
                        "USERNAME": "testuser",
                        "PASSWORD": "testpassword"
                    }
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully created connection"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_connection",
                    operation_input_params={"Name": "mcp_test_connection"},
                    expected_keys=["ConnectionType", "ConnectionProperties"]
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="delete_connection",
                    delete_params={"connection_name": "mcp_test_connection"},
                    boto_client=aws_clients["glue"]
                )
            ]
        ),  
        MCPTestCase(
            test_name="get_connection_basic",
            tool_name="manage_aws_glue_connections",
            input_params={
                "operation": "get-connection",
                "connection_name": "mcp_test_connection"
            },
            dependencies=["create_connection_basic"],
            validators=[
                ContainsTextValidator("Successfully retrieved connection")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="create_connection_missing_name",
            tool_name="manage_aws_glue_connections",
            input_params={  
                "operation": "create-connection",
                "connection_input": {
                    "ConnectionType": "JDBC",
                    "ConnectionProperties": {
                        "JDBC_CONNECTION_URL": "jdbc:mysql://example.com:3306/testdb",
                        "USERNAME": "testuser",
                        "PASSWORD": "testpassword"
                    }
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("connection_name and connection_input are required for create operation")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="create_connection_with_description",
            tool_name="manage_aws_glue_connections",
            input_params={
                "operation": "create-connection",
                "connection_name": "mcp_test_connection_with_description",
                "connection_input": {
                    "ConnectionType": "JDBC",
                    "ConnectionProperties": {
                        "JDBC_CONNECTION_URL": "jdbc:mysql://example.com:3306/testdb",
                        "USERNAME": "testuser",
                        "PASSWORD": "testpassword"
                    },
                    "Description": "Test connection with description"
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully created connection"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_connection",
                    operation_input_params={"Name": "mcp_test_connection_with_description"},
                    expected_keys=["ConnectionType", "ConnectionProperties", "Description"] 
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="delete_connection",
                    delete_params={"connection_name": "mcp_test_connection_with_description"},
                    boto_client=aws_clients["glue"]
                )   
            ]
        ),
        MCPTestCase(
            test_name="get_connection_not_exist",
            tool_name="manage_aws_glue_connections",
            input_params={
                "operation": "get-connection",
                "connection_name": "non_existent_connection"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("not found")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="get_connections_all",
            tool_name="manage_aws_glue_connections",
            input_params={
                "operation": "list-connections",
                "max_results": 10
            },
            dependencies=["create_connection_basic", "create_connection_with_description"],
            validators=[
                ContainsTextValidator("Successfully listed 2 connections", expected_count=2)  
            ],
            clean_ups=[]
        ),  
        MCPTestCase(
            test_name="update_connection_description",
            tool_name="manage_aws_glue_connections",
            input_params={
                "operation": "update-connection",
                "connection_name": "mcp_test_connection",
                "connection_input": {
                    "Description": "Updated description for MCP test connection",
                    "ConnectionType": "JDBC",
                    "ConnectionProperties": {
                        "JDBC_CONNECTION_URL": "jdbc:mysql://example.com:3306/testdb",
                        "USERNAME": "testuser",
                        "PASSWORD": "testpassword"
                    }
                }
            },
            dependencies=["create_connection_basic"],
            validators=[
                ContainsTextValidator("Successfully updated connection"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_connection",
                    operation_input_params={"Name": "mcp_test_connection"},
                    expected_keys=["Description"]
                )
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="update_connection_missing_name",
            tool_name="manage_aws_glue_connections",
            input_params={
                "operation": "update-connection",
                "connection_input": {
                    "Description": "Updated description for MCP test connection"
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("connection_name and connection_input are required for update operation")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="update_connection_not_exist",
            tool_name="manage_aws_glue_connections",
            input_params={
                "operation": "update-connection",
                "connection_name": "non_existent_connection",
                "connection_input": {
                    "Description": "Updated description for MCP test connection",
                    "ConnectionType": "JDBC",
                    "ConnectionProperties": {
                        "JDBC_CONNECTION_URL": "jdbc:mysql://example.com:3306/testdb",
                        "USERNAME": "testuser",
                        "PASSWORD": "testpassword"
                    }
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("not found")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="delete_connection_basic",
            tool_name="manage_aws_glue_connections",
            input_params={
                "operation": "delete-connection",
                "connection_name": "mcp_test_connection"
            },
            dependencies=["create_connection_basic"],
            validators=[
                ContainsTextValidator("Successfully deleted connection"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_connection",
                    operation_input_params={"Name": "mcp_test_connection"},
                    validate_absence=True
                )
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="delete_connection_not_exist",
            tool_name="manage_aws_glue_connections",
            input_params={
                "operation": "delete-connection",
                "connection_name": "non_existent_connection"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("not found")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="delete_connection_missing_name",
            tool_name="manage_aws_glue_connections",
            input_params={
                "operation": "delete-connection"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("connection_name is required for delete operation")
            ],
            clean_ups=[]
        )
    ]
