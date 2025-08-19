from typing import List

from data_processing_mcp_server_tests.core.mcp_validators import ContainsTextValidator, AWSBotoValidator
from data_processing_mcp_server_tests.core.mcp_cleanup import DeleteAWSResources
from data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase


def glue_workflows_test_cases(aws_clients) -> List[MCPTestCase]:
    return [
        MCPTestCase(
            test_name="create_glue_workflow_basic",
            tool_name="manage_aws_glue_workflows",
            input_params={
                "operation": "create-workflow",
                "workflow_name": "mcp_test_workflow",
                "workflow_definition": {
                    "Description": "Test workflow created by MCP",
                    "DefaultRunProperties": {
                        "env": "test",
                        "team": "dataprocessing"
                    }
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully created workflow"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_workflow",
                    operation_input_params={"Name": "mcp_test_workflow"},
                    expected_keys=["Description", "DefaultRunProperties"]
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="delete_workflow",
                    delete_params={"Name": "mcp_test_workflow"},
                    boto_client=aws_clients["glue"]
                )
            ]
        ),
        MCPTestCase(
            test_name="get_glue_workflow_basic",
            tool_name="manage_aws_glue_workflows",
            input_params={
                "operation": "get-workflow",
                "workflow_name": "mcp_test_workflow"
            },
            dependencies=["create_glue_workflow_basic"],
            validators=[
                ContainsTextValidator("Successfully retrieved workflow")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="create_glue_workflow_missing_name",
            tool_name="manage_aws_glue_workflows",
            input_params={
                "operation": "create-workflow",
                "workflow_definition": {
                    "Description": "Test workflow created by MCP"
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("workflow_name and workflow_definition are required for create-workflow operation")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="list_glue_workflows",
            tool_name="manage_aws_glue_workflows",
            input_params={
                "operation": "list-workflows",
                "max_results": 10
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully retrieved workflows")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="get_glue_workflow_not_exist",
            tool_name="manage_aws_glue_workflows",
            input_params={
                "operation": "get-workflow",
                "workflow_name": "non_existent_workflow"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("not found")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="start_glue_workflow_run",
            tool_name="manage_aws_glue_workflows",
            input_params={
                "operation": "start-workflow-run",
                "workflow_name": "mcp_test_workflow",

            },
            dependencies=["create_glue_workflow_basic", "create_glue_trigger_basic"],
            validators=[
                ContainsTextValidator("Successfully started workflow run"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_workflow_run",
                    operation_input_params={"Name": "mcp_test_workflow"},
                    injectable_params={"RunId": "{{start_glue_workflow_run.result.content[0].text.run_id}}"},
                    expected_keys=["RunId", "Status"]
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="stop_workflow_run",
                    delete_params={"Name": "mcp_test_workflow"},
                    boto_client=aws_clients["glue"],
                    resource_field="run_id",
                    target_param_key="RunId"
                )
            ]
        ),
        MCPTestCase(
            test_name="start_glue_workflow_run_missing_name",
            tool_name="manage_aws_glue_workflows",
            input_params={
                "operation": "start-workflow-run"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("workflow_name is required for start-workflow-run operation")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="delete_glue_workflow",
            tool_name="manage_aws_glue_workflows",
            input_params={
                "operation": "delete-workflow",
                "workflow_name": "mcp_test_workflow"
            },
            dependencies=["create_glue_workflow_basic"],
            validators=[
                ContainsTextValidator("Successfully deleted workflow"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_workflow",
                    operation_input_params={"Name": "mcp_test_workflow"},
                    validate_absence=True
                )
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="delete_glue_workflow_not_exist",
            tool_name="manage_aws_glue_workflows",
            input_params={
                "operation": "delete-workflow",
                "workflow_name": "non_existent_workflow"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("not found")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="delete_glue_workflow_missing_name",
            tool_name="manage_aws_glue_workflows",
            input_params={
                "operation": "delete-workflow"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("workflow_name is required for delete-workflow operation")
            ],
            clean_ups=[]
        )
    ]
