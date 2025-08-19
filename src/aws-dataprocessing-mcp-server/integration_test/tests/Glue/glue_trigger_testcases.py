from typing import List

from data_processing_mcp_server_tests.core.mcp_validators import ContainsTextValidator, AWSBotoValidator
from data_processing_mcp_server_tests.core.mcp_cleanup import DeleteAWSResources
from data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase

def glue_triggers_test_cases(s3_bucket, glue_role, aws_clients) -> List[MCPTestCase]:
    return [
        MCPTestCase(
            test_name="create_glue_job_basic",
            tool_name="manage_aws_glue_jobs",
            input_params={
                "operation": "create-job",
                "job_name": "mcp-test-job-basic",
                "job_definition": {
                    "Command": {
                        "Name": "glueetl",
                        "ScriptLocation": f"s3://{s3_bucket}/mcp-test-script.py"
                    },
                    "Role": glue_role,
                    "GlueVersion": "4.0",
                    "MaxCapacity": 2,
                    "Description": "Basic test job created by MCP server"
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully created Glue job"),
                AWSBotoValidator(
                    aws_clients["glue"], 
                    operation="get_job", 
                    operation_input_params={"job_name": "mcp-test-job-basic"}, 
                    expected_keys=["job_definition"])
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="delete_job", 
                    delete_params={"job_name": "mcp-test-job-basic"}, 
                    boto_client=aws_clients["glue"])
            ]
        ),
        MCPTestCase(
            test_name="create_glue_trigger_basic",
            tool_name="manage_aws_glue_triggers",
            input_params={
                "operation": "create-trigger",
                "trigger_name": "mcp_test_trigger",
                "trigger_definition": {
                    "Type": "ON_DEMAND", 
                    "Description": "Test trigger created by MCP",
                    "Actions": [
                        {
                            "JobName": "mcp-test-job-basic",
                            "Arguments": {"--key1": "value1"}
                        }
                    ],
                    "WorkflowName": "mcp_test_workflow"  
                }
            },
            dependencies=["create_glue_job_basic", "create_glue_workflow_basic"],
            validators=[
                ContainsTextValidator("Successfully created trigger"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_trigger",
                    operation_input_params={"Name": "mcp_test_trigger"},
                    expected_keys=["Name", "Type", "Description", "Actions"]
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="delete_trigger",
                    delete_params={"Name": "mcp_test_trigger"},
                    boto_client=aws_clients["glue"]
                )
            ]
        ),
        MCPTestCase(
            test_name="create_glue_trigger_no_workflow",
            tool_name="manage_aws_glue_triggers",
            input_params={
                "operation": "create-trigger",
                "trigger_name": "mcp_test_trigger_no_workflow",
                "trigger_definition": {
                    "Type": "ON_DEMAND", 
                    "Description": "Test trigger without workflow",
                    "Actions": [
                        {
                            "JobName": "mcp-test-job-basic",
                            "Arguments": {"--key1": "value1"}
                        }
                    ]
                }
            },
            dependencies=["create_glue_job_basic"],
            validators=[
                ContainsTextValidator("Successfully created trigger"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_trigger",
                    operation_input_params={"Name": "mcp_test_trigger_no_workflow"},
                    expected_keys=["Name", "Type", "Description", "Actions"]
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="delete_trigger",
                    delete_params={"Name": "mcp_test_trigger_no_workflow"},
                    boto_client=aws_clients["glue"]
                )
            ]
        ),
        MCPTestCase(
            test_name="get_glue_trigger_basic",
            tool_name="manage_aws_glue_triggers",
            input_params={
                "operation": "get-trigger",
                "trigger_name": "mcp_test_trigger"
            },
            dependencies=["create_glue_trigger_basic"],
            validators=[
                ContainsTextValidator("Successfully retrieved trigger")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="create_glue_trigger_missing_name",
            tool_name="manage_aws_glue_triggers",
            input_params={
                "operation": "create-trigger",
                "trigger_definition": {
                    "Type": "ON_DEMAND", 
                    "Description": "Test trigger created by MCP"
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("trigger_name and trigger_definition are required for create-trigger operation")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="get_glue_trigger_not_exist",
            tool_name="manage_aws_glue_triggers",
            input_params={
                "operation": "get-trigger",
                "trigger_name": "non_existent_trigger"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("not found")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="start_glue_trigger_run",
            tool_name="manage_aws_glue_triggers",
            input_params={
                "operation": "start-trigger",
                "trigger_name": "mcp_test_trigger"
            },
            dependencies=["create_glue_trigger_basic"],
            validators=[
                ContainsTextValidator("Successfully started trigger"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_trigger",
                    operation_input_params={"Name": "mcp_test_trigger"},
                    expected_keys=["Name", "Description", "Actions"]
                )
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="start_glue_trigger_run_missing_name",
            tool_name="manage_aws_glue_triggers",
            input_params={
                "operation": "start-trigger"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("trigger_name is required for start-trigger operation")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="stop_glue_trigger_run_not_exist",
            tool_name="manage_aws_glue_triggers",
            input_params={
                "operation": "stop-trigger",
                "trigger_name": "non_existent_trigger"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("not found")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="stop_glue_trigger_run_missing_name",
            tool_name="manage_aws_glue_triggers",
            input_params={
                "operation": "stop-trigger"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("trigger_name is required for stop-trigger operation")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="delete_glue_trigger",
            tool_name="manage_aws_glue_triggers",
            input_params={
                "operation": "delete-trigger",
                "trigger_name": "mcp_test_trigger_no_workflow"
            },
            dependencies=["create_glue_trigger_no_workflow"],
            validators=[
                ContainsTextValidator("Successfully deleted trigger"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_trigger",
                    operation_input_params={"Name": "mcp_test_trigger_no_workflow"},
                    validate_absence=True
                )
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="delete_glue_trigger_not_exist",
            tool_name="manage_aws_glue_triggers",
            input_params={
                "operation": "delete-trigger",
                "trigger_name": "non_existent_trigger"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("not found")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="delete_glue_trigger_missing_name",
            tool_name="manage_aws_glue_triggers",
            input_params={
                "operation": "delete-trigger"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("trigger_name is required for delete-trigger operation")
            ],
            clean_ups=[]
        )

    ]
