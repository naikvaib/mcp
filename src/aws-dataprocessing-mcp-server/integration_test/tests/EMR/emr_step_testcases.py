from typing import List

from data_processing_mcp_server_tests.core.mcp_validators import ContainsTextValidator, AWSBotoValidator
from data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase
from data_processing_mcp_server_tests.core.mcp_cleanup import DeleteAWSResources

def emr_step_test_cases(s3_bucket, aws_clients) -> List[MCPTestCase]:
    return [
        MCPTestCase(
            test_name="add_step_to_emr_cluster",
            tool_name="manage_aws_emr_ec2_steps",
            input_params={
                "operation": "add-steps",
                "cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}",  # Use the cluster_id from the dependency's response
                "steps": [
                    {
                        "Name": "SparkExampleStep",
                        "ActionOnFailure": "CONTINUE",
                        "HadoopJarStep": {
                            "Jar": "command-runner.jar",
                            "Args": [
                                "spark-submit",
                                "--deploy-mode",
                                "cluster",
                                "s3://{s3_bucket}/app.jar",
                                "--arg1", "val1"
                            ]
                        }
                    }
                ]
            },
            dependencies=["create_emr_cluster_basic"],
            validators=[
                ContainsTextValidator("Successfully added 1 steps to EMR cluster"),
                AWSBotoValidator(
                    boto_client=aws_clients["emr"],
                    operation="describe_step",
                    operation_input_params={},
                    expected_keys=["StepId"],
                    injectable_params={"cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}",
                                       "step_id": "{{add_step_to_emr_cluster.result.content[0].text.step_ids[0]}}"}
                )
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="add_step_missing_cluster_id",
            tool_name="manage_aws_emr_ec2_steps",
            input_params={
                "operation": "add-step",
                "steps": {
                    "Name": "MCP Test Step",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["spark-submit", "--deploy-mode", "cluster", "script.py"]
                    }
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Field required")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="describe_emr_step",
            tool_name="manage_aws_emr_ec2_steps",
            input_params={
                "operation": "describe-step",
                "cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}",  # Use the cluster_id from the dependency's response
                "step_id": "{{add_step_to_emr_cluster.result.content[0].text.step_ids[0]}}"  # Use the step_id from the dependency's response
            },
            dependencies=["add_step_to_emr_cluster"],
            validators=[
                ContainsTextValidator("Successfully described step")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="describe_emr_step_missing_ids",
            tool_name="manage_aws_emr_ec2_steps",
            input_params={
                "operation": "describe-step"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Field required")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="list_emr_steps",
            tool_name="manage_aws_emr_ec2_steps",
            input_params={  
                "operation": "list-steps",
                "cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}",  # Use the cluster_id from the dependency's response
                "max_results": 10
            },
            dependencies=["add_step_to_emr_cluster"],
            validators=[
                ContainsTextValidator("Successfully listed steps for EMR cluster", expected_count=1)
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="list_emr_steps_missing_cluster_id",
            tool_name="manage_aws_emr_ec2_steps",
            input_params={
                "operation": "list-steps",
                "max_results": 10
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Field required")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="cancel_emr_step",
            tool_name="manage_aws_emr_ec2_steps",
            input_params={
                "operation": "cancel-steps",
                "cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}",  # Use the cluster_id from the dependency's response
                "step_ids": ["{{add_step_to_emr_cluster.result.content[0].text.step_ids[0]}}"]  # Use the step_id from the dependency's response
            },
            dependencies=["add_step_to_emr_cluster"],
            validators=[
                ContainsTextValidator("Successfully initiated cancellation for 1 steps")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="cancel_emr_step_missing_ids",
            tool_name="manage_aws_emr_ec2_steps",
            input_params={
                "operation": "cancel-steps"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Field required")
            ],
            clean_ups=[]
        )

    ]
