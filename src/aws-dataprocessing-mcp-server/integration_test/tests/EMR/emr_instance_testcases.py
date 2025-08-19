from typing import List

from data_processing_mcp_server_tests.core.mcp_validators import ContainsTextValidator, AWSBotoValidator
from data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase
from data_processing_mcp_server_tests.core.mcp_cleanup import DeleteAWSResources

def emr_instance_test_cases(aws_clients) -> List[MCPTestCase]:
    return[
        MCPTestCase(
            test_name="add_instance_fleet_to_emr_cluster",
            tool_name="manage_aws_emr_ec2_instances",
            input_params={
                "operation": "add-instance-fleet",
                "cluster_id": "{{create_cluster_with_fleet.result.content[0].text.cluster_id}}",  # Use the cluster_id from the dependency's response
                "instance_fleet": {
                    "InstanceFleetType": "TASK",
                    "Name": "mcp-test-fleet",
                    "TargetOnDemandCapacity": 1,
                    "TargetSpotCapacity": 1,
                    "InstanceTypeConfigs": [
                    {
                        "InstanceType": "m5.xlarge",
                        "WeightedCapacity": 1,
                        "BidPriceAsPercentageOfOnDemandPrice": 80
                    }
                    ]
                }
            },
            dependencies=["create_cluster_with_fleet"],
            validators=[
                ContainsTextValidator("Successfully added instance fleet to EMR cluster"),
                AWSBotoValidator(
                    boto_client=aws_clients["emr"],
                    operation="list_instance_fleets",
                    operation_input_params={},
                    expected_keys=["InstanceFleetId"],
                    injectable_params={"cluster_id": "{{create_cluster_with_fleet.result.content[0].text.cluster_id}}"}
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="terminate_job_flows",
                    boto_client=aws_clients["emr"],
                    resource_field="cluster_id",
                    target_param_key="JobFlowIds",
                    param_is_list=True,
                )
            ]
        ),
        MCPTestCase(
            test_name="add_instance_fleet_missing_cluster_id",
            tool_name="manage_aws_emr_ec2_instances",
            input_params={
                "operation": "add-instance-fleet",
                "instance_fleet": {
                    "InstanceFleetType": "TASK",
                    "Name": "mcp-test-fleet",
                    "TargetOnDemandCapacity": 1,
                    "TargetSpotCapacity": 1,
                    "InstanceTypeConfigs": [
                        {
                            "InstanceType": "m5.xlarge",
                            "WeightedCapacity": 1,
                            "BidPriceAsPercentageOfOnDemandPrice": 80
                        }
                    ]
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("cluster_id and instance_fleet are required")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="add_instance_group_to_emr_cluster_basic",
            tool_name="manage_aws_emr_ec2_instances",
            input_params={
                "operation": "add-instance-groups",
                "cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}",  # Use the cluster_id from the dependency's response
                "instance_groups": [
                    {
                    "Name": "TestCoreGroup",
                    "InstanceRole": "TASK",
                    "InstanceType": "m1.small",
                    "InstanceCount": 2
                    }
                ]
            },
            dependencies=["create_emr_cluster_basic"],
            validators=[
                ContainsTextValidator("Successfully added instance groups to EMR cluster"),
                AWSBotoValidator(
                    boto_client=aws_clients["emr"],
                    operation="list_instance_groups",
                    operation_input_params={},
                    expected_keys=["InstanceGroupId"],
                    injectable_params={"cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}"}
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="terminate_job_flows",
                    boto_client=aws_clients["emr"],
                    resource_field="cluster_id",
                    target_param_key="JobFlowIds",
                    param_is_list=True,
                )
            ]
        ),
        MCPTestCase(
            test_name="modify_instance_fleet_missing_ids",
            tool_name="manage_aws_emr_ec2_instances",
            input_params={
                "operation": "modify-instance-fleet",
                "instance_fleet": {
                    "Name": "mcp-test-fleet-modified",
                    "TargetOnDemandCapacity": 2,
                    "TargetSpotCapacity": 2,
                    "InstanceTypeConfigs": [
                        {
                            "InstanceType": "m5.xlarge",
                            "WeightedCapacity": 1,
                            "BidPriceAsPercentageOfOnDemandPrice": 80
                        }
                    ]
                }   
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("cluster_id, instance_fleet_id, and instance_fleet_config are required")
            ],
            clean_ups=[]
        ),
        
        MCPTestCase(
            test_name="modify_instance_group_missing_ids",
            tool_name="manage_aws_emr_ec2_instances",
            input_params={
                "operation": "modify-instance-groups",
                "instance_group": {
                    "InstanceCount": 3,
                    "InstanceType": "m5.xlarge"
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("instance_group_configs is required for modify-instance-groups operation")
            ],
            clean_ups=[]
        ),

        MCPTestCase(
            test_name="list_instance_fleets",
            tool_name="manage_aws_emr_ec2_instances",
            input_params={
                "operation": "list-instance-fleets",
                "cluster_id": "{{create_cluster_with_fleet.result.content[0].text.cluster_id}}",  # Use the cluster_id from the dependency's response
                "max_results": 10
            },
            dependencies=["add_instance_fleet_to_emr_cluster"],
            validators=[
                ContainsTextValidator("Successfully listed instance fleets for EMR cluster", expected_count=3) # 1 master, 1 core, 1 task
            ],
            clean_ups=[]
        ),

        MCPTestCase(
            test_name="list_instance_groups",
            tool_name="manage_aws_emr_ec2_instances",
            input_params={
                "operation": "list-instances",
                "cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}", 
                "max_results": 10
            },
            dependencies=["add_instance_group_to_emr_cluster_basic"],
            validators=[
                ContainsTextValidator("uccessfully listed instances for EMR cluster ") 
            ],
            clean_ups=[]
        ),

        MCPTestCase(
            test_name="list_supported_instance_types",
            tool_name="manage_aws_emr_ec2_instances",
            input_params={
                "operation": "list-supported-instance-types",
                "release_label": "emr-6.3.0",
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully listed supported instance types for EMR")
            ],
            clean_ups=[]
        ),
    ]
