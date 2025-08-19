from typing import List

from data_processing_mcp_server_tests.core.mcp_validators import ContainsTextValidator, AWSBotoValidator
from data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase
from data_processing_mcp_server_tests.core.mcp_cleanup import DeleteAWSResources

def emr_cluster_test_cases(aws_clients) -> List[MCPTestCase]:
    return [
        MCPTestCase(
            test_name="create_emr_cluster_basic",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "create-cluster",
                "name": "mcp-test-emr-cluster",
                "release_label": "emr-6.3.0",
                "applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
                "instances": {
                    "InstanceGroups": [
                        {
                            "Name": "Master",
                            "InstanceRole": "MASTER",
                            "InstanceType": "m5.xlarge",
                            "InstanceCount": 1
                        },
                        {
                            "Name": "Core",
                            "InstanceRole": "CORE",
                            "InstanceType": "m5.xlarge",
                            "InstanceCount": 2
                        }
                    ],
                    "Ec2KeyName": "your-ec2-key-name",
                    "KeepJobFlowAliveWhenNoSteps": True
                },
                "service_role": "EMR_DefaultRole",
                "job_flow_role": "EMR_EC2_DefaultRole"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully created EMR cluster"),
                AWSBotoValidator(
                    boto_client=aws_clients["emr"],
                    operation="describe_cluster",
                    operation_input_params={},
                    expected_keys=["Name", "release_label"],
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
            test_name="create_emr_cluster_missing_name",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "create-cluster",
                "release_label": "emr-6.3.0",
                "applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
                "instance_type": "m5.xlarge",
                "instance_count": 2,
                "ec2_key_name": "your-ec2-key-name"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("name, release_label, and instances are required for create-cluster operation")
            ],
            clean_ups=[]
        ),

        MCPTestCase(
            test_name="list_emr_clusters",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "list-clusters",
                "max_results": 10
            },
            dependencies=["create_emr_cluster_basic"],
            validators=[
                ContainsTextValidator("Successfully listed EMR clusters")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="create_security_configuration",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "create-security-configuration",
                "name": "mcp-test-security-config",
                "security_configuration": "{\"EncryptionConfiguration\":{\"EnableInTransitEncryption\":true,\"InTransitEncryptionConfiguration\":{\"TLSCertificateConfiguration\":{\"CertificateAuthorityArn\":\"arn:aws:acm:us-west-2:123456789012:certificate/12345678-1234-1234-1234-123456789012\"}}}}"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully created security configuration"),
                AWSBotoValidator(
                    boto_client=aws_clients["emr"],
                    operation="describe_security_configuration",
                    operation_input_params={"Name": "mcp-test-security-config"},
                    expected_keys=["EncryptionConfiguration"]
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="delete_security_configuration",
                    delete_params={"Name": "mcp-test-security-config"},
                    boto_client=aws_clients["emr"]
                )
            ]
        ),
        MCPTestCase(
            test_name="describe_emr_cluster",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "describe-cluster",
                "cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}" # Use the cluster_id from the dependency's response
            },
            dependencies=["create_emr_cluster_basic"],
            validators=[
                ContainsTextValidator("Successfully described EMR cluster")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="describe_emr_cluster_missing_id",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "describe-cluster"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("cluster_id is required for describe-cluster operation")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="modify_emr_cluster",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "modify-cluster",
                "cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}", # Use the cluster_id from the dependency's response
                "step_concurrency_level": 1
            },
            dependencies=["create_emr_cluster_basic"],
            validators=[
                ContainsTextValidator("Successfully modified EMR cluster"),
                AWSBotoValidator(
                    boto_client=aws_clients["emr"],
                    operation="describe_cluster",
                    operation_input_params={},
                    expected_keys=["step_concurrency_level"],
                    injectable_params= {"cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}"}
                )
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="modify_emr_cluster_missing_id",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "modify-cluster",
                "name": "mcp-test-emr-cluster-modified"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("cluster_id is required for modify-cluster operation")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="modify_emr_cluster_attribute",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "modify-cluster-attributes",
                "cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}", 
                "termination_protected": True,
            },
            dependencies=["create_emr_cluster_basic"],
            validators=[
                ContainsTextValidator("Successfully modified attributes for EMR cluster"),
                AWSBotoValidator(
                    boto_client=aws_clients["emr"],
                    operation="describe_cluster",
                    operation_input_params={},  
                    expected_keys=["termination_protected"],
                    injectable_params={"cluster_id": "{{create_emr_cluster_basic.result.content[0].text.cluster_id}}"}
                )
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="terminate_emr_cluster",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "terminate-clusters",
                "cluster_ids": ["{{create_emr_cluster_basic.result.content[0].text.cluster_id}}"]
            },
            dependencies=["create_emr_cluster_basic"],
            validators=[
                ContainsTextValidator("Successfully initiated termination for 1 MCP-managed EMR clusters")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="terminate_emr_cluster_missing_id",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "terminate-clusters"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("cluster_ids is required for terminate-clusters operation")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="create_cluster_with_fleet",
            tool_name="manage_aws_emr_clusters",
            input_params={
                "operation": "create-cluster",
                "name": "mcp-test-emr-cluster-with-fleet",
                "release_label": "emr-6.3.0",
                "applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
                "instances": {
                    "InstanceFleets": [
                        {
                            "Name": "Master Fleet",
                            "InstanceFleetType": "MASTER",
                            "TargetOnDemandCapacity": 1,
                            "InstanceTypeConfigs": [
                                {
                                    "InstanceType": "m5.xlarge",
                                    "WeightedCapacity": 1
                                }
                            ]
                        },
                        {
                            "Name": "Core Fleet",
                            "InstanceFleetType": "CORE",
                            "TargetOnDemandCapacity": 2,
                            "InstanceTypeConfigs": [
                                {
                                    "InstanceType": "m5.xlarge",
                                    "WeightedCapacity": 1
                                }
                            ]
                        }
                    ],
                    "Ec2KeyName": "your-ec2-key-name",
                    "KeepJobFlowAliveWhenNoSteps": True
                },
                "service_role": "EMR_DefaultRole",
                "job_flow_role": "EMR_EC2_DefaultRole"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully created EMR cluster"),
                AWSBotoValidator(
                    boto_client=aws_clients["emr"],
                    operation="describe_cluster",
                    operation_input_params={},
                    expected_keys=["InstanceFleets"],
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
    ]
