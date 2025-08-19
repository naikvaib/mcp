
from typing import List
import os
from data_processing_mcp_server_tests.core.mcp_validators import ContainsTextValidator, AWSBotoValidator
from data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase
from data_processing_mcp_server_tests.core.mcp_cleanup import DeleteAWSResources

def s3_test_cases(s3_bucket, aws_clients) -> List[MCPTestCase]:
    return [
        MCPTestCase(
            test_name="list_s3_buckets_default_region",
            tool_name="list_s3_buckets",
            input_params={},
            dependencies=[],
            validators=[
                ContainsTextValidator("Looking for S3 buckets")
            ],
            clean_ups=[]
        ),  
        MCPTestCase(
            test_name="list_s3_buckets_with_region",
            tool_name="list_s3_buckets",
            input_params={
                "region": os.environ.get("AWS_REGION", "us-west-2")
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Looking for S3 buckets", bucket_count=0)
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="list_s3_buckets_with_invalid_region",
            tool_name="list_s3_buckets",
            input_params={
                "region": "invalid-region"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Error listing S3 buckets: An error occurred (InvalidRegion) when calling the ListBuckets operation: The region 'invalid-region' is invalid")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="upload_file_to_s3",
            tool_name="upload_to_s3",
            input_params={
                "bucket_name": f"{s3_bucket}",
                "code_content": "print('Hello from MCP S3 test')",
                "s3_key": "mcp-test-upload-s3.py"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Python code uploaded successfully!"),
                AWSBotoValidator(
                    boto_client=aws_clients["s3"],
                    operation="get_object",
                    operation_input_params={
                        "Bucket": f"{s3_bucket}",
                        "Key": "mcp-test-upload-s3.py"
                    },
                    expected_keys=["code_content"]
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="delete_object",
                    delete_params={"Bucket": f"{s3_bucket}", "Key": "mcp-test-upload-s3.py"},
                    boto_client=aws_clients["s3"]
                )   
            ]
        ),
        MCPTestCase(
            test_name="upload_file_to_s3_missing_bucket",
            tool_name="upload_to_s3",
            input_params={
                "code_content": "print('Hello from MCP S3 test')",
                "s3_key": "mcp-test-upload-s3.py"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Field required")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="analyze_all_s3_usage",
            tool_name="analyze_s3_usage_for_data_processing",
            input_params={
                "bucket_name": None
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Analyzing bucket")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="analyze_s3_usage_with_bucket",
            tool_name="analyze_s3_usage_for_data_processing",
            input_params={
                "bucket_name": f"{s3_bucket}"
            },
            dependencies=["upload_file_to_s3"],
            validators=[
                ContainsTextValidator("Analyzing bucket")
            ],
            clean_ups=[]
        )
    ]
