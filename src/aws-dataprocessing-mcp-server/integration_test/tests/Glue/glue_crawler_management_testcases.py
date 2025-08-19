from typing import List

from data_processing_mcp_server_tests.core.mcp_validators import ContainsTextValidator, AWSBotoValidator
from data_processing_mcp_server_tests.core.mcp_cleanup import DeleteAWSResources
from data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase


def glue_crawler_management_test_cases(aws_clients) -> List[MCPTestCase]:
    return [
        MCPTestCase(
            test_name="get_crawler_metrics",
            tool_name="manage_aws_glue_crawler_management",
            input_params={
                "operation": "get-crawler-metrics",
                "crawler_name": "mcp_test_crawler"
            },
            dependencies=["create_glue_crawler_basic"],
            validators=[
                ContainsTextValidator("Successfully retrieved crawler metrics")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="start-crawler-schedule",
            tool_name="manage_aws_glue_crawler_management",
            input_params={
                "operation": "start-crawler-schedule",
                "crawler_name": "mcp_test_crawler_with_schedule"
            },
            dependencies=["create_crawler_with_schedule"],
            validators=[
                ContainsTextValidator("Successfully started schedule"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_crawler",
                    operation_input_params={"Name": "mcp_test_crawler_with_schedule"},
                    expected_keys=["crawler_name"]
                )
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="start-crawler-schedule_missing_name",
            tool_name="manage_aws_glue_crawler_management",
            input_params={
                "operation": "start-crawler-schedule"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("crawler_name is required for start-crawler-schedule operation")
            ],
            clean_ups=[]
        ),

        MCPTestCase(
            test_name="stop-crawler-schedule_missing_name",
            tool_name="manage_aws_glue_crawler_management",
            input_params={
                "operation": "stop-crawler-schedule"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("crawler_name is required for stop-crawler-schedule operation")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="update-crawler-schedule",
            tool_name="manage_aws_glue_crawler_management",
            input_params={
                "operation": "update-crawler-schedule",
                "crawler_name": "mcp_test_crawler_with_schedule",
                "schedule": "cron(0 18 * * ? *)"
            },
            dependencies=["create_crawler_with_schedule"],
            validators=[
                ContainsTextValidator("Successfully updated schedule for crawler"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_crawler",
                    operation_input_params={"Name": "mcp_test_crawler_with_schedule"},
                    expected_keys=["crawler_name"]
                )
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="update-crawler-schedule_missing_name",
            tool_name="manage_aws_glue_crawler_management",
            input_params={
                "operation": "update-crawler-schedule",
                "schedule": "cron(0 18 * * ? *)"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("crawler_name and schedule are required for update-crawler-schedule operation")
            ],
            clean_ups=[]
        )
    ]
