from typing import List

from data_processing_mcp_server_tests.core.mcp_validators import ContainsTextValidator, AWSBotoValidator
from data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase
from data_processing_mcp_server_tests.core.mcp_cleanup import DeleteAWSResources

def glue_classifiers_test_cases(aws_clients) -> List[MCPTestCase]:
    return [
        MCPTestCase(
            test_name="create_glue_classifier_basic",
            tool_name="manage_aws_glue_classifiers",
            input_params={
                "operation": "create-classifier",
                "classifier_name": "mcp_test_classifier",
                "classifier_definition": {
                    "GrokClassifier": {
                        "Name": "mcp_test_grok_classifier",
                        "GrokPattern": "%{COMBINEDAPACHELOG}",
                        "Classification": "apache_log"
                    }
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully created classifier"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_classifier",
                    operation_input_params={"classifier_name": "mcp_test_grok_classifier"},
                    expected_keys=["Name", "GrokClassifier.GrokPattern", "GrokClassifier.Classification"]
                )
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api="delete_classifier",
                    delete_params={"Name": "mcp_test_grok_classifier"},
                    boto_client=aws_clients["glue"]
                )
            ]
        ),
        MCPTestCase(
            test_name="get_glue_classifier_basic",
            tool_name="manage_aws_glue_classifiers",
            input_params={
                "operation": "get-classifier",
                "classifier_name": "mcp_test_grok_classifier"
            },
            dependencies=["create_glue_classifier_basic"],
            validators=[
                ContainsTextValidator("Successfully retrieved classifier")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="create_glue_classifier_missing_name",
            tool_name="manage_aws_glue_classifiers",
            input_params={
                "operation": "create-classifier",
                "classifier_definition": {
                    "GrokClassifier": {
                        "GrokPattern": "%{COMBINEDAPACHELOG}",
                        "Classification": "apache_log"
                    }
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Missing required parameter")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="get_glue_classifiers",
            tool_name="manage_aws_glue_classifiers",
            input_params={
                "operation": "get-classifiers",
                "max_results": 10
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Successfully retrieved classifiers")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="get_glue_classifier_not_exist",
            tool_name="manage_aws_glue_classifiers",
            input_params={
                "operation": "get-classifier",
                "classifier_name": "non_existent_classifier"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("not found")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="update_glue_classifier",
            tool_name="manage_aws_glue_classifiers",
            input_params={
                "operation": "update-classifier",
                "classifier_name": "mcp_test_grok_classifier",
                "classifier_definition": {
                    "GrokClassifier": {
                        "Name": "mcp_test_grok_classifier",
                        "GrokPattern": "%{COMBINEDAPACHELOG}",
                        "Classification": "apache_log_updated"
                    }
                }
            },
            dependencies=["create_glue_classifier_basic"],
            validators=[
                ContainsTextValidator("Successfully updated classifier"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_classifier",
                    operation_input_params={"classifier_name": "mcp_test_grok_classifier"},
                    expected_keys=["Name", "GrokClassifier.GrokPattern", "GrokClassifier.Classification"]
                )
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="update_glue_classifier_missing_name",
            tool_name="manage_aws_glue_classifiers",
            input_params={
                "operation": "update-classifier",
                "classifier_definition": {
                    "GrokClassifier": {
                        "GrokPattern": "%{COMBINEDAPACHELOG}",
                        "Classification": "apache_log_updated"
                    }
                }
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("Missing required parameter ")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="delete_glue_classifier",
            tool_name="manage_aws_glue_classifiers",
            input_params={
                "operation": "delete-classifier",
                "classifier_name": "mcp_test_grok_classifier"
            },
            dependencies=["create_glue_classifier_basic"],
            validators=[
                ContainsTextValidator("Successfully deleted classifier"),
                AWSBotoValidator(
                    boto_client=aws_clients["glue"],
                    operation="get_classifier",
                    operation_input_params={"classifier_name": "mcp_test_grok_classifier"},
                    validate_absence=True
                )
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="delete_glue_classifier_not_exist",
            tool_name="manage_aws_glue_classifiers",
            input_params={
                "operation": "delete-classifier",
                "classifier_name": "non_existent_classifier"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("not found")
            ],
            clean_ups=[]
        ),
        MCPTestCase(
            test_name="delete_glue_classifier_missing_name",
            tool_name="manage_aws_glue_classifiers",
            input_params={
                "operation": "delete-classifier"
            },
            dependencies=[],
            validators=[
                ContainsTextValidator("classifier_name is required for delete-classifier operation")
            ],
            clean_ups=[]
        )
    ]
