# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from data_processing_mcp_server_tests.core.mcp_cleanup import DeleteAWSResources
from data_processing_mcp_server_tests.core.mcp_validators import (
    AWSBotoValidator,
    ContainsTextValidator,
)
from data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase
from typing import List


def glue_profiles_test_cases(aws_clients) -> List[MCPTestCase]:
    """Glue profile test cases."""
    return [
        MCPTestCase(
            test_name='create_glue_profile_basic',
            tool_name='manage_aws_glue_usage_profiles',
            input_params={
                'operation': 'create-profile',
                'profile_name': 'mcp_test_profile',
                'configuration': {
                    'SessionConfiguration': {'Timeout': {'DefaultValue': '300'}},
                    'JobConfiguration': {'MaxRetries': {'DefaultValue': '3'}},
                },
            },
            dependencies=[],
            validators=[
                ContainsTextValidator('Successfully created usage profile mcp_test_profile'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_usage_profile',
                    operation_input_params={'Name': 'mcp_test_profile'},
                    expected_keys=['profile_name', 'configuration'],
                ),
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api='delete_usage_profile',
                    delete_params={'Name': 'mcp_test_profile'},
                    boto_client=aws_clients['glue'],
                )
            ],
        ),
        MCPTestCase(
            test_name='create_glue_profile_not_check_configuration',
            tool_name='manage_aws_glue_usage_profiles',
            input_params={
                'operation': 'create-profile',
                'profile_name': 'mcp_test_profile_no_check',
                'configuration': {
                    'SessionConfiguration': {'Timeout': {'DefaultValue': '300'}},
                    'JobConfiguration': {'MaxRetries': {'DefaultValue': '3'}},
                },
            },
            dependencies=[],
            validators=[
                ContainsTextValidator(
                    'Successfully created usage profile mcp_test_profile_no_check'
                ),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_usage_profile',
                    operation_input_params={'Name': 'mcp_test_profile_no_check'},
                    expected_keys=['profile_name'],
                ),
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api='delete_usage_profile',
                    delete_params={'Name': 'mcp_test_profile_no_check'},
                    boto_client=aws_clients['glue'],
                )
            ],
        ),
        MCPTestCase(
            test_name='get_glue_profile_not_check_configuration',
            tool_name='manage_aws_glue_usage_profiles',
            input_params={'operation': 'get-profile', 'profile_name': 'mcp_test_profile_no_check'},
            dependencies=['create_glue_profile_not_check_configuration'],
            validators=[ContainsTextValidator('Successfully retrieved usage profile')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_glue_profile_not_exist',
            tool_name='manage_aws_glue_usage_profiles',
            input_params={'operation': 'get-profile', 'profile_name': 'non_existent_profile'},
            dependencies=[],
            validators=[
                ContainsTextValidator('UsageProfile non_existent_profile does not exists')
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_glue_profile_missing_name',
            tool_name='manage_aws_glue_usage_profiles',
            input_params={'operation': 'get-profile'},
            dependencies=[],
            validators=[ContainsTextValidator('Field required')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='update_glue_profile_not_check_configuration',
            tool_name='manage_aws_glue_usage_profiles',
            input_params={
                'operation': 'update-profile',
                'profile_name': 'mcp_test_profile_no_check',
                'configuration': {
                    'SessionConfiguration': {'Timeout': {'DefaultValue': '600'}},
                    'JobConfiguration': {'MaxRetries': {'DefaultValue': '5'}},
                },
            },
            dependencies=['create_glue_profile_not_check_configuration'],
            validators=[
                ContainsTextValidator('Successfully updated usage profile'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_usage_profile',
                    operation_input_params={'Name': 'mcp_test_profile_no_check'},
                    expected_keys=['profile_name', 'configuration'],
                ),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='update_glue_profile_missing_name',
            tool_name='manage_aws_glue_usage_profiles',
            input_params={
                'operation': 'update-profile',
                'configuration': {
                    'SessionConfiguration': {'Timeout': {'DefaultValue': '600'}},
                    'JobConfiguration': {'MaxRetries': {'DefaultValue': '5'}},
                },
            },
            dependencies=[],
            validators=[ContainsTextValidator('Field required')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_glue_profile_not_check_configuration',
            tool_name='manage_aws_glue_usage_profiles',
            input_params={
                'operation': 'delete-profile',
                'profile_name': 'mcp_test_profile_no_check',
            },
            dependencies=['create_glue_profile_not_check_configuration'],
            validators=[
                ContainsTextValidator('Successfully deleted usage profile'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_usage_profile',
                    operation_input_params={'Name': 'mcp_test_profile_no_check'},
                    validate_absence=True,
                ),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_glue_profile_not_exist',
            tool_name='manage_aws_glue_usage_profiles',
            input_params={'operation': 'delete-profile', 'profile_name': 'non_existent_profile'},
            dependencies=[],
            validators=[ContainsTextValidator('Usage profile non_existent_profile not found')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_glue_profile_missing_name',
            tool_name='manage_aws_glue_usage_profiles',
            input_params={'operation': 'delete-profile'},
            dependencies=[],
            validators=[ContainsTextValidator('Field required')],
            clean_ups=[],
        ),
    ]
