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

from data_processing_mcp_server_tests.core.aws_setup import create_non_mcp_job, upload_script
from data_processing_mcp_server_tests.core.mcp_cleanup import DeleteAWSResources
from data_processing_mcp_server_tests.core.mcp_validators import (
    AWSBotoValidator,
    ContainsTextValidator,
)
from data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase
from typing import List


def glue_job_test_cases(s3_bucket, glue_role, aws_clients) -> List[MCPTestCase]:
    """Glue job test cases."""
    return [
        MCPTestCase(
            test_name='create_glue_job_basic',
            tool_name='manage_aws_glue_jobs',
            input_params={
                'operation': 'create-job',
                'job_name': 'mcp-test-job-basic',
                'job_definition': {
                    'Command': {
                        'Name': 'glueetl',
                        'ScriptLocation': f's3://{s3_bucket}/glue_job_script/mcp-test-script.py',
                    },
                    'Role': glue_role,
                    'GlueVersion': '5.0',
                    'MaxCapacity': 2,
                    'Description': 'Basic test job created by MCP server',
                },
            },
            aws_resources=[
                lambda s3_bucket=s3_bucket: upload_script(
                    'mcp-test-script.py', s3_bucket=s3_bucket, prefix='glue_job_script/'
                )
            ],
            dependencies=[],
            validators=[
                ContainsTextValidator('Successfully created Glue job'),
                AWSBotoValidator(
                    aws_clients['glue'],
                    operation='get_job',
                    operation_input_params={'job_name': 'mcp-test-job-basic'},
                    expected_keys=[
                        'job_definition.Command.Name',
                        'job_definition.Command.ScriptLocation',
                        'job_definition.Role',
                        'job_definition.GlueVersion',
                        'job_definition.MaxCapacity',
                    ],
                ),
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api='delete_job',
                    delete_params={'job_name': 'mcp-test-job-basic'},
                    boto_client=aws_clients['glue'],
                )
            ],
        ),
        MCPTestCase(
            test_name='create_glue_job_with_worker_config',
            tool_name='manage_aws_glue_jobs',
            input_params={
                'operation': 'create-job',
                'job_name': 'mcp-test-job-with-worker-config',
                'job_definition': {
                    'Command': {
                        'Name': 'glueetl',
                        'ScriptLocation': f's3://{s3_bucket}/glue_job_script/mcp-test-script.py',
                    },
                    'Role': glue_role,
                    'GlueVersion': '5.0',
                    'WorkerType': 'G.1X',
                    'NumberOfWorkers': 2,
                    'Description': 'Test job with worker config created by MCP server',
                },
            },
            aws_resources=[
                lambda s3_bucket=s3_bucket: upload_script(
                    'mcp-test-script.py', s3_bucket=s3_bucket, prefix='glue_job_script/'
                )
            ],
            dependencies=[],
            validators=[
                ContainsTextValidator('Successfully created Glue job'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_job',
                    operation_input_params={'job_name': 'mcp-test-job-with-worker-config'},
                    expected_keys=['job_definition.WorkerType', 'job_definition.NumberOfWorkers'],
                ),
            ],
            clean_ups=[
                DeleteAWSResources(
                    delete_api='delete_job',
                    delete_params={'job_name': 'mcp-test-job-with-worker-config'},
                    boto_client=aws_clients['glue'],
                )
            ],
        ),
        MCPTestCase(
            test_name='update_glue_job_definition',
            tool_name='manage_aws_glue_jobs',
            input_params={
                'operation': 'update-job',
                'job_name': 'mcp-test-job-basic',
                'job_definition': {
                    'Command': {
                        'Name': 'glueetl',
                        'ScriptLocation': f's3://{s3_bucket}/glue_job_script/mcp-test-script.py',
                    },
                    'Role': glue_role,
                    'GlueVersion': '5.0',
                    'WorkerType': 'G.1X',
                    'NumberOfWorkers': 4,
                    'Description': 'UPDATED: Basic test job created by MCP server',
                },
            },
            aws_resources=[],
            dependencies=['create_glue_job_basic'],
            validators=[
                ContainsTextValidator('Successfully updated MCP-managed job'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_job',
                    operation_input_params={'job_name': 'mcp-test-job-basic'},
                    expected_keys=['job_definition.Description'],
                ),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='update_non_mcp_job',
            tool_name='manage_aws_glue_jobs',
            input_params={
                'operation': 'update-job',
                'job_name': 'non-mcp-test-job',
                'job_definition': {'Role': glue_role, 'Description': 'UPDATED Should Fail'},
            },
            aws_resources=[lambda: create_non_mcp_job('non-mcp-test-job')],
            dependencies=[],
            validators=[ContainsTextValidator('missing required tags')],
        ),
        MCPTestCase(
            test_name='delete_glue_job',
            tool_name='manage_aws_glue_jobs',
            input_params={'operation': 'delete-job', 'job_name': 'mcp-test-job-basic'},
            dependencies=['create_glue_job_basic'],
            validators=[
                ContainsTextValidator('Successfully deleted MCP-managed Glue job'),
                AWSBotoValidator(
                    boto_client=aws_clients['glue'],
                    operation='get_job',
                    operation_input_params={'job_name': 'mcp-test-job-basic'},
                    validate_absence=True,
                ),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='delete_non_mcp_job',
            tool_name='manage_aws_glue_jobs',
            input_params={'operation': 'delete-job', 'job_name': 'non-mcp-test-job'},
            aws_resources=[lambda: create_non_mcp_job('non-mcp-test-job')],
            dependencies=[],
            validators=[ContainsTextValidator('missing required tags')],
            clean_ups=[
                DeleteAWSResources(
                    delete_api='delete_job',
                    delete_params={'job_name': 'non-mcp-test-job'},
                    boto_client=aws_clients['glue'],
                )  # only delete this non-mcp job here since it take too long to delete it
            ],
        ),
        MCPTestCase(
            test_name='delete_mcp_job_does_not_exist',
            tool_name='manage_aws_glue_jobs',
            input_params={'operation': 'delete-job', 'job_name': 'job-name-does-not-exist'},
            dependencies=[],
            validators=[ContainsTextValidator('not found')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_job_basic',
            tool_name='manage_aws_glue_jobs',
            input_params={'operation': 'get-job', 'job_name': 'mcp-test-job-basic'},
            dependencies=['create_glue_job_basic'],
            validators=[ContainsTextValidator('Successfully retrieved job')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_job_missing_job_name',
            tool_name='manage_aws_glue_jobs',
            input_params={'operation': 'get-job'},
            dependencies=[],
            validators=[
                ContainsTextValidator(
                    'Error executing tool manage_aws_glue_jobs: job_name is required for get-job operation'
                )
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_job_not_exist',
            tool_name='manage_aws_glue_jobs',
            input_params={'operation': 'get-job', 'job_name': 'job-name-does-not-exist'},
            dependencies=[],
            validators=[ContainsTextValidator('not found')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_jobs_all',
            tool_name='manage_aws_glue_jobs',
            input_params={'operation': 'get-jobs', 'max_results': 10},
            dependencies=['create_glue_job_basic', 'create_glue_job_with_worker_config'],
            validators=[
                ContainsTextValidator(
                    'Successfully retrieved jobs', expected_count=2
                )  # 2 created job
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='start_job_run_basic',
            tool_name='manage_aws_glue_jobs',
            input_params={'operation': 'start-job-run', 'job_name': 'mcp-test-job-basic'},
            dependencies=['create_glue_job_basic'],
            validators=[ContainsTextValidator('Successfully started job run')],
            clean_ups=[
                DeleteAWSResources(
                    delete_api='batch_stop_job_run',
                    boto_client=aws_clients['glue'],
                    delete_params={'JobName': 'mcp-test-job-basic'},
                    resource_field='job_run_id',
                    target_param_key='JobRunIds',
                    param_is_list=True,
                )
            ],
        ),
        MCPTestCase(
            test_name='start_job_run_missing_job_name',
            tool_name='manage_aws_glue_jobs',
            input_params={'operation': 'start-job-run'},
            dependencies=[],
            validators=[
                ContainsTextValidator(
                    'Error executing tool manage_aws_glue_jobs: job_name is required'
                ),
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='start_job_run_not_exist',
            tool_name='manage_aws_glue_jobs',
            input_params={'operation': 'start-job-run', 'job_name': 'job-name-does-not-exist'},
            dependencies=[],
            validators=[ContainsTextValidator('not found')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_job_run_missing_job_name',
            tool_name='manage_aws_glue_jobs',
            input_params={
                'operation': 'get-job-run',
                'job_run_id': 'some-job-run-id',  # job_name is required
            },
            dependencies=[],
            validators=[
                ContainsTextValidator(
                    'Error executing tool manage_aws_glue_jobs: job_name and job_run_id are required for get-job-run operation'
                )
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_job_run_missing_job_run_id',
            tool_name='manage_aws_glue_jobs',
            input_params={
                'operation': 'get-job-run',
                'job_name': 'mcp-test-job-basic',  # job_run_id is required
            },
            dependencies=[],
            validators=[
                ContainsTextValidator(
                    'Error executing tool manage_aws_glue_jobs: job_name and job_run_id are required for get-job-run operation'
                )
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_job_runs_all',
            tool_name='manage_aws_glue_jobs',
            input_params={'operation': 'get-job-runs', 'job_name': 'mcp-test-job-basic'},
            dependencies=['start_job_run_basic'],
            validators=[ContainsTextValidator('Successfully retrieved job runs')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='get_job_run_not_exist',
            tool_name='manage_aws_glue_jobs',
            input_params={
                'operation': 'get-job-run',
                'job_name': 'mcp-test-job-basic',
                'job_run_id': 'job-run-id-does-not-exist',
            },
            dependencies=['create_glue_job_basic'],
            validators=[ContainsTextValidator('not found')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='stop_job_run_basic',
            tool_name='manage_aws_glue_jobs',
            input_params={
                'operation': 'stop-job-run',
                'job_name': 'mcp-test-job-basic',
                'job_run_id': '{{start_job_run_basic.result.content[0].text.job_run_id}}',
            },
            dependencies=['start_job_run_basic'],
            validators=[ContainsTextValidator('Successfully stopped job run')],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='stop_job_run_missing_job_name',
            tool_name='manage_aws_glue_jobs',
            input_params={
                'operation': 'stop-job-run',
                'job_run_id': 'job-run-id-does-not-exist',  # job_name is required
            },
            dependencies=[],
            validators=[
                ContainsTextValidator(
                    'Error executing tool manage_aws_glue_jobs: job_name and job_run_id are required for stop-job-run operation'
                )
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='stop_job_run_missing_job_run_id',
            tool_name='manage_aws_glue_jobs',
            input_params={'operation': 'stop-job-run', 'job_name': 'mcp-test-job-basic'},
            dependencies=[],
            validators=[
                ContainsTextValidator(
                    'Error executing tool manage_aws_glue_jobs: job_name and job_run_id are required for stop-job-run operation'
                )
            ],
            clean_ups=[],
        ),
        MCPTestCase(
            test_name='batch_stop_job_run',
            tool_name='manage_aws_glue_jobs',
            input_params={
                'operation': 'batch-stop-job-run',
                'job_name': 'mcp-test-job-basic',
                'job_run_ids': ['{{start_job_run_basic.result.content[0].text.job_run_id}}'],
            },
            dependencies=['start_job_run_basic'],
            validators=[
                ContainsTextValidator('Successfully processed batch stop job run request')
            ],
            clean_ups=[],
        ),
    ]
