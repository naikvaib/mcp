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

import boto3
import os
import pytest
from botocore.config import Config
from collections import OrderedDict
from data_processing_mcp_server_tests.core.aws_setup import (
    create_s3_bucket_if_not_exists,
    get_or_create_glue_role,
)
from data_processing_mcp_server_tests.core.mcp_executor import Executor
from data_processing_mcp_server_tests.models.mcp_test_case import MCPTestCase
from inspect import getmodule
from tests.Athena.athena_catalog_testcases import athena_data_catalog_test_cases
from tests.Athena.athena_database_table_testcases import athena_databse_table_test_cases
from tests.Athena.athena_named_query_testcases import athena_named_query_test_cases
from tests.Athena.athena_query_execution_testcases import athena_query_execution_test_cases
from tests.Athena.athena_workgroup_testcases import athena_workgroup_test_cases
from tests.EMR.emr_cluster_testcases import emr_cluster_test_cases
from tests.EMR.emr_instance_testcases import emr_instance_test_cases
from tests.EMR.emr_step_testcases import emr_step_test_cases
from tests.Glue.glue_catalog_testcases import glue_catalogs_test_cases
from tests.Glue.glue_classifier_testcases import glue_classifiers_test_cases
from tests.Glue.glue_connection_testcase import glue_connections_test_cases
from tests.Glue.glue_crawler_management_testcases import glue_crawler_management_test_cases
from tests.Glue.glue_crawler_testcases import glue_crawlers_test_cases
from tests.Glue.glue_database_testcase import glue_database_test_cases
from tests.Glue.glue_encrption_testcases import glue_encryption_test_cases
from tests.Glue.glue_job_testcase import glue_job_test_cases
from tests.Glue.glue_partition_testcase import glue_partitions_test_cases
from tests.Glue.glue_profile_testcases import glue_profiles_test_cases
from tests.Glue.glue_security_config_testcases import glue_security_configurations_test_cases
from tests.Glue.glue_session_testcases import glue_sessions_test_cases
from tests.Glue.glue_table_testcase import glue_table_test_cases
from tests.Glue.glue_trigger_testcases import glue_triggers_test_cases
from tests.Glue.glue_workflow_testcases import glue_workflows_test_cases
from tests.IAM.iam_testcases import iam_test_cases
from tests.S3.s3_testcases import s3_test_cases
from typing import List, Tuple


def load_all_grouped_test_cases(
    s3_bucket, glue_role, aws_clients
) -> List[Tuple[str, List[MCPTestCase]]]:
    """Load and group test cases by their originating test case function (not tool_name)."""
    grouped_functions = OrderedDict(
        [
            (
                'glue_jobs',
                glue_job_test_cases(s3_bucket, glue_role, aws_clients)
                + glue_triggers_test_cases(s3_bucket, glue_role, aws_clients)
                + glue_workflows_test_cases(aws_clients),
            ),
            (
                'glue_database_table',
                glue_database_test_cases(s3_bucket, aws_clients)
                + glue_table_test_cases(s3_bucket, aws_clients)
                + glue_partitions_test_cases(s3_bucket, aws_clients),
            ),
            ('glue_connections', glue_connections_test_cases(aws_clients)),
            ('glue_catalogs', glue_catalogs_test_cases(aws_clients)),
            (
                'glue_crawlers',
                glue_crawlers_test_cases(s3_bucket, glue_role, aws_clients)
                + glue_crawler_management_test_cases(aws_clients),
            ),
            ('glue_encryption', glue_encryption_test_cases(aws_clients)),
            ('glue_sessions', glue_sessions_test_cases(glue_role, aws_clients)),
            ('glue_classifiers', glue_classifiers_test_cases(aws_clients)),
            ('glue_profiles', glue_profiles_test_cases(aws_clients)),
            ('glue_security_configurations', glue_security_configurations_test_cases(aws_clients)),
            ('iam', iam_test_cases(glue_role, aws_clients)),
            ('s3', s3_test_cases(s3_bucket, aws_clients)),
            (
                'emr',
                emr_cluster_test_cases(aws_clients)
                + emr_step_test_cases(s3_bucket, aws_clients)
                + emr_instance_test_cases(aws_clients),
            ),
            (
                'athena_query',
                athena_named_query_test_cases(aws_clients)
                + athena_query_execution_test_cases(s3_bucket, aws_clients),
            ),
            (
                'athena_data_catalog_db_table',
                athena_data_catalog_test_cases(aws_clients)
                + athena_databse_table_test_cases(s3_bucket, aws_clients),
            ),
            ('athena_workgroup', athena_workgroup_test_cases(s3_bucket, aws_clients)),
        ]
    )

    return list(grouped_functions.items())


@pytest.fixture(scope='session')
def test_executor(test_cases, mcp_env):
    """Create and execute test executor."""
    executor = Executor(test_cases, mcp_env)
    execution_results = executor.execute_tests()
    return execution_results


@pytest.fixture(scope='session')
def test_results(test_executor):
    """Extract test results from executor."""
    return test_executor['success_map']


@pytest.fixture(scope='session')
def test_details(test_executor):
    """Extract test details from executor."""
    return {result['test_name']: result for result in test_executor['results']}


@pytest.fixture(scope='session')
def aws_clients():
    """Create AWS service clients."""
    config = Config(user_agent_extra='awslabs/mcp/aws-dataprocessing-mcp-server-test-framework/')
    session = boto3.Session(
        profile_name=os.environ.get('AWS_PROFILE'), region_name=os.environ.get('AWS_REGION')
    )
    services = ['glue', 'iam', 'emr', 's3', 'athena', 'sts']
    return {service: session.client(service, config=config) for service in services}


@pytest.fixture(scope='session')
def glue_role():
    """Get or create Glue service role."""
    return get_or_create_glue_role()


@pytest.fixture(scope='session')
def s3_bucket():
    """Create S3 bucket for testing."""
    return create_s3_bucket_if_not_exists()


@pytest.fixture(scope='session')
def test_case_groups(s3_bucket, glue_role, aws_clients):
    """Load all test case groups."""
    return load_all_grouped_test_cases(s3_bucket, glue_role, aws_clients)


@pytest.fixture
def group(request, test_case_groups):
    """Get test case group by index."""
    return test_case_groups[request.param]


def pytest_generate_tests(metafunc):
    """Generate test parameters for group fixture."""
    if 'group' in metafunc.fixturenames:
        module = getmodule(metafunc.function)
        test_case_groups = module.test_case_groups.__wrapped__(
            s3_bucket=module.s3_bucket.__wrapped__(),
            glue_role=module.glue_role.__wrapped__(),
            aws_clients=module.aws_clients.__wrapped__(),
        )
        indices = list(range(len(test_case_groups)))
        ids = [name for name, _ in test_case_groups]
        metafunc.parametrize('group', indices, indirect=True, ids=ids)


def test_tool_group(group, mcp_env):
    """Execute a group of test cases and validate results."""
    tool_name, test_cases = group
    executor = Executor(test_cases, mcp_env)
    execution_results = executor.execute_tests()

    failed_messages = []

    for result in execution_results['results']:
        test_name = result['test_name']
        validations = result.get('validations', [])

        for v in validations:
            if not v.success:
                msg = f'[{tool_name}] {test_name} failed: {v.error_message}'
                failed_messages.append(msg)

    if failed_messages:
        full_error_message = '\n'.join(failed_messages)
        pytest.fail(f'Some test cases failed:\n{full_error_message}')
