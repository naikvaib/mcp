#!/usr/bin/env python3
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

"""Test runner using JSON test case configuration."""

import boto3
import json
import os
import tempfile
import uuid
from src.data_processing_mcp_server_tests.core.aws_setup import AWSSetup
from src.data_processing_mcp_server_tests.core.mcp_client import MCPClient
from src.data_processing_mcp_server_tests.core.mcp_server import MCPServerManager
from src.data_processing_mcp_server_tests.core.reporting import ReportGenerator
from src.data_processing_mcp_server_tests.core.test_executor import TestExecutor
from src.data_processing_mcp_server_tests.core.validators import (
    get_operation_validators,
)
from src.data_processing_mcp_server_tests.models.test_case import MCPTestCase


# Global AWS setup - use environment variable for region if available
aws_setup = AWSSetup(profile_name=None, region=os.environ.get('AWS_REGION', 'us-east-1'))


def ensure_unique_job_name(test_case):
    """Ensure job has a unique name to avoid conflicts."""
    if 'job_name' in test_case.input_params:
        unique_suffix = uuid.uuid4().hex[:8]
        original_name = test_case.input_params['job_name']
        test_case.input_params['job_name'] = f'{original_name}-{unique_suffix}'
        print(f'Using unique job name: {test_case.input_params["job_name"]}')


def create_non_mcp_job_if_needed(test_case, operation_type):
    """Create a non-MCP managed job for negative test cases if needed."""
    if test_case.test_name.lower().startswith('negative'):
        # Import the create_non_mcp_job function from our helper
        from tests.glue.create_non_mcp_job import create_non_mcp_job

        # Check if job exists first
        job_name = test_case.input_params.get('job_name')
        exists, _ = aws_setup.check_job_exists(job_name)

        if not exists:
            print(f'Creating non-MCP job {job_name} for negative {operation_type} test...')
            aws_region = os.environ.get('AWS_REGION', 'us-east-1')
            job_name, role_name = create_non_mcp_job(None, aws_region, job_name)

            # Store role name for cleanup
            test_case._role_name = role_name
            print(f'Created non-MCP job: {job_name} with role: {role_name}')

            # Wait a bit for AWS to propagate the job
            import time

            time.sleep(5)

        return True
    return False


def handle_placeholder_fields(test_case):
    """Replace PLACEHOLDER fields in job definition with actual values."""
    if 'job_definition' in test_case.input_params:
        # Handle PLACEHOLDER Role
        if (
            'Role' in test_case.input_params['job_definition']
            and test_case.input_params['job_definition']['Role'] == 'PLACEHOLDER'
        ):
            role_name, role_arn = aws_setup.setup_test_role(
                'mcp-test-role-for-glue', ['AWSGlueServiceRole']
            )
            test_case.input_params['job_definition']['Role'] = role_arn

        # Handle PLACEHOLDER ScriptLocation
        if 'Command' in test_case.input_params['job_definition']:
            cmd = test_case.input_params['job_definition']['Command']
            if 'ScriptLocation' in cmd and cmd['ScriptLocation'] == 'PLACEHOLDER':
                # Upload default test script if placeholder is used
                setup_s3_script(test_case)


def create_mcp_job_if_needed(test_case, job_name):
    """Create an MCP-managed job if it doesn't exist."""
    # Check if job exists
    exists, _ = aws_setup.check_job_exists(job_name)
    if not exists:
        client = MCPClient(test_case._mcp_client.server_manager)

        # Create job using create-job operation
        create_params = {
            'operation': 'create-job',
            'job_name': job_name,
            'job_definition': {
                'GlueVersion': '4.0',
                'Command': {
                    'Name': 'glueetl',
                    'ScriptLocation': 's3://mcp-test-script-s3/mcp-test-script.py',
                },
                'MaxRetries': 0,
                'Description': 'Job created for test',
            },
        }

        # Setup role for the create operation
        role_name, role_arn = aws_setup.setup_test_role(
            'mcp-test-role-for-glue', ['AWSGlueServiceRole']
        )
        create_params['job_definition']['Role'] = role_arn

        # Create the job
        print(f'Creating job {job_name} for test...')
        response = client.call_tool('manage_aws_glue_jobs', create_params)
        if response.get('isError'):
            raise Exception(f'Failed to create job for test: {response}')


# Setup functions for each operation type
def setup_create_job(test_case):
    """Setup function for create-job operation."""
    # Ensure job has a unique name
    ensure_unique_job_name(test_case)

    # Handle placeholder fields
    handle_placeholder_fields(test_case)


def setup_update_job(test_case):
    """Setup function for update-job operation."""
    # For update tests, we first need to ensure the job exists and is managed by MCP

    # Ensure job has a unique name
    ensure_unique_job_name(test_case)
    job_name = test_case.input_params.get('job_name')

    # Handle placeholder fields
    handle_placeholder_fields(test_case)

    # For negative test cases, ensure the job exists (but not managed by MCP)
    if create_non_mcp_job_if_needed(test_case, 'update'):
        return

    # For positive test cases, create an MCP-managed job if needed
    create_mcp_job_if_needed(test_case, job_name)


def setup_delete_job(test_case):
    """Setup function for delete-job operation."""
    # Similar to update, for delete tests we need to ensure the job exists

    # Ensure job has a unique name
    ensure_unique_job_name(test_case)
    job_name = test_case.input_params.get('job_name')

    # For negative test cases, ensure the job exists (but not managed by MCP)
    if create_non_mcp_job_if_needed(test_case, 'delete'):
        return

    # For positive test cases, create an MCP-managed job if needed
    create_mcp_job_if_needed(test_case, job_name)


def setup_s3_script(test_case):
    """Custom setup function to upload script to S3 bucket."""
    # Use a simple test script that just prints a message
    script_content = """
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

print("Test script for MCP server testing")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("test_job", {})

print("Job completed successfully")
job.commit()
"""

    # Write script to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as temp:
        temp.write(script_content)
        local_script_path = temp.name

    bucket_name = 'mcp-test-script-s3'
    s3_key = f'mcp-test-script-{uuid.uuid4().hex[:8]}.py'

    # Ensure bucket exists
    if not aws_setup.check_bucket_exists(bucket_name):
        print(f'Creating S3 bucket: {bucket_name}')
        s3 = boto3.client('s3', region_name=aws_setup.region)
        s3.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': aws_setup.region}
        )

    s3_uri = aws_setup.upload_script_to_s3(local_script_path, bucket_name, s3_key)
    test_case.input_params['job_definition']['Command']['ScriptLocation'] = s3_uri
    print(f'Uploaded script to: {s3_uri}')

    # Clean up temporary file
    os.unlink(local_script_path)


# Cleanup functions
def cleanup_create_job(test_case):
    """Cleanup function for create-job operation."""
    job_name = test_case.input_params.get('job_name')
    if job_name:
        print(f'Cleaning up job: {job_name}')
        aws_setup.delete_glue_job(job_name)

    # Cleanup IAM role if it exists for non-MCP jobs
    if hasattr(test_case, '_role_name') and test_case._role_name:
        print(f'Cleaning up IAM role: {test_case._role_name}')
        aws_setup.delete_iam_role(test_case._role_name, ['AWSGlueServiceRole'])


def cleanup_update_job(test_case):
    """Cleanup function for update-job operation."""
    job_name = test_case.input_params.get('job_name')
    if job_name:
        print(f'Cleaning up job: {job_name}')
        aws_setup.delete_glue_job(job_name)


def cleanup_delete_job(test_case):
    """Cleanup function for delete-job operation."""
    # No cleanup needed for delete-job as the job should already be deleted
    # But we'll double-check and delete if it still exists
    job_name = test_case.input_params.get('job_name')
    if job_name:
        exists, _ = aws_setup.check_job_exists(job_name)
        if exists:
            print(f'Job {job_name} still exists after delete test, cleaning up')
            aws_setup.delete_glue_job(job_name)


def main():
    """Execute the test runner with configured test cases from JSON files.

    Sets up the MCP server, loads and groups test cases by operation,
    runs tests with appropriate validators, generates reports, and
    performs cleanup operations.
    """
    # Get server path and AWS region from environment variables if available
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    default_path = os.path.abspath(os.path.join(current_dir, "../../awslabs/aws_dataprocessing_mcp_server"))
    server_path = os.environ.get("MCP_SERVER_PATH", default_path)
    
    aws_region = os.environ.get('AWS_REGION', 'us-east-1')

    server_manager = MCPServerManager(
        server_path, aws_profile=None, aws_region=aws_region, server_args='--allow-write'
    )

    try:
        server_manager.start()
        client = MCPClient(server_manager)
        client.initialize()

        # Load test cases from JSON and group by operation
        with open('tests/glue/test_cases.json', 'r') as f:
            test_data = json.load(f)

        # Group test cases by operation
        operations = {}
        for test_dict in test_data:
            test_case = MCPTestCase(
                test_name=test_dict['test_name'],
                tool_name=test_dict['tool_name'],
                input_params=test_dict['input_params'],
            )
            operation = test_case.input_params.get('operation')
            if operation not in operations:
                operations[operation] = []
            operations[operation].append(test_case)

        # Generate final reports
        report_formats = ['markdown', 'json', 'html']
        report_dir = 'test_reports'

        # Create separate executors for each operation type
        all_results = []
        all_executors = []  # Keep track of all executors for later cleanup

        for operation, test_cases in operations.items():
            executor = TestExecutor(client)
            all_executors.append(executor)

            # Store reference to MCP client for setup functions
            for test_case in test_cases:
                test_case._mcp_client = client

            # Add operation-specific validators
            validators = get_operation_validators(operation)
            for validator in validators:
                executor.add_validator(validator)

            # Add test cases with appropriate setup/cleanup functions based on operation type
            for test_case in test_cases:
                if operation == 'create-job':
                    executor.add_test_case(test_case, setup_create_job, cleanup_create_job)
                elif operation == 'update-job':
                    executor.add_test_case(test_case, setup_update_job, cleanup_update_job)
                elif operation == 'delete-job':
                    executor.add_test_case(test_case, setup_delete_job, cleanup_delete_job)
                else:
                    # For other operations, use no setup/cleanup
                    executor.add_test_case(test_case, None, None)

            # Run tests for this operation - but don't clean up yet
            result_info = executor.run_tests_only(report_dir=report_dir)
            all_results.extend(result_info)

        # Now run all cleanups after all tests have completed
        print('\n=== Running All Cleanups ===')
        cleanup_results = []
        for executor in all_executors:
            cleanup_results.extend(executor.run_all_cleanups())

        # Generate reports after all tests and cleanups are done
        report_generator = ReportGenerator(output_dir=report_dir)
        report_paths = report_generator.generate_report(all_results, formats=report_formats)

        # Print summary
        passed = sum(1 for r in all_results if r.success)
        total = len(all_results)
        print('\n=== Final Summary ===')
        print(f'Total: {total}, Passed: {passed}, Failed: {total - passed}')
        print(f'Success Rate: {passed / total * 100:.1f}%' if total > 0 else 'No tests run')

        for fmt, path in report_paths.items():
            print(f'{fmt.upper()} Report: {path}')

        # Print cleanup summary
        print('\n=== Cleanup Summary ===')
        for name, success, error in cleanup_results:
            status = 'SUCCESS' if success else f'FAILED: {error}'
            print(f'Cleanup for {name}: {status}')

    finally:
        server_manager.stop()

    import sys

    # Exit with code 1 if any test failed, so GitHub Actions detects failure
    if passed < total:
        print('❌ One or more tests failed. Exiting with status code 1.')
        sys.exit(1)
    else:
        print('✅ All tests passed. Exiting with status code 0.')
        sys.exit(0)


if __name__ == '__main__':
    main()
