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

"""AWS setup utilities for test execution."""

import boto3
import json
import os
import time
import uuid
from botocore.exceptions import ProfileNotFound
from typing import Any, Dict, List, Optional, Tuple


class AWSSetup:
    """Utility class for AWS resource setup and management."""

    def __init__(self, profile_name: Optional[str] = None, region: Optional[str] = None):
        """Initialize AWS setup with profile and region.

        Args:
            profile_name: AWS profile name to use
            region: AWS region name
        """
        try:
            # Try to use the specified profile first
            session = boto3.Session(profile_name=profile_name, region_name=region)
        except ProfileNotFound:
            # Fall back to environment variables or default profile if the specified profile doesn't exist
            print(
                f"AWS profile '{profile_name}' not found. Falling back to environment variables or default profile."
            )
            session = boto3.Session(region_name=region)

        self.glue = session.client('glue')
        self.iam = session.client('iam')
        self.s3 = session.client('s3')
        self.athena = session.client('athena')
        self.emr = session.client('emr')
        self.cloudwatch = session.client('cloudwatch')
        self.logs = session.client('logs')
        self.region = region or session.region_name
        self.profile = profile_name

    def setup_test_role(
        self, role_name_prefix: str, policies: Optional[List[str]] = None
    ) -> Tuple[str, str]:
        """Create or reuse a test IAM role.

        Args:
            role_name_prefix: Prefix for the role name
            policies: List of AWS managed policy names to attach

        Returns:
            Tuple of (role_name, role_arn)
        """
        # Check for existing role with matching policies
        existing_role = self._find_existing_role(role_name_prefix, policies or [])
        if existing_role:
            print(f'Reusing existing IAM role: {existing_role[0]}')
            return existing_role

        # Create a unique role name
        role_name = f'{role_name_prefix}-{uuid.uuid4().hex[:8]}'

        assume_role_policy = {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Effect': 'Allow',
                    'Principal': {'Service': 'glue.amazonaws.com'},
                    'Action': 'sts:AssumeRole',
                }
            ],
        }

        response = self.iam.create_role(
            RoleName=role_name, AssumeRolePolicyDocument=json.dumps(assume_role_policy)
        )

        role_arn = response['Role']['Arn']

        # Attach policies
        if policies:
            for policy in policies:
                policy_arn = self._get_policy_arn(policy)
                print(f'Attaching policy {policy_arn} to role {role_name}')
                self.iam.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)

        # Wait for role propagation
        print(f'Created IAM role {role_name}, waiting for propagation...')
        time.sleep(7)

        return role_name, role_arn

    def _find_existing_role(
        self, role_name_prefix: str, policies: List[str]
    ) -> Optional[Tuple[str, str]]:
        """Find existing role with matching prefix and policies."""
        try:
            paginator = self.iam.get_paginator('list_roles')
            for page in paginator.paginate():
                for role in page['Roles']:
                    if role['RoleName'].startswith(role_name_prefix):
                        if self._role_has_policies(role['RoleName'], policies):
                            return role['RoleName'], role['Arn']
        except Exception as e:
            print(f'Error checking existing roles: {e}')
        return None

    def _role_has_policies(self, role_name: str, required_policies: List[str]) -> bool:
        """Check if role has all required policies attached."""
        try:
            response = self.iam.list_attached_role_policies(RoleName=role_name)
            attached_policies = {p['PolicyName'] for p in response['AttachedPolicies']}
            return set(required_policies).issubset(attached_policies)
        except Exception:
            return False

    def _get_policy_arn(self, policy: str) -> str:
        """Get the full ARN for a policy."""
        service_role_policies = {
            'AWSGlueServiceRole',
            'EMR_EC2_DefaultRole',
            'EMR_DefaultRole',
            'EMR_AutoScaling_DefaultRole',
        }

        if policy in service_role_policies:
            return f'arn:aws:iam::aws:policy/service-role/{policy}'
        else:
            return f'arn:aws:iam::aws:policy/{policy}'

    def upload_script_to_s3(self, local_path: str, bucket: str, key: str) -> str:
        """Upload a script to S3.

        Args:
            local_path: Local path to the script file
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            S3 URI for the uploaded file
        """
        if not os.path.exists(local_path):
            raise FileNotFoundError(f'Script file not found: {local_path}')

        print(f'Uploading {local_path} to s3://{bucket}/{key}')
        self.s3.upload_file(local_path, bucket, key)
        return f's3://{bucket}/{key}'

    def check_bucket_exists(self, bucket: str) -> bool:
        """Check if an S3 bucket exists and is accessible.

        Args:
            bucket: S3 bucket name

        Returns:
            True if the bucket exists and is accessible
        """
        try:
            self.s3.head_bucket(Bucket=bucket)
            return True
        except Exception:
            return False

    def check_job_exists(self, job_name: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Check if a Glue job exists.

        Args:
            job_name: Name of the Glue job

        Returns:
            Tuple of (exists, job_details)
        """
        try:
            response = self.glue.get_job(JobName=job_name)
            return True, response['Job']
        except self.glue.exceptions.EntityNotFoundException:
            return False, None

    def delete_glue_job(self, job_name: str) -> bool:
        """Delete a Glue job.

        Args:
            job_name: Name of the Glue job

        Returns:
            True if the job was deleted or didn't exist
        """
        try:
            self.glue.delete_job(JobName=job_name)
            print(f'Deleted Glue job: {job_name}')
            return True
        except self.glue.exceptions.EntityNotFoundException:
            print(f'Glue job {job_name} not found, skipping deletion')
            return True
        except Exception as e:
            print(f'Error deleting Glue job {job_name}: {e}')
            return False

    # We don't delete test IAM role we created for now
    def delete_iam_role(self, role_name: str, policies: Optional[List[str]] = None) -> bool:
        """Delete an IAM role after detaching policies.

        Args:
            role_name: Name of the IAM role
            policies: List of AWS managed policy names to detach

        Returns:
            True if the role was deleted or didn't exist
        """
        try:
            # First detach policies
            if policies:
                for policy in policies:
                    policy_arn = self._get_policy_arn(policy)
                    try:
                        self.iam.detach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
                        print(f'Detached policy {policy_arn} from role {role_name}')
                    except Exception as e:
                        print(f'Error detaching policy {policy_arn} from role {role_name}: {e}')

            # Then delete the role
            self.iam.delete_role(RoleName=role_name)
            print(f'Deleted IAM role: {role_name}')
            return True
        except self.iam.exceptions.NoSuchEntityException:
            print(f'IAM role {role_name} not found, skipping deletion')
            return True
        except Exception as e:
            print(f'Error deleting IAM role {role_name}: {e}')
            return False

    def get_glue_job_tags(self, job_name: str) -> Dict[str, str]:
        """Get tags for a Glue job.

        Args:
            job_name: Name of the Glue job

        Returns:
            Dictionary of tags (key-value pairs)
        """
        try:
            response = self.glue.get_tags(ResourceArn=self._get_job_arn(job_name))
            return response.get('Tags', {})
        except Exception as e:
            print(f'Error getting tags for job {job_name}: {e}')
            return {}

    def _get_job_arn(self, job_name: str) -> str:
        """Get the ARN for a Glue job.

        Args:
            job_name: Name of the Glue job

        Returns:
            ARN of the Glue job
        """
        # First get the AWS account ID
        account_id = boto3.client('sts').get_caller_identity().get('Account')
        return f'arn:aws:glue:{self.region}:{account_id}:job/{job_name}'

    def list_glue_databases(self) -> List[Dict[str, Any]]:
        """List all Glue databases.

        Returns:
            List of database details
        """
        try:
            response = self.glue.get_databases()
            return response.get('DatabaseList', [])
        except Exception as e:
            print(f'Error listing databases: {e}')
            return []

    def delete_glue_database(self, database_name: str) -> bool:
        """Delete a Glue database.

        Args:
            database_name: Name of the database

        Returns:
            True if the database was deleted or didn't exist
        """
        try:
            self.glue.delete_database(Name=database_name)
            print(f'Deleted Glue database: {database_name}')
            return True
        except self.glue.exceptions.EntityNotFoundException:
            print(f'Glue database {database_name} not found, skipping deletion')
            return True
        except Exception as e:
            print(f'Error deleting Glue database {database_name}: {e}')
            return False

    def list_glue_tables(self, database_name: str) -> List[Dict[str, Any]]:
        """List tables in a Glue database.

        Args:
            database_name: Name of the database

        Returns:
            List of table details
        """
        try:
            response = self.glue.get_tables(DatabaseName=database_name)
            return response.get('TableList', [])
        except Exception as e:
            print(f'Error listing tables in database {database_name}: {e}')
            return []

    def start_glue_job_run(self, job_name: str, arguments: Optional[Dict[str, str]] = None) -> str:
        """Start a Glue job run.

        Args:
            job_name: Name of the Glue job
            arguments: Job arguments

        Returns:
            Job run ID
        """
        try:
            response = self.glue.start_job_run(JobName=job_name, Arguments=arguments or {})
            run_id = response['JobRunId']
            print(f'Started Glue job {job_name}, run ID: {run_id}')
            return run_id
        except Exception as e:
            print(f'Error starting job {job_name}: {e}')
            raise

    def wait_for_job_run_completion(
        self,
        job_name: str,
        run_id: str,
        timeout_seconds: int = 300,
        check_interval_seconds: int = 10,
    ) -> Dict[str, Any]:
        """Wait for a Glue job run to complete.

        Args:
            job_name: Name of the Glue job
            run_id: Job run ID
            timeout_seconds: Maximum time to wait in seconds
            check_interval_seconds: Interval between checks in seconds

        Returns:
            Job run details
        """
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            response = self.glue.get_job_run(JobName=job_name, RunId=run_id)
            status = response['JobRun']['JobRunState']

            # Check for terminal states
            if status in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT', 'ERROR']:
                print(f'Job run {run_id} completed with status: {status}')
                return response['JobRun']

            print(f'Job run {run_id} status: {status}, waiting...')
            time.sleep(check_interval_seconds)

        raise TimeoutError(f'Job run {run_id} did not complete within {timeout_seconds} seconds')
