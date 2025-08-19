import os
import tempfile
import boto3
import json
from data_processing_mcp_server_tests.utils.logger import get_logger
import botocore.exceptions
from typing import Optional

logger = get_logger(__name__)

session = boto3.Session(profile_name=os.environ.get('AWS_PROFILE'), region_name=os.environ.get("AWS_REGION"),)
s3_boto_client = session.client('s3')
iam_boto_client = session.client('iam')
glue_boto_client = session.client('glue')

def get_standard_bucket_name() -> str:
    """Construct the standard integration test bucket name."""
    region = session.region_name
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return f"dataprocessing-{account_id}-{region}-integration-test".lower()

def create_s3_bucket_if_not_exists():
    """Create the standard test S3 bucket if it does not exist."""
    bucket_name = get_standard_bucket_name()

    try:
        s3_boto_client.head_bucket(Bucket=bucket_name)
        logger.info(f"[Setup] Bucket already exists: {bucket_name}")
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            logger.info(f"[Setup] Creating bucket: {bucket_name}")
            s3_boto_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": session.region_name}
            )
            logger.info(f"[Setup] Created bucket: {bucket_name}")
        else:
            raise RuntimeError(f"[Setup Error] Unexpected error when checking/creating bucket: {e}")
    return bucket_name

def get_or_create_glue_role():
    """Get or create IAM role for Glue job.
    
    Returns:
        str: The ARN of the IAM role
    """
    role_name = "test-mcp-glue-role"
    paginator = iam_boto_client.get_paginator('list_roles')
    for page in paginator.paginate():
        for role in page['Roles']:
            if role['RoleName'] == role_name:
                logger.info(f"Reusing existing IAM role: {role['RoleName']}")
                # Check if role has required permissions before returning
                try:
                    role_policies = iam_boto_client.list_attached_role_policies(RoleName=role['RoleName'])
                    required_policies = ['AWSGlueServiceRole', 'service-role/AWSGlueServiceRole']
                    has_required = any(p['PolicyName'] in required_policies for p in role_policies['AttachedPolicies'])
                    if not has_required:
                        logger.warning(f"Warning: Role {role['RoleName']} may not have required Glue permissions")
                except Exception as e:
                    logger.error(f"Error checking role policies for {role['RoleName']}: {e}")
                return role['Arn']
    # If no existing role found, create a new one
    assume_role_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }
    try:
        response = iam_boto_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy),
            Description="Role for MCP server Glue job testing"
        )
        role_arn = response['Role']['Arn']
        
        # Attach required Glue service policy
        iam_boto_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
        )
        
        logger.info(f"Created new IAM role with Glue permissions: {role_arn}")
        return role_arn
    except Exception as e:
        raise ValueError(f"Failed to create IAM role: {e}")

  
def upload_script(script_name="mcp-test-script.py", s3_bucket=None, prefix="glue_job_script/"):
    """Upload script to S3 and update the tool parameters."""
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
    try:
        create_s3_bucket_if_not_exists()
        if s3_bucket is None:
            s3_bucket = create_s3_bucket_if_not_exists()

        if prefix and not prefix.endswith("/"):
            prefix += "/"

        s3_key = f"{prefix}{script_name}"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as temp:
            temp.write(script_content)
            local_script_path = temp.name

        s3_boto_client.upload_file(local_script_path, s3_bucket, s3_key)
        logger.info(f"[Setup] Uploaded script to s3://{s3_bucket}/{s3_key}")
        os.unlink(local_script_path)    
        return {"Bucket": s3_bucket, "Key": s3_key}

    except Exception as e:
        raise RuntimeError(f"Failed to upload script to S3: {e}")

def create_non_mcp_job(job_name="non-mcp-test-job"):
    """
    Create a Glue job directly with boto3 (not via MCP server)
    so it doesn't have MCP managed tags.
    """
    try:
        logger.info(f"Creating Glue job: {job_name} (not managed by MCP)")

        # Ensure script bucket exists and script is uploaded
        create_s3_bucket_if_not_exists()
        upload_script(script_name="non-mcp-script.py", s3_bucket=get_standard_bucket_name(), prefix="glue_job_script/")

        bucket_name = get_standard_bucket_name()
        script_location = f"s3://{bucket_name}/glue_job_script/non-mcp-script.py"

        job_params = {
            "Name": job_name,
            "Role": get_or_create_glue_role(),
            "Command": {
                "Name": "glueetl",
                "ScriptLocation": script_location
            },
            "GlueVersion": "5.0",
            "Tags": {
                "CreatedBy": "DirectAPI",
                "Purpose": "NegativeTestCase",
                "ManagedBy": "DirectAWS"  # Not MCP managed
            }
        }

        glue_boto_client.create_job(**job_params)
        logger.info(f"Created non-MCP job: {job_name}")
    except Exception as e:
        raise RuntimeError(f"Failed to create non-MCP job '{job_name}': {e}")
