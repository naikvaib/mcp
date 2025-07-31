import os
import tempfile
import boto3
import json

session = boto3.Session(profile_name=os.environ.get('AWS_PROFILE'), region_name=os.environ.get("AWS_REGION"),)
s3_boto_client = session.client('s3')
iam_boto_client = session.client('iam')
glue_boto_client = session.client('glue')

def upload_script():
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
    # Write script to temporary file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as temp:
        temp.write(script_content)
        local_script_path = temp.name
    
    bucket_name = "mcp-test-script-s3"
    s3_key = f"mcp-test-script.py"
    aws_region = os.environ.get('AWS_REGION')
    # Ensure bucket exists
    try:
        s3_boto_client.head_bucket(Bucket=bucket_name)
    except Exception:
        print(f"Creating S3 bucket: {bucket_name}")
        s3_boto_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': aws_region}
        )
    
    s3_uri = f"s3://{bucket_name}/{s3_key}"
    s3_boto_client.upload_file(local_script_path, bucket_name, s3_key)
    
    print(f"Uploaded script to: {s3_uri}")
    
    # Clean up temporary file
    os.unlink(local_script_path)
        

def get_or_create_glue_role():
    """Get or create IAM role for Glue job.
    
    Returns:
        str: The ARN of the IAM role
    """
    role_name = "mcp-test-glue-role"
    paginator = iam_boto_client.get_paginator('list_roles')
    for page in paginator.paginate():
        for role in page['Roles']:
            if role['RoleName'] == role_name:
                print(f"Reusing existing IAM role: {role['RoleName']}")
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
        print(f"Created new IAM role: {role_arn}")
        return role_arn
    except Exception as e:
        raise ValueError(f"Failed to create IAM role: {e}")


def create_non_mcp_job():
    """
    Create a Glue job directly with boto3 (not via MCP server)
    so it doesn't have MCP managed tags.
    """
    job_name = "non-mcp-test-job"
    
    print(f"Creating Glue job: {job_name} (not managed by MCP)")
    
    # Create a job without MCP managed tags
    job_params = {
        "Name": job_name,
        "Role": get_or_create_glue_role(),
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": "s3://non-mcp-test-script-s3/glue_script_ccbdf7ad.py"
        },
        "GlueVersion": "4.0",
        "Tags": {
            "CreatedBy": "DirectAPI",
            "Purpose": "NegativeTestCase",
            "ManagedBy": "DirectAWS"  # Not MCP managed
        }
    }
    
    glue_boto_client.create_job(**job_params)
    print(f"Created non-MCP job: {job_name}")



def create_athena_results_bucket():
    """Create S3 bucket for Athena query results."""

    bucket_name = "mcp-athena-results-bucket"
    try:
        s3_boto_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': boto3.Session().region_name} 
        )
        print(f"Created S3 bucket: {bucket_name}")
    except Exception as e:
        if "BucketAlreadyOwnedByYou" in str(e):
            print(f"Bucket {bucket_name} already exists")
        else:
            print(f"Error creating bucket: {e}")
