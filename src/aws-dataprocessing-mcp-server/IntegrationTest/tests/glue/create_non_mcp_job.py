#!/usr/bin/env python3
"""
Helper script to create a non-MCP managed job for negative test cases.
This job doesn't have the MCP managed tags, so operations like update/delete
from the MCP server should fail with permission errors.
"""

import boto3
import argparse
import uuid

def create_non_mcp_job(profile_name, region_name, job_name=None):
    """
    Create a Glue job directly with boto3 (not via MCP server)
    so it doesn't have MCP managed tags.
    """
    if not job_name:
        job_name = f"non-mcp-test-job-{uuid.uuid4().hex[:8]}"
    
    session = boto3.Session(profile_name=profile_name)
    glue = session.client('glue', region_name=region_name)
    iam = session.client('iam', region_name=region_name)
    
    # Setup IAM role
    print(f"Setting up IAM role for non-MCP job...")
    role_name = f"non-mcp-test-role-{uuid.uuid4().hex[:8]}"
    
    # Create role with assume role policy
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
    
    response = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=str(assume_role_policy).replace("'", '"')
    )
    
    role_arn = response['Role']['Arn']
    
    # Attach Glue service role policy
    policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn=policy_arn
    )
    
    print(f"Creating Glue job: {job_name} (not managed by MCP)")
    
    # Create a job without MCP managed tags
    job_params = {
        "Name": job_name,
        "Role": role_arn,
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": "s3://aws-glue-scripts/default-script.py"
        },
        "GlueVersion": "4.0",
        "Tags": {
            "CreatedBy": "DirectAPI",
            "Purpose": "NegativeTestCase",
            "ManagedBy": "DirectAWS"  # Not MCP managed
        }
    }
    
    glue.create_job(**job_params)
    print(f"Created non-MCP job: {job_name}")
    print(f"Role: {role_name}")
    
    return job_name, role_name

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create a non-MCP managed Glue job for testing')
    parser.add_argument('--profile', type=str, default='kathryncoding', help='AWS profile name')
    parser.add_argument('--region', type=str, default='us-west-1', help='AWS region name')
    parser.add_argument('--job-name', type=str, help='Job name (optional, defaults to generated name)')
    
    args = parser.parse_args()
    job_name, role_name = create_non_mcp_job(args.profile, args.region, args.job_name)
    
    print("\nUse this job name in your negative test cases:")
    print(f"Job name: {job_name}")
