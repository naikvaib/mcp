# AWS Glue Job MCP Server Test Framework

This directory contains tests for the AWS Glue Job operations in the Data Processing MCP Server. The test framework validates the create-job, update-job, and delete-job operations.

## Test Case Structure

Test cases are defined in `test_cases.json`, with each test case having:

- `test_name`: A unique identifier for the test
- `tool_name`: The MCP server tool to use (typically "glue")
- `input_params`: The parameters to pass to the tool, including:
  - `operation`: The operation to test (create-job, update-job, delete-job)
  - Other operation-specific parameters

## Validators

The framework includes specific validators for each operation:

### Create Job Validators
- Validates job exists in AWS
- Validates job has MCP managed tags
- Validates job name matches input parameters
- Validates job parameters match input parameters

### Update Job Validators
- Validates job exists in AWS
- Validates job has MCP managed tags (operation should fail otherwise)
- Validates job parameters were correctly updated

### Delete Job Validators
- Validates job has MCP managed tags (operation should fail otherwise)
- Validates job was successfully deleted

## Setup and Cleanup Functions

Each operation has dedicated setup and cleanup functions:

### Create Job
- **Setup**: Creates IAM role, uploads script to S3 if needed
- **Cleanup**: Deletes the created job

### Update Job
- **Setup**: Ensures job exists by creating it first if needed
- **Cleanup**: Deletes the job

### Delete Job
- **Setup**: Ensures job exists by creating it first if needed
- **Cleanup**: Verifies job was deleted, cleans up if not

## Negative Testing

For testing error handling, there are negative test cases:

1. **Non-MCP Job Tests**: Operations on jobs not created by the MCP server should fail
   - Use the `create_non_mcp_job.py` script to create non-MCP jobs for testing

## Running Tests

Execute tests from the project root:

```bash
python run_tests.py
```

## Adding New Test Cases

1. Add your new test case to `test_cases.json` following the existing structure
2. Test cases will automatically use the appropriate validators
3. For negative test cases, prefix the test name with "negative_" 

## Creating Non-MCP Jobs for Testing

To create a job without MCP managed tags (for negative testing):

```bash
python tests/glue/create_non_mcp_job.py [--profile PROFILE] [--region REGION] [--job-name JOB_NAME]
```

Then update the negative test cases in `test_cases.json` with the job name:

```json
{
  "test_name": "negative_update_non_mcp_job",
  "tool_name": "glue",
  "input_params": {
    "operation": "update-job",
    "job_name": "job-name-from-script-output",
    ...
  }
}
