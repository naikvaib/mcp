# Testing the GitHub Actions Workflow

This document provides instructions on how to test the MCP Tool Test Framework GitHub Action to ensure it's working correctly.

## Local Testing

Before pushing changes to GitHub, you can test the workflow locally to ensure the test framework functions correctly:

1. **Test the run_tests.py script:**

   ```bash
   # Ensure you have AWS credentials configured
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_REGION=us-west-1
   
   # Run the test script
   python run_tests.py
   ```

   Verify that:
   - Tests execute successfully
   - Test failures cause the script to exit with a non-zero status code
   - The script displays a proper summary without attempting to generate reports

2. **Test with a local MCP server:**

   If you have a local MCP server setup:

   ```bash
   export MCP_SERVER_PATH=/path/to/your/local/mcp_server
   python run_tests.py
   ```

## Testing in GitHub Actions

To validate that the workflow works correctly in GitHub Actions:

### 1. Create a Test Branch

```bash
git checkout -b test-github-action
```

### 2. Introduce Test Cases

You can test both success and failure scenarios:

#### Testing Success Scenario

Push your changes without modifying the test cases:

```bash
git add .
git commit -m "Test GitHub Action - success scenario"
git push origin test-github-action
```

Create a pull request to the main branch and observe that:
- The GitHub Action runs successfully
- All tests pass
- The PR can be merged

#### Testing Failure Scenario

Modify a test case in `tests/glue/test_cases.json` to deliberately cause a failure:

```json
{
  "test_name": "intentionally_failing_test",
  "tool_name": "manage_aws_glue_jobs",
  "input_params": {
    "operation": "non-existent-operation",
    "job_name": "should-fail-job"
  }
}
```

Then:

```bash
git add .
git commit -m "Test GitHub Action - failure scenario"
git push origin test-github-action
```

Create a PR and observe that:
- The GitHub Action runs and fails
- The PR cannot be merged due to failing checks

### 3. Verify AWS Integration

To test that the AWS integration works properly in the GitHub environment:

1. Ensure your repository has the correct AWS credentials configured as secrets:
   - AWS_ACCESS_KEY_ID
   - AWS_SECRET_ACCESS_KEY

2. Verify in the GitHub Actions logs that:
   - The action connects to AWS successfully
   - AWS resources are created as expected
   - AWS resources are properly cleaned up after tests

### 4. Test Workflow Changes

If you've made changes to the workflow file itself (`.github/workflows/mcp-tool-tests.yml`), you can verify they work by:

1. Creating a new branch with your changes
2. Pushing the changes
3. Observing that the workflow runs as expected with your modifications

## Common Issues and Troubleshooting

1. **AWS Credential Issues:**
   - Check that the AWS credentials are properly configured as secrets
   - Ensure the credentials have appropriate permissions

2. **MCP Server Issues:**
   - Verify the MCP server repository URL is correct
   - Check that the server path in environment variables is correctly set

3. **Test Case Issues:**
   - Validate JSON syntax in test cases
   - Ensure test cases use supported operations

4. **GitHub Actions Environment Issues:**
   - Verify Python dependencies are correctly installed
   - Check that the workflow is triggered on the intended events

## Creating a Validation PR

A good practice is to create a dedicated Pull Request that demonstrates the GitHub Action working correctly before merging the Action implementation itself. This allows stakeholders to see the action in operation.
