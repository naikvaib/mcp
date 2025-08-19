

# MCP Tool Test Framework

A unified testing framework for validating all AWS-based tools integrated into the **Data Processing MCP Server**, including Glue, Athena, EMR, S3, and IAM tools. 

It ensures consistent setup, validation, and cleanup across tools, with support for dependency tracking, parallel execution, and CI integration via GitHub Actions.

## Features

* **Tool-Level Validation**: Verify the behavior of MCP tools like `manage_aws_glue_jobs`, etc.
* **Automated Test Lifecycle**: Supports full test phases with setup → trigger → validate → cleanup.
* **Custom Validators**: Check AWS state via `boto3`, or response content via keyword matching.
* **Parallel Execution**: Tests grouped by service (e.g., Glue, Athena) and executed in parallel via `pytest-xdist`.
* **CI-Ready**: Fully integrated with GitHub Actions to run tests on pull requests or merges.

## Framework Structure

```
src/data_processing_mcp_server_tests/
├── core/
│   ├── test_executor.py       # Main class to execute test cases with dependency handling
│   ├── mcp_client.py          # Wrapper for MCP server interaction
│   ├── mcp_server.py          # Local server launch logic
│   └── aws_setup.py           # One-time AWS resource setup (buckets, dummy jobs)
│
├── models/
│   ├── mcp_test_case.py       # Data class for defining test cases
│   ├── validators.py          # Custom validators (response, AWS state)
│   └── clean_up.py            # Cleanup logic for AWS resources
│
├── utils/
│   ├── injection.py           # Parameter injection utilities
│   └── utils.py               # Other Map utilities using in validators
│
test/
  ├── test_parallel_pytest.py          # Main test file with all tool test cases and fixtures
  ├── Glue/
  │     ├── glue_job_testcases.py      # Testcases for glue job tool
  │     ├── glue_database_testcases.py # Testcases for glue database tool
  │     └── other_testcases.py
  ├── EMR/ 
  └── Other_Services/  
                          
Configuration:
├── conftest.py                # Pytest configuration and fixtures
├── pytest.ini                 # Pytest settings
├── requirements.txt           # Python dependencies
└── README.md  
```


## Installation

```bash
pip install -r requirements.txt
```
**Requirements**:
- Python 3.10+
- AWS credentials via environment variables, AWS CLI profile, or IAM role
- `MCP_SERVER_PATH` set for local runs


## Writing a Test Case

Each `MCPTestCase` defines:

* `test_name`: Unique name for the test
* `tool_name`: The MCP tool to invoke and test
* `input_params`: Parameters passed to the MCP server
* `validators`: List of `TextValidator` or `AWSBotoValidator` objects to check correctness
* `dependencies`: Test names that must run before this one runs
* `clean_ups`: Optional logic to delete resources post-test
* `aws_resources`: Optional AWS resource setup functions

### Example Test Case

```python
MCPTestCase(
    test_name="create_glue_job_basic",
    tool_name="manage_aws_glue_jobs",
    input_params={
        "operation": "create-job",
        "job_name": "mcp-test-job-basic",
        "job_definition": {
            "Command": {
                "Name": "glueetl",
                "ScriptLocation": "s3://{s3_bucket}/glue_job_script/mcp-test-script.py"
            },
            "Role": glue_role,
            "GlueVersion": "5.0",
            "MaxCapacity": 2,
            "Description": "Basic test job created by MCP server"
        }
    },
    aws_resources=[lambda s3_bucket=s3_bucket: upload_script("mcp-test-script.py", s3_bucket=s3_bucket, prefix="glue_job_script/")],
    dependencies=[],
    validators=[
        ContainsTextValidator("Successfully created Glue job"),
        AWSBotoValidator(
            aws_clients["glue"], 
            operation="get_job", 
            operation_input_params={"job_name": "mcp-test-job-basic"}, 
            expected_keys=[
                "job_definition.Command.Name",
                "job_definition.Command.ScriptLocation",
                "job_definition.Role",
                "job_definition.GlueVersion",
                "job_definition.MaxCapacity"
            ])
    ],
    clean_ups=[
        DeleteAWSResources(
            delete_api="delete_job", 
            delete_params={"job_name": "mcp-test-job-basic"}, 
            boto_client=aws_clients["glue"])
    ]
)
```
**Tips**:
- Place tests in the relevant service folder under `test/` (e.g., `Glue/`, `EMR/`).
- Register tests in `load_all_grouped_test_cases()` in `test_parallel_pytest.py`.
- Use unique resource names and include cleanup logic if any resources are created during test.

## Runnig Tests Locally

Run all tool groups in parallel:
```bash
pytest -n auto -v -k test_tool_group
```

Run a single tool group:
```bash
pytest -v -k "test_tool_group and glue"
```

## CI Integration

The MCP Tool Test Framework is fully integrated with **GitHub Actions** to ensure automated, repeatable integration testing for MCP tools.

### Workflow Behavior

**Triggered On**:

* Pull requests targeting `main` that modify files under `src/aws-dataprocessing-mcp-server/`
* Direct pushes to key branches (e.g., `integration-test-feature`)

**Execution**:
  1. Checks out the MCP Server code
  2. Installs Python and dependencies
  3. Configures AWS credentials via **GitHub OIDC** (preferred)
  4. Runs all MCP integration tests in parallel (`pytest -n auto`)
  5. If any test fails, the workflow fails

### Contributor Workflow (Forks)

To reduce AWS costs and protect upstream secrets:

* Setup GitHub OIDC integration with AWS IAM
* Fork the repo and your AWS credentials as GitHub Secrets in the fork (`AWS_ROLE_ARN`, `AWS_REGION`)
* Run the workflow in your fork (triggered by a PR within your fork)
* Attach the workflow run link to the upstream PR

### Example GitHub Actions Workflow

```yaml
name: MCP Tool Tests

on:
  pull_request:
    branches: [ main ]
    paths:
      - 'src/aws-dataprocessing-mcp-server/**'

permissions:
  id-token: write
  contents: read

jobs:
  run-mcp-tests:
    if: github.repository != 'awslabs/mcp'  # Skip upstream to save CI costs
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - run: |
          pip install "mcp[cli]>=1.6.0"
          pip install -r src/aws-dataprocessing-mcp-server/IntegrationTest/requirements.txt

      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_ROLE_ARN }}
          aws-region: ${{ secrets.AWS_REGION }}

      - run: |
          cd src/aws-dataprocessing-mcp-server/IntegrationTest
          pytest -n auto -v -k test_tool_group
```
