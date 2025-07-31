

# MCP Tool Test Framework

A unified testing framework for validating all AWS-based tools integrated into the **Data Processing MCP Server**, including Glue, Athena, EMR, S3, and IAM tools. It ensures consistent setup, validation, and cleanup across tools with support for dependency tracking and parallel test execution via `pytest`.


## Features

* Modular test case definition with validators and cleanups
* Dependency-aware execution using topological sort
* Built-in AWS resource cleanup logic
* Validators for both response content and live AWS state (via boto3)
* Pytest-based execution with full parallelism support
* CI-ready with GitHub Actions


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
│   └── injection.py           # Parameter injection utilities
│
tests/
└── test_glue_pytest.py        # Main test file with all tool test cases and fixtures

Configuration:
├── conftest.py                # Pytest configuration and fixtures
├── pytest.ini                # Pytest settings
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

---

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

> Recommended Python 3.9+. You also need AWS CLI credentials set up (via environment, profile, or IAM role. The easiest way is to copy and paste your aws temporary credentials into your terminal.)



### 2. Run Tests (Grouped by Tool)

```bash
pytest -n auto -v -k test_tool_group
```

> This runs `test_tool_group` for each tool category (`glue`, `athena`, etc.) in parallel using `pytest-xdist`.



### 3. Run Single Tool Group

```bash
pytest -v -k "test_tool_group and glue"
```



## How It Works

Each `MCPTestCase` defines:

* `tool_name`: The MCP tool to invoke
* `input_params`: Parameters passed to the MCP server
* `validators`: List of `TextValidator` or `AWSBotoValidator` objects to check correctness
* `dependencies`: Test names that must succeed before this one runs
* `clean_ups`: Optional logic to delete resources post-test

The `Executor` performs:

* Topological sort of dependencies
* Recursive setup and tool invocation
* Validation and cleanup per test case
* Aggregation of results for reporting



## Example Test Case

```python
MCPTestCase(
    test_name="create_athena_query",
    tool_name="manage_aws_athena_named_queries",
    input_params={
        "operation": "create",
        "name": "test_query",
        "query_string": "SELECT * FROM table",
        "description": "Test query"
    },
    dependencies=[],
    validators=[ContainsTextValidator("Successfully created")],
    clean_ups=[
        DeleteAWSResources(
            delete_api="delete_named_query",
            boto_client=aws_clients["athena"],
            resource_field="named_query_id",
            target_param_key="NamedQueryId"
        )
    ]
)
```

The framework includes several built-in validator and cleanup classes:

* `ContainsTextValidator`: Validates response contains expected text
* `AWSBotoValidator`: Validates AWS resource state using boto3 clients
* `DeleteAWSResources`: Cleanup helper for deleting AWS resources



## Cleanup

All AWS resources (Glue jobs, Athena queries, etc.) are deleted after test execution using built-in `DeleteAWSResources`.




## Extending

To add support for a new MCP tool:

1. Create test cases in `tool_xyz_test_cases()`
2. Implement validators and cleanups as needed
3. Register them in `load_all_grouped_test_cases()`
