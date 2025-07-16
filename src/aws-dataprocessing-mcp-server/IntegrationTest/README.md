# Data Processing MCP Server Tests Framework

A comprehensive testing framework for validating MCP server tools interacting with AWS services.

## Table of Contents

- [Overview](#overview)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Running Tests](#running-tests)
  - [Local Test Execution](#local-test-execution)
  - [Hydra Integration](#hydra-integration)
- [Test Reporting System](#test-reporting-system)
  - [Available Report Formats](#available-report-formats)
  - [Report Configuration](#report-configuration)
  - [Report Content](#report-content)
  - [Viewing Reports](#viewing-reports)
- [Creating Test Cases](#creating-test-cases)
  - [Basic Test Structure](#basic-test-structure)
  - [Creating Test Cases](#creating-test-cases-1)
  - [Adding Validators](#adding-validators)
  - [Setup and Cleanup Functions](#setup-and-cleanup-functions)
- [Advanced Usage](#advanced-usage)
  - [Custom Validators](#custom-validators)
  - [AWS Resource Management](#aws-resource-management)
  - [Handling Test Results](#handling-test-results)
- [Architecture](#architecture)
  - [Core Components](#core-components)
- [Best Practices](#best-practices)

## Overview

The Data Processing MCP Server Tests Framework provides a structured way to test MCP server tools that interact with AWS services. It supports:

- Test case definition and organization
- Test setup and cleanup
- Custom validation rules
- AWS resource management
- Comprehensive test reporting in multiple formats (Markdown, JSON, HTML)
- Detailed error analysis and fix recommendations

## Getting Started

### Prerequisites

- Python 3.8 or higher
- AWS CLI configured with appropriate permissions
- Access to the DataProcessing MCP Server repository

### Installation

1. Clone this repository:
   ```bash
   git clone <repository-url>
   cd DataProcessingMCPServerTests
   ```

2. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Running Tests

### Local Test Execution

To run tests locally:

1. Ensure your AWS credentials are properly configured. The framework uses boto3's credential resolution chain, so it can use:
   - Environment variables
   - AWS profile configuration
   - EC2 instance profile (if running on EC2)

2. Run a test module:
   ```bash
   python example_glue_test.py
   ```

The test results will be displayed with color-coded pass/fail status for each test case and validation. Additionally, detailed reports are generated in various formats in the `test_reports` directory.

## Creating Test Cases

### Basic Test Structure

A typical test file has the following structure:

```python
from src.data_processing_mcp_server_tests.core.test_case import MCPTestCase
from src.data_processing_mcp_server_tests.core.test_executor import TestExecutor
from src.data_processing_mcp_server_tests.core.validators import ValidationResult
from src.data_processing_mcp_server_tests.core.mcp_client import MCPClient
from src.data_processing_mcp_server_tests.core.mcp_server import MCPServerManager
from src.data_processing_mcp_server_tests.core.aws_setup import AWSSetup

# 1. Define test cases
# 2. Define setup and cleanup functions
# 3. Create and configure the test executor
# 4. Run tests
```

### Creating Test Cases

Test cases define what MCP tool to call and with what parameters:

```python
from src.data_processing_mcp_server_tests.core.test_case import MCPTestCase

# Create a test case for an AWS Glue job
test_case = MCPTestCase(
    test_name="create_glue_job",
    tool_name="manage_aws_glue_jobs",
    input_params={
        "operation": "create-job",
        "job_name": "test-job",
        "job_definition": {
            "Command": {"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
            "Role": "arn:aws:iam::account:role/GlueJobRole",
            "GlueVersion": "4.0",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 2
        }
    }
)
```

### Adding Validators

Validators verify that test cases produce expected results:

```python
from src.data_processing_mcp_server_tests.core.validators import ValidationResult

def validate_glue_job_worker_count(test_case) -> ValidationResult:
    """Validate that the Glue job has the expected worker count"""
    expected_count = test_case.input_params["job_definition"]["NumberOfWorkers"]
    job_name = test_case.input_params["job_name"]

    session = boto3.Session(profile_name='your_profile')
    glue = session.client('glue')

    try:
        response = glue.get_job(JobName=job_name)
        actual_count = response['Job']['NumberOfWorkers']

        if actual_count == expected_count:
            return ValidationResult(True, f"Job has {actual_count} workers as expected")
        else:
            return ValidationResult(False, f"Job has {actual_count} workers, expected {expected_count}")
    except Exception as e:
        return ValidationResult(False, f"Error checking job: {str(e)}")

# Add to executor
executor.add_validator(validate_glue_job_worker_count)
```

### Setup and Cleanup Functions

Setup and cleanup functions prepare the environment before tests and clean up afterward:

```python
def setup_glue_role(test_case):
    """Reusable setup function for Glue job role"""
    role_name, role_arn = aws_setup.setup_test_role("test-glue-role", ["AWSGlueServiceRole"])
    test_case.input_params["job_definition"]["Role"] = role_arn
    print(f"Created IAM role: {role_name}")

def cleanup_glue_job(test_case):
    """Reusable cleanup function for Glue job"""
    job_name = test_case.input_params.get("job_name")
    if job_name:
        aws_setup.delete_glue_job(job_name)

# Add to executor with setup and cleanup
executor.add_test_case(test_case, setup_glue_role, cleanup_glue_job)
```

## Advanced Usage

### Custom Validators

Create custom validators to check specific aspects of your tests:

```python
def my_custom_validator(test_case) -> ValidationResult:
    """Custom validator - check if job name contains 'test'"""
    job_name = test_case.input_params.get("job_name", "")
    if "test" in job_name:
        return ValidationResult(True, f"Job name '{job_name}' contains 'test'")
    return ValidationResult(False, f"Job name '{job_name}' should contain 'test'")

executor.add_validator(my_custom_validator)
```

### AWS Resource Management

Use the AWSSetup utility class to manage AWS resources:

```python
aws_setup = AWSSetup(profile_name="profile_name", region="us-west-1")

# Create IAM role
role_name, role_arn = aws_setup.setup_test_role("test-role", ["AWSGlueServiceRole"])

# Upload script to S3
s3_uri = aws_setup.upload_script_to_s3("local_script.py", "my-bucket", "scripts/test.py")

# Check if Glue job exists
exists, details = aws_setup.check_job_exists("my-job")

# Clean up resources
aws_setup.delete_glue_job("my-job")
```

### Handling Test Results

Process test results programmatically:

```python
# Run tests with report generation
report_formats = ['markdown', 'json', 'html']  # Choose your preferred formats
report_dir = 'test_reports'  # Define output directory

# Run tests and get results
result_info = executor.run_all_tests(
    report_formats=report_formats,
    report_dir=report_dir
)

# Access test results
results = result_info["results"]
summary = result_info["summary"]
report_paths = result_info["reports"]

# Example: Analyze results
failed_tests = [result for result in results if not result.success]
if failed_tests:
    print(f"{len(failed_tests)} tests failed")
    for test in failed_tests:
        print(f"Test {test.test_case.test_name} failed: {test.error}")

# Example: Access summary statistics
print(f"Success rate: {summary['success_rate']:.1f}%")

# Example: Work with generated reports
for fmt, path in report_paths.items():
    print(f"{fmt.upper()} report saved to: {path}")
```

## Architecture

### Core Components

The framework consists of several key components:

- **MCPTestCase**: Defines a test case with test name, tool name, and input parameters
- **TestExecutor**: Runs tests and validators, manages test lifecycle
- **ValidationResult**: Represents the result of a validation check
- **TestResult**: Encapsulates the results of a test execution
- **ReportGenerator**: Generates detailed test reports in multiple formats
- **MCPServerManager**: Manages the MCP server process
- **MCPClient**: Communicates with the MCP server
- **AWSSetup**: Utility for AWS resource management

## Test Reporting System

The framework includes a comprehensive reporting system that generates detailed reports about test execution results in multiple formats. These reports are especially useful for debugging failures, tracking test history, and sharing results with team members.

### Available Report Formats

The reporting system supports the following output formats:

1. **Markdown (.md)**: Human-readable text format suitable for documentation, GitHub, and other platforms that support Markdown.

2. **JSON (.json)**: Machine-readable format for programmatic processing, integration with CI/CD systems, and data analysis.

3. **HTML (.html)**: Rich format with styling for viewing in web browsers, suitable for dashboards and shared reports.

All report formats contain the same core information, just presented differently for different use cases.

### Report Configuration

Configure the reporting system when running tests:

```python
# In your test module
executor = TestExecutor(client)
# Add test cases...

# Configure and run tests with report generation
result_info = executor.run_all_tests(
    report_formats=['markdown', 'json', 'html'],  # Choose formats
    report_dir='test_reports'  # Set output directory
)
```

Reports are saved with timestamped filenames (e.g., `test_report_20250708_201603.md`) to prevent overwriting previous reports.

### Report Content

Each report includes:

- **Summary Statistics**: Total tests, passed tests, failed tests, and success rate
- **Detailed Test Results**: For each test:
  - Test name and status (PASS/FAIL)
  - Tool name and execution time
  - Input parameters used
  - Response received
  - Validation results with pass/fail status
  - Error details (if any)
  - Fix recommendations for common error patterns
- **Error Summary**: Grouped failures by error type for easier debugging

### Viewing Reports

Reports are generated in the specified directory (defaults to `test_reports`):

- **Markdown Reports**: View in any text editor or markdown viewer
- **JSON Reports**: Process with data analysis tools or view with JSON viewers
- **HTML Reports**: Open in any web browser for a formatted view

The console output will display the paths to all generated reports after test execution.
