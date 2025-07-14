"""Test result classes used across the testing framework."""

from typing import List, Optional
from .test_case import MCPTestCase
from ..core.validators import ValidationResult


class TestResult:
    """Class representing the result of a test execution."""
    
    def __init__(self, 
                 test_case: MCPTestCase,
                 success: bool,
                 validation_results: List[ValidationResult] = None,
                 error: Optional[str] = None,
                 execution_time: float = 0.0):
        """Initialize a test result.
        
        Args:
            test_case: The test case that was executed
            success: Whether the test was successful
            validation_results: List of validation results
            error: Error message (if any)
            execution_time: Time taken to execute the test (in seconds)
        """
        self.test_case = test_case
        self.success = success
        self.validation_results = validation_results or []
        self.error = error
        self.execution_time = execution_time
