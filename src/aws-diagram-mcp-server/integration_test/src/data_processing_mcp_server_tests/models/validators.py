from dataclasses import dataclass
from typing import Dict, Any, Optional, List
from abc import ABC, abstractmethod


@dataclass
class ValidationResult:
    success: bool
    error_message: Optional[str] = None

class Validator(ABC):
    """Base abstract validator class."""
    def __init__(self):
        """Initialize Validator."""
        self.validator_type = "base"

class TextValidator(Validator):
    """Validator for checking text in MCP tool responses."""
    def __init__(self):
        """Initialize TextValidator.
        
        Args:
            expected_string: String to look for in the response
            field_path: Optional dot-notation path to check (e.g., 'job.name')
        """
        self.validator_type = "text"

    @abstractmethod
    def validate(self, actual_response: Dict[str, Any]) -> ValidationResult:
        """Validate the response contains expected text.
        
        Args:
            actual_response: The response from the MCP tool call
            
        Returns:
            ValidationResult: Result of the validation
        """
        pass


class BotoValidator(Validator):
    """Validator for checking AWS resources using boto3."""
    def __init__(self, boto_client: Any):
        """
        Args:
            boto_client: Boto3 client for a specific AWS service (e.g., Glue, EMR)
            operation: Boto3 operation name (e.g., 'get_job', 'get_tags')
            operation_input_params: Parameters to call the operation
            expected_keys: Optional list of keys to check in the response
            validate_absence: If True, checks that the keys are not present in the response
        """
        super().__init__()
        self.validator_type = "boto"

        if not boto_client:
            raise ValueError("boto_client must be provided")
        self.boto_client = boto_client


    @abstractmethod
    def validate(self, tool_params: Dict[str, Any], response_map: Optional[Dict[str, Any]] = None) -> ValidationResult:
        """Validate AWS resources using boto3.
        
        Args:
            tool_params: Parameters for the boto3 call
            
        Returns:
            ValidationResult: Result of the validation
        """
        pass

