from typing import Dict, List, Any, Callable, Optional
from dataclasses import dataclass, field
from .validators import Validator

@dataclass
class MCPTestCase:
    """Test case definition for MCP server tests."""
    
    # Basic test identification
    test_name: str
    tool_name: str
    input_params: Dict[str, Any]  # Dictionary of parameters instead of Parameters class
    dependencies: List[str] = field(default_factory=list)
    validators: List[Validator] = field(default_factory=list)
    clean_ups: List[Callable] = field(default_factory=list)