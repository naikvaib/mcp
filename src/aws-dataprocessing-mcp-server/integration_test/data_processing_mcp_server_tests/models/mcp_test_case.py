from typing import Dict, List, Any, Callable
from dataclasses import dataclass, field
from .validators import Validator

@dataclass
class MCPTestCase:
    """Test case definition for MCP server tests."""
    
    test_name: str
    tool_name: str
    input_params: Dict[str, Any]  
    dependencies: List[str] = field(default_factory=list)
    validators: List[Validator] = field(default_factory=list)
    clean_ups: List[Callable] = field(default_factory=list)
    aws_resources: List[Callable[[], None]] = field(default_factory=list)