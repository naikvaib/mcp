"""Test case definitions for MCP server tests."""

import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Callable


@dataclass
class MCPTestCase:
    """Test case definition for MCP server tests."""
    
    # Basic test identification
    test_name: str
    tool_name: str
    input_params: Dict[str, Any] = field(default_factory=dict)
    
    # Test execution response (set after execution)
    response: Optional[Dict[str, Any]] = None
    
    # Test configuration
    timeout_seconds: int = 120
    test_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
