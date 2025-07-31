"""MCP client for stdio communication."""

import json
import uuid
from typing import Dict, Any, Optional


class MCPClient:
    """MCP client for stdio communication with server."""
    
    def __init__(self, server_manager):
        """Initialize MCP client with server manager."""
        self.server_manager = server_manager
        self.process = server_manager.process
        self.initialized = False
        
    def initialize(self) -> Dict[str, Any]:
        """Initialize the MCP session."""
        if self.initialized:
            return {"result": "already_initialized"}
            
        # Send initialize request with proper MCP parameters
        response = self.send_request("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {
                "roots": {
                    "listChanged": True
                },
                "sampling": {}
            },
            "clientInfo": {
                "name": "test-client",
                "version": "1.0.0"
            }
        })
        
        if "error" not in response:
            # Send initialized notification to complete the handshake
            self.send_notification("notifications/initialized", {})
            self.initialized = True
            
        return response
    
    def send_notification(self, method: str, params: Optional[Dict[str, Any]] = None) -> None:
        """Send a JSON-RPC notification (no response expected).
        
        Args:
            method: The method name
            params: Parameters for the method
        """
        if not self.process or self.process.poll() is not None:
            raise RuntimeError("MCP server is not running")
            
        notification = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params or {}
        }
        
        # Send notification
        notification_json = json.dumps(notification) + "\n"
        self.process.stdin.write(notification_json)
        self.process.stdin.flush()
        
    def send_request(self, method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Send a JSON-RPC request to the MCP server.
        
        Args:
            method: The method name to call
            params: Parameters for the method
            
        Returns:
            The response from the server
        """
        if not self.process or self.process.poll() is not None:
            raise RuntimeError("MCP server is not running")
            
        request_id = str(uuid.uuid4())
        request = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params or {}
        }
        
        # Send request
        request_json = json.dumps(request) + "\n"
        self.process.stdin.write(request_json)
        self.process.stdin.flush()
        
        # Read response
        response_line = self.process.stdout.readline()
        if not response_line:
            raise RuntimeError("No response from MCP server")
            
        try:
            response = json.loads(response_line.strip())
            return response
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Invalid JSON response: {response_line}") from e
    
    def list_tools(self) -> Dict[str, Any]:
        """List available tools from the MCP server."""
        if not self.initialized:
            init_response = self.initialize()
            if "error" in init_response:
                return init_response
                
        return self.send_request("tools/list", {})
    
    def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Call a tool on the MCP server.
        
        Args:
            tool_name: Name of the tool to call
            arguments: Arguments for the tool
            
        Returns:
            The tool response
        """
        return self.send_request("tools/call", {
            "name": tool_name,
            "arguments": arguments
        })