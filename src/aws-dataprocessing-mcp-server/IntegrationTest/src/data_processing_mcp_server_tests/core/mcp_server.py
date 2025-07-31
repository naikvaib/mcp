"""MCP server management for test execution."""

import os
import subprocess
import time
import atexit
from typing import Optional, Tuple, Dict


class MCPServerManager:
    """Manages an MCP server instance for testing."""
    
    def __init__(
        self,
        server_path: str,
        aws_profile: Optional[str] = None,
        aws_region: Optional[str] = None,
        server_args: Optional[str] = None
    ):
        """Initialize the MCP server manager.

        Args:
            server_path: Path to the MCP server directory
            aws_profile: AWS profile to use
            aws_region: AWS region to use
            server_args: Additional command-line arguments for the server
        """
        self.server_path = server_path
        self.aws_profile = aws_profile
        self.aws_region = aws_region
        self.server_args = server_args or ""
        self.process = None
        
    def start(self) -> str:
        """Start the MCP server as a subprocess.
        
        Returns:
            The URL of the running server
        """
        if not os.path.exists(self.server_path):
            raise FileNotFoundError(f"MCP server not found at: {self.server_path}")
            
        # Set up environment
        env = os.environ.copy()
        if self.aws_profile:
            env["AWS_PROFILE"] = self.aws_profile
        if self.aws_region:
            # Set both AWS_REGION and AWS_DEFAULT_REGION to ensure compatibility
            # with all AWS SDK components and tools
            env["AWS_DEFAULT_REGION"] = self.aws_region
            env["AWS_REGION"] = self.aws_region
        
        # Add directory containing awslabs to PYTHONPATH
        # For path like /path/to/dataprocessing-mcp-server/awslabs/dataprocessing_mcp_server
        # We need /path/to/dataprocessing-mcp-server in PYTHONPATH
        awslabs_parent = os.path.dirname(os.path.dirname(self.server_path))
        if "PYTHONPATH" in env:
            env["PYTHONPATH"] = f"{awslabs_parent}:{env['PYTHONPATH']}"
        else:
            env["PYTHONPATH"] = awslabs_parent
        
        # Find server.py in the directory
        server_py = os.path.join(self.server_path, "server.py")
        if not os.path.exists(server_py):
            raise FileNotFoundError(f"server.py not found at: {server_py}")
        
        # Build command
        cmd = ["python", server_py]
        if self.server_args:
            import shlex
            cmd.extend(shlex.split(self.server_args))
        
        print(f"Starting MCP server: {' '.join(cmd)}")
        
        self.process = subprocess.Popen(
            cmd,
            cwd=self.server_path,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        atexit.register(self.stop)
        
        # Wait for server to be ready
        self._wait_for_server()
        print("MCP server started successfully")
        return "stdio"
    
    def _wait_for_server(self, timeout: int = 5) -> None:
        """Wait for server to be ready."""
        print("Waiting for MCP server to be ready...")
        time.sleep(1)  # Give server time to start
        
        if self.process.poll() is not None:
            stdout, stderr = self._collect_process_output()
            raise RuntimeError(
                f"MCP server exited with code {self.process.returncode}\n"
                f"STDOUT: {stdout}\nSTDERR: {stderr}"
            )
    
    def _collect_process_output(self) -> Tuple[str, str]:
        """Collect stdout and stderr from the process."""
        if not self.process:
            return ("", "")
        
        try:
            stdout, stderr = self.process.communicate(timeout=1)
            return (stdout or "", stderr or "")
        except subprocess.TimeoutExpired:
            return ("", "")
    
    def stop(self) -> None:
        """Stop the MCP server."""
        if self.process:
            print("Stopping MCP server...")
            self.process.terminate()
            
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
            
            self.process = None
            print("MCP server stopped")
