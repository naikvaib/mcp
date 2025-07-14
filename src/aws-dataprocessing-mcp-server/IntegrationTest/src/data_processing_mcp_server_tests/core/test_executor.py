"""Format-agnostic test executor with bound validators."""

import time
import logging
import os
from typing import List, Optional, Callable, Tuple, Dict, Any

from ..models.test_case import MCPTestCase
from .mcp_client import MCPClient
from .validators import ValidationResult
from ..models.test_result import TestResult
from .reporting import ReportGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestExecutor:
    def __init__(self, mcp_client: MCPClient):
        self.mcp_client = mcp_client
        self.validators: List[Callable] = []
        self.test_entries: List[Tuple[MCPTestCase, Optional[Callable], Optional[Callable]]] = []
        # Track executed tests that need cleanup
        self.executed_tests: List[Tuple[MCPTestCase, Optional[Callable]]] = []
    
    def add_validator(self, validator_func: Callable):
        """Add a validator function to this executor"""
        self.validators.append(validator_func)
    
    def run_test(self, test_case: MCPTestCase, setup_func: Optional[Callable] = None, cleanup_func: Optional[Callable] = None) -> TestResult:
        """Run a single test case with optional setup function"""
        logger.info(f"Running test: {test_case.test_name}")
        start_time = time.time()
        
        try:
            # Setup
            if setup_func:
                setup_func(test_case)
            
            # Execute MCP tool
            response = self.mcp_client.call_tool(
                test_case.tool_name,
                test_case.input_params
            )
            # Store response in test case
            test_case.response = response
            
            # Run all validators bound to this executor
            validation_results = []
            for validator_func in self.validators:
                result = validator_func(test_case)
                validation_results.append(result)
            
            success = all(v.success for v in validation_results)
            
            result = TestResult(
                test_case=test_case,
                success=success,
                validation_results=validation_results,
                execution_time=time.time() - start_time
            )
                
        except Exception as e:
            result = TestResult(
                test_case=test_case,
                success=False,
                error=str(e),
                execution_time=time.time() - start_time
            )
        
        # Store test case and cleanup function for later execution
        # regardless of whether the test succeeded or failed
        if cleanup_func:
            self.executed_tests.append((test_case, cleanup_func))
            
        return result
    
    def add_test_case(self, test_case: MCPTestCase, setup_func: Optional[Callable] = None, cleanup_func: Optional[Callable] = None):
        """Add a test case with optional setup/cleanup functions to this executor"""
        self.test_entries.append((test_case, setup_func, cleanup_func))
    
    def run_all_cleanups(self):
        """Run all cleanup functions for executed tests"""
        logger.info(f"Running cleanup for {len(self.executed_tests)} test cases")
        cleanup_results = []
        
        for test_case, cleanup_func in self.executed_tests:
            try:
                logger.info(f"Running cleanup for test: {test_case.test_name}")
                cleanup_func(test_case)
                cleanup_results.append((test_case.test_name, True, None))
            except Exception as e:
                logger.error(f"Cleanup failed for test {test_case.test_name}: {str(e)}")
                cleanup_results.append((test_case.test_name, False, str(e)))
        
        # Reset the executed tests list
        self.executed_tests = []
        return cleanup_results

    def run_tests_only(self, report_dir: str = "test_reports") -> List[TestResult]:
        """Run all test cases but don't perform cleanup - return the test results
        
        Args:
            report_dir: Directory where reports will be saved (not used directly, but kept for consistency)
            
        Returns:
            List of TestResult objects
        """
        from colorama import Fore, Style, init
        init(autoreset=True)
        
        # Execute all tests
        results = [self.run_test(tc, setup, cleanup) for tc, setup, cleanup in self.test_entries]
        
        # Print formatted results to console
        print("\n=== Test Results ===")
        for i, result in enumerate(results, 1):
            test_status = f"{Fore.GREEN}PASS{Style.RESET_ALL}" if result.success else f"{Fore.RED}FAIL: {result.error}{Style.RESET_ALL}"
            print(f"Test {i} ({result.test_case.test_name}): {test_status}")
            
            for j, validation in enumerate(result.validation_results, 1):
                val_status = f"{Fore.GREEN}PASS{Style.RESET_ALL}" if validation.success else f"{Fore.RED}FAIL{Style.RESET_ALL}"
                print(f"  Validation {j}: {val_status} - {validation.message}")
        
        # Summary for console output
        passed = sum(1 for r in results if r.success)
        total = len(results)
        summary_color = Fore.GREEN if passed == total else Fore.RED
        print(f"\n{summary_color}Summary: {passed}/{total} tests passed{Style.RESET_ALL}")
        
        return results
        
    def run_all_tests(self, report_formats: List[str] = None, report_dir: str = "test_reports") -> Dict[str, Any]:
        """Run all test cases added to this executor with formatted output and report generation
        
        Args:
            report_formats: List of report formats to generate ('markdown', 'json', 'html').
                           If None, defaults to ['markdown', 'json']
            report_dir: Directory where reports will be saved
            
        Returns:
            Dictionary with test results and report information
        """
        from colorama import Fore, Style, init
        init(autoreset=True)
        
        # Execute all tests
        results = [self.run_test(tc, setup, cleanup) for tc, setup, cleanup in self.test_entries]
        
        # Print formatted results to console
        print("\n=== Test Results ===")
        for i, result in enumerate(results, 1):
            test_status = f"{Fore.GREEN}PASS{Style.RESET_ALL}" if result.success else f"{Fore.RED}FAIL: {result.error}{Style.RESET_ALL}"
            print(f"Test {i} ({result.test_case.test_name}): {test_status}")
            
            for j, validation in enumerate(result.validation_results, 1):
                val_status = f"{Fore.GREEN}PASS{Style.RESET_ALL}" if validation.success else f"{Fore.RED}FAIL{Style.RESET_ALL}"
                print(f"  Validation {j}: {val_status} - {validation.message}")
        
        # Summary for console output
        passed = sum(1 for r in results if r.success)
        total = len(results)
        summary_color = Fore.GREEN if passed == total else Fore.RED
        print(f"\n{summary_color}Summary: {passed}/{total} tests passed{Style.RESET_ALL}")
        
        # Generate reports
        report_generator = ReportGenerator(output_dir=report_dir)
        report_paths = report_generator.generate_report(results, formats=report_formats)
        
        # Print report paths
        print("\n=== Test Reports ===")
        for fmt, path in report_paths.items():
            print(f"{fmt.upper()} Report: {os.path.abspath(path)}")
            
        # Run all cleanup functions at the end
        print("\n=== Running All Cleanups ===")
        cleanup_results = self.run_all_cleanups()
        
        # Print cleanup results
        for name, success, error in cleanup_results:
            status = f"{Fore.GREEN}SUCCESS{Style.RESET_ALL}" if success else f"{Fore.RED}FAILED: {error}{Style.RESET_ALL}"
            print(f"Cleanup for {name}: {status}")
        
        return {
            "results": results,
            "summary": {
                "total": total,
                "passed": passed,
                "failed": total - passed,
                "success_rate": passed/total*100 if total > 0 else None
            },
            "reports": report_paths,
            "cleanup_results": cleanup_results
        }
