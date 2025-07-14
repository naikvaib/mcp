"""Test result reporting system."""

import json
import os
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging

from ..models.test_result import TestResult

logger = logging.getLogger(__name__)

# Custom JSON encoder for datetime objects
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(CustomEncoder, self).default(obj)

class ReportGenerator:
    """Generates formatted reports from test results."""
    
    def __init__(self, output_dir: str = "test_reports"):
        """Initialize the report generator.
        
        Args:
            output_dir: Directory where report files will be saved
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def generate_report(self, results: List[TestResult], formats: List[str] = None) -> Dict[str, str]:
        """Generate reports in the specified formats.
        
        Args:
            results: List of TestResult objects
            formats: List of format names ('markdown', 'json', 'html')
                    If None, defaults to ['markdown', 'json']
        
        Returns:
            Dict mapping format names to file paths where reports were saved
        """
        if formats is None:
            formats = ['markdown', 'json']
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_paths = {}
        
        for fmt in formats:
            if fmt == 'markdown':
                content = self._generate_markdown(results)
                filename = f"test_report_{timestamp}.md"
            elif fmt == 'json':
                content = self._generate_json(results)
                filename = f"test_report_{timestamp}.json"
            elif fmt == 'html':
                content = self._generate_html(results)
                filename = f"test_report_{timestamp}.html"
            else:
                logger.warning(f"Unsupported report format: {fmt}")
                continue
            
            filepath = os.path.join(self.output_dir, filename)
            with open(filepath, 'w') as f:
                f.write(content)
            
            report_paths[fmt] = filepath
            logger.info(f"Generated {fmt} report: {filepath}")
        
        return report_paths
    
    def _generate_markdown(self, results: List[TestResult]) -> str:
        """Generate a markdown report from test results."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        lines = [
            "# Test Execution Report",
            f"Generated: {timestamp}",
            "",
            "## Summary",
            ""
        ]
        
        # Summary statistics
        total = len(results)
        passed = sum(1 for r in results if r.success)
        failed = total - passed
        
        lines.extend([
            f"- **Total Tests:** {total}",
            f"- **Passed:** {passed}",
            f"- **Failed:** {failed}",
            f"- **Success Rate:** {passed/total*100:.1f}%" if total > 0 else "- **Success Rate:** N/A",
            ""
        ])
        
        # Test Results
        lines.append("## Test Results")
        lines.append("")
        
        for i, result in enumerate(results, 1):
            status = "✅ PASS" if result.success else "❌ FAIL"
            lines.append(f"### {i}. {result.test_case.test_name} ({status})")
            lines.append("")
            lines.append(f"- **Tool:** `{result.test_case.tool_name}`")
            lines.append(f"- **Execution Time:** {result.execution_time:.2f}s")
            
            # Input parameters (formatted as JSON)
            lines.append("- **Input Parameters:**")
            lines.append("```json")
            lines.append(json.dumps(result.test_case.input_params, indent=2, cls=CustomEncoder))
            lines.append("```")
            
            # Response (if available)
            if result.test_case.response:
                lines.append("- **Response:**")
                lines.append("```json")
                lines.append(json.dumps(result.test_case.response, indent=2, cls=CustomEncoder))
                lines.append("```")
            
            # Validation Results
            if result.validation_results:
                lines.append("- **Validations:**")
                for j, validation in enumerate(result.validation_results, 1):
                    status_icon = "✅" if validation.success else "❌"
                    lines.append(f"  {j}. {status_icon} {validation.message}")
                    
                    if validation.details:
                        lines.append("     ```json")
                        lines.append(f"     {json.dumps(validation.details, indent=2, cls=CustomEncoder)}")
                        lines.append("     ```")
            
            # Error (if any)
            if result.error:
                lines.append("- **Error:**")
                lines.append("```")
                lines.append(result.error)
                lines.append("```")
                
                # Add fix recommendation if available
                fix = self._generate_fix_recommendation(result)
                if fix:
                    lines.append("- **Fix Recommendation:**")
                    lines.append(f"  {fix}")
            
            lines.append("")
        
        # Error Summary
        if failed > 0:
            lines.append("## Error Summary")
            lines.append("")
            
            error_groups = self._group_errors(results)
            for error_type, cases in error_groups.items():
                lines.append(f"### {error_type}")
                lines.append("")
                for case in cases:
                    lines.append(f"- **{case.test_case.test_name}**: {case.error}")
                lines.append("")
        
        return "\n".join(lines)
    
    def _generate_json(self, results: List[TestResult]) -> str:
        """Generate a JSON report from test results."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Summary statistics
        total = len(results)
        passed = sum(1 for r in results if r.success)
        
        report_data = {
            "meta": {
                "generated_at": timestamp,
                "total_tests": total,
                "passed_tests": passed,
                "failed_tests": total - passed,
                "success_rate": round(passed/total*100, 1) if total > 0 else None
            },
            "test_results": []
        }
        
        # Add test results
        for result in results:
            test_data = {
                "test_name": result.test_case.test_name,
                "test_id": result.test_case.test_id,
                "success": result.success,
                "tool_name": result.test_case.tool_name,
                "execution_time": round(result.execution_time, 2),
                "input_params": result.test_case.input_params,
            }
            
            # Add response if available
            if result.test_case.response:
                test_data["response"] = result.test_case.response
            
            # Add validation results
            if result.validation_results:
                test_data["validations"] = [
                    {
                        "success": v.success,
                        "message": v.message,
                        "details": v.details
                    }
                    for v in result.validation_results
                ]
            
            # Add error if any
            if result.error:
                test_data["error"] = result.error
                
                # Add fix recommendation if available
                fix = self._generate_fix_recommendation(result)
                if fix:
                    test_data["fix_recommendation"] = fix
            
            report_data["test_results"].append(test_data)
        
        # Add error summary
        failed_results = [r for r in results if not r.success]
        if failed_results:
            error_groups = self._group_errors(results)
            report_data["error_summary"] = {
                error_type: [
                    {
                        "test_name": case.test_case.test_name,
                        "error": case.error
                    }
                    for case in cases
                ]
                for error_type, cases in error_groups.items()
            }
        
        return json.dumps(report_data, indent=2, cls=CustomEncoder)
    
    def _generate_html(self, results: List[TestResult]) -> str:
        """Generate an HTML report from test results."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Summary statistics
        total = len(results)
        passed = sum(1 for r in results if r.success)
        failed = total - passed
        success_rate = f"{passed/total*100:.1f}%" if total > 0 else "N/A"
        
        html = [
            "<!DOCTYPE html>",
            "<html>",
            "<head>",
            "    <meta charset='UTF-8'>",
            "    <meta name='viewport' content='width=device-width, initial-scale=1.0'>",
            "    <title>Test Execution Report</title>",
            "    <style>",
            "        body { font-family: Arial, sans-serif; line-height: 1.6; margin: 0; padding: 20px; color: #333; }",
            "        .container { max-width: 1200px; margin: 0 auto; }",
            "        h1, h2, h3 { margin-top: 0; }",
            "        .summary { background-color: #f5f5f5; padding: 15px; border-radius: 4px; margin-bottom: 20px; }",
            "        .summary-item { margin: 5px 0; }",
            "        .test-case { background-color: #fff; border: 1px solid #ddd; border-radius: 4px; padding: 15px; margin-bottom: 15px; }",
            "        .pass { color: #28a745; }",
            "        .fail { color: #dc3545; }",
            "        .validation { margin: 10px 0; padding-left: 15px; }",
            "        pre { background-color: #f8f9fa; padding: 10px; overflow: auto; border-radius: 4px; }",
            "        .error-summary { background-color: #fff8f8; border: 1px solid #ffcdd2; border-radius: 4px; padding: 15px; margin-top: 20px; }",
            "    </style>",
            "</head>",
            "<body>",
            "    <div class='container'>",
            f"        <h1>Test Execution Report</h1>",
            f"        <p>Generated: {timestamp}</p>",
            "",
            "        <div class='summary'>",
            "            <h2>Summary</h2>",
            f"            <div class='summary-item'><strong>Total Tests:</strong> {total}</div>",
            f"            <div class='summary-item'><strong>Passed:</strong> <span class='pass'>{passed}</span></div>",
            f"            <div class='summary-item'><strong>Failed:</strong> <span class='fail'>{failed}</span></div>",
            f"            <div class='summary-item'><strong>Success Rate:</strong> {success_rate}</div>",
            "        </div>",
            "",
            "        <h2>Test Results</h2>"
        ]
        
        # Test Results
        for i, result in enumerate(results, 1):
            status_class = "pass" if result.success else "fail"
            status_text = "PASS" if result.success else "FAIL"
            
            html.extend([
                f"        <div class='test-case'>",
                f"            <h3>{i}. {result.test_case.test_name} <span class='{status_class}'>({status_text})</span></h3>",
                f"            <div><strong>Tool:</strong> {result.test_case.tool_name}</div>",
                f"            <div><strong>Execution Time:</strong> {result.execution_time:.2f}s</div>",
                "",
                f"            <div><strong>Input Parameters:</strong></div>",
                f"            <pre>{json.dumps(result.test_case.input_params, indent=2, cls=CustomEncoder)}</pre>"
            ])
            
            # Response (if available)
            if result.test_case.response:
                html.extend([
                    f"            <div><strong>Response:</strong></div>",
                    f"            <pre>{json.dumps(result.test_case.response, indent=2, cls=CustomEncoder)}</pre>"
                ])
            
            # Validation Results
            if result.validation_results:
                html.append(f"            <div><strong>Validations:</strong></div>")
                
                for j, validation in enumerate(result.validation_results, 1):
                    status_class = "pass" if validation.success else "fail"
                    status_icon = "✓" if validation.success else "✗"
                    
                    html.append(f"            <div class='validation {status_class}'>{j}. {status_icon} {validation.message}</div>")
                    
                    if validation.details:
                        html.append(f"            <pre>{json.dumps(validation.details, indent=2, cls=CustomEncoder)}</pre>")
            
            # Error (if any)
            if result.error:
                html.extend([
                    f"            <div><strong>Error:</strong></div>",
                    f"            <pre class='fail'>{result.error}</pre>"
                ])
                
                # Add fix recommendation if available
                fix = self._generate_fix_recommendation(result)
                if fix:
                    html.extend([
                        f"            <div><strong>Fix Recommendation:</strong></div>",
                        f"            <div>{fix}</div>"
                    ])
            
            html.append("        </div>")
        
        # Error Summary
        if failed > 0:
            html.extend([
                "        <div class='error-summary'>",
                "            <h2>Error Summary</h2>"
            ])
            
            error_groups = self._group_errors(results)
            for error_type, cases in error_groups.items():
                html.append(f"            <h3>{error_type}</h3>")
                html.append("            <ul>")
                
                for case in cases:
                    html.append(f"                <li><strong>{case.test_case.test_name}</strong>: {case.error}</li>")
                
                html.append("            </ul>")
            
            html.append("        </div>")
        
        html.extend([
            "    </div>",
            "</body>",
            "</html>"
        ])
        
        return "\n".join(html)
    
    def _group_errors(self, results: List[TestResult]) -> Dict[str, List[TestResult]]:
        """Group failed tests by error type."""
        error_groups = {}
        
        for result in results:
            if not result.success and result.error:
                # Simplified error classification
                error_type = self._classify_error(result.error)
                
                if error_type not in error_groups:
                    error_groups[error_type] = []
                
                error_groups[error_type].append(result)
        
        return error_groups
    
    def _classify_error(self, error_message: str) -> str:
        """Classify errors into categories based on patterns."""
        if not error_message:
            return "Unknown Error"
        
        # Simple classification based on common patterns
        if "permission" in error_message.lower() or "access denied" in error_message.lower():
            return "Permission Errors"
        elif "not found" in error_message.lower() or "doesn't exist" in error_message.lower():
            return "Resource Not Found Errors"
        elif "timeout" in error_message.lower():
            return "Timeout Errors"
        elif "validation" in error_message.lower():
            return "Validation Errors"
        elif "parameter" in error_message.lower():
            return "Parameter Errors"
        elif "connection" in error_message.lower() or "network" in error_message.lower():
            return "Connection Errors"
        else:
            # Extract first few words or use generic category
            words = error_message.split()
            if len(words) >= 2:
                return " ".join(words[:2]) + " Errors"
            return "Other Errors"
    
    def _generate_fix_recommendation(self, result: TestResult) -> Optional[str]:
        """Generate fix recommendations based on error patterns."""
        if not result.error:
            return None
            
        error = result.error.lower()
        
        # IAM/Permission patterns
        if "access denied" in error or "not authorized" in error:
            return "Check IAM permissions. The role may need additional permissions to access the requested resource."
            
        # Parameter validation patterns
        if "invalid parameter" in error or "validation error" in error:
            # Try to extract the parameter name
            param_match = re.search(r"parameter ['\"]?([a-zA-Z0-9_]+)['\"]?", error)
            if param_match:
                param = param_match.group(1)
                return f"Check the value provided for parameter '{param}'. It may be invalid or improperly formatted."
            return "Check input parameters. One or more parameters may be invalid."
            
        # Resource not found patterns
        if "not found" in error or "doesn't exist" in error:
            resource_match = re.search(r"resource ['\"]?([a-zA-Z0-9_]+)['\"]?", error)
            if resource_match:
                resource = resource_match.group(1)
                return f"The resource '{resource}' does not exist. Check if it was created properly or if the identifier is correct."
            return "The requested resource does not exist. Verify the resource identifier and ensure it was created correctly."
            
        # Generic S3 URI pattern
        if "invalid s3 uri" in error:
            return "Check the S3 URI format. It should be in the format 's3://bucket-name/path/to/object'."
            
        # Job name pattern
        if "job name" in error and ("invalid" in error or "not found" in error):
            return "Verify the job name. It might contain invalid characters or might not exist."
        
        return None
