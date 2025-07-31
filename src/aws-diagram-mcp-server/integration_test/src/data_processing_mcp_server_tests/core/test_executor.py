from typing import Dict, Optional, Tuple, Any
from collections import deque, defaultdict
from .mcp_client import MCPClient
from ..models.validators import BotoValidator, ValidationResult
from ..utils.injection import extract_path
import re

class Executor:
    def __init__(self, test_cases, mcp_client: MCPClient):
        self.test_cases = test_cases
        self.mcp_client = mcp_client
        self.test_case_map = {tc.test_name: tc for tc in test_cases}
        reverse_graph = self._generate_reverse_dependency_graph(test_cases)
        self.sorted_test_cases = self._topological_sort(reverse_graph)
        self.response_map = {tc.test_name: None for tc in test_cases}
        
    def _generate_reverse_dependency_graph(self, test_cases):
        graph = defaultdict(list)

        for test_case in test_cases:
            test_name = test_case.test_name
            if test_name not in graph:
                graph[test_name] = []
            
            for dependency in test_case.dependencies:
                graph[dependency].append(test_name)
                
        return graph
    
    def _topological_sort(self, graph):
        """
        Performs topological sort on the given dependency graph using Kahn's Algorithm
        """
        # Calculate in-degrees for each node
        in_degree = defaultdict(int)
        for node in graph:
            in_degree[node] = 0  # Initialize all nodes with 0
            
        for node in graph:
            for neighbor in graph[node]:
                in_degree[neighbor] += 1

        # Start with nodes that have no dependencies
        queue = deque([node for node in graph if in_degree[node] == 0])
        sorted_order = []

        while queue:
            node = queue.popleft()
            sorted_order.append(node)

            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        # Check for cycles
        if len(sorted_order) != len(graph):
            raise ValueError("Cycle detected! Topological sort not possible.")
            
        return [self.test_case_map[name] for name in sorted_order]
    
    def _check_all_dependencies_succeeded(self, test_name: str, success_map: Dict[str, bool]) -> Tuple[bool, Optional[str]]:
        if (test_name in success_map) and (not success_map[test_name]):
            return False, test_name
        
        dependencies = self.test_case_map[test_name].dependencies
        for dependency in dependencies:
            ok, fail_test_name = self._check_all_dependencies_succeeded(dependency, success_map)
            if not ok:
                return False, fail_test_name

        return True, None

    def _inject_input_params(self, raw_params: Dict[str, Any], response_map: Dict[str, Any]) -> Dict[str, Any]:
        def resolve(value):
            if isinstance(value, str):
                matches = re.findall(r"\{\{(.+?)\}\}", value)
                for m in matches:
                    parts = m.split(".", 1)
                    dep_name = parts[0]
                    sub_path = parts[1] if len(parts) > 1 else ""
                    dep_response = response_map.get(dep_name)
                    if not dep_response:
                        raise ValueError(f"Missing response for dependency: {dep_name}")
                    resolved_value = extract_path(dep_response, sub_path)
                    value = value.replace(f"{{{{{m}}}}}", str(resolved_value))
                return value
            elif isinstance(value, list):
                return [resolve(v) for v in value]
            elif isinstance(value, dict):
                return {k: resolve(v) for k, v in value.items()}
            else:
                return value

        return resolve(raw_params)


    def _setup_all_and_call_tool(self, test_name: str):
        """
        Recursively sets up a test and all its dependencies.
        Returns the test case response after running the tool.
        """
        test_case = self.test_case_map[test_name]

        for dependency in test_case.dependencies:
            self._setup_all_and_call_tool(dependency)
            resp = self.response_map.get(dependency)
            print(f"actual response for {dependency}: {resp}")
            if resp is None:
                print(f"[Injector] Skipping {test_name} because dependency '{dependency}' response is None")
                return

        input_params = self._inject_input_params(test_case.input_params, self.response_map)
        response = self.mcp_client.call_tool(test_case.tool_name, input_params)
        self.response_map[test_name] = response

        
    def _clean_up_all(self, test_name: str):
        """
        Recursively cleans up a test and all its dependencies in reverse order.
        """
        test_case = self.test_case_map[test_name]
        
        try:
            for clean_uper in test_case.clean_ups:
                clean_uper.clean_up(test_case.input_params, self.response_map[test_name] or {})

        except Exception as e:
            print(f"Error during clean up for {test_name}: {str(e)}")
        
        # Then clean up all dependencies in reverse order
        for dependency in reversed(test_case.dependencies):
            self._clean_up_all(dependency)
        

    def execute_tests(self):
        success_map = defaultdict(bool)
        results = []

        for test_case in self.sorted_test_cases:
            test_name = test_case.test_name
            ok, fail_test_name = self._check_all_dependencies_succeeded(test_name, success_map)
            if not ok:
                print(f"Skipping test {test_name} because dependency {fail_test_name} failed")
                results.append({
                    "test_name": test_name,
                    "success": False,
                    "validations": [ValidationResult(False, f"Dependency {fail_test_name} failed")]
                })
                success_map[test_name] = False
                continue

            try:
                # Run setup and execute tool
                self._setup_all_and_call_tool(test_name)

                # Run validators
                all_validations_passed = True
                validation_results = []

                for validator in test_case.validators:
                    if isinstance(validator, BotoValidator):
                        result = validator.validate(
                            tool_params=test_case.input_params,
                            response_map=self.response_map
                        )
                    else:
                        result = validator.validate(self.response_map[test_name])

                    validation_results.append(result)
                    if not result.success:
                        print(f"Validation failed: {result.error_message}")
                        all_validations_passed = False

                success_map[test_name] = all_validations_passed
                print(f"Test {test_name} {'PASSED' if all_validations_passed else 'FAILED'}")

                results.append({
                    "test_name": test_name,
                    "success": all_validations_passed,
                    "validations": validation_results
                })

            except Exception as e:
                print(f"Error executing test {test_name}: {str(e)}")
                error_msg = f"Exception: {str(e)}"
                success_map[test_name] = False
                validation_results = [ValidationResult(False, error_msg)]    
                results.append({ 
                    "test_name": test_name,
                    "success": False,
                    "validations": validation_results
                })
            finally:
                self._clean_up_all(test_name)

        success_count = sum(1 for name, success in success_map.items() if success)
        total_count = len(self.sorted_test_cases)
        return {
            "success_map": success_map,
            "results": results,
            "summary": {
                "total": total_count,
                "passed": success_count,
                "success_rate": (success_count / total_count * 100) if total_count > 0 else 0
            }
        }
