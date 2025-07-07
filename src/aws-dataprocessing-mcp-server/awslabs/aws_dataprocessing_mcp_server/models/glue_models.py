# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from mcp.types import CallToolResult
from pydantic import Field
from typing import Any, Dict, List, Optional


# Response models for Workflows
class CreateWorkflowResponse(CallToolResult):
    """Response model for create workflow operation."""

    workflow_name: str = Field(..., description='Name of the created workflow')
    operation: str = Field(default='create-workflow', description='Creates a new workflow.')


class DeleteWorkflowResponse(CallToolResult):
    """Response model for delete workflow operation."""

    workflow_name: str = Field(..., description='Name of the deleted workflow')
    operation: str = Field(default='delete-workflow', description='Deletes a workflow.')


class GetWorkflowResponse(CallToolResult):
    """Response model for get workflow operation."""

    workflow_name: str = Field(..., description='Name of the workflow')
    workflow_details: Dict[str, Any] = Field(..., description='Complete workflow definition')
    operation: str = Field(
        default='get-workflow', description='Retrieves resource metadata for a workflow.'
    )


class ListWorkflowsResponse(CallToolResult):
    """Response model for get workflows operation."""

    workflows: List[Dict[str, Any]] = Field(..., description='List of workflows')
    next_token: Optional[str] = Field(None, description='Token for pagination')
    operation: str = Field(
        default='list-workflows', description='Lists names of workflows created in the account.'
    )


class StartWorkflowRunResponse(CallToolResult):
    """Response model for start workflow run operation."""

    workflow_name: str = Field(..., description='Name of the workflow')
    run_id: str = Field(..., description='ID of the workflow run')
    operation: str = Field(
        default='start-workflow-run', description='Starts a new run of the specified workflow.'
    )


# Response models for Triggers
class CreateTriggerResponse(CallToolResult):
    """Response model for create trigger operation."""

    trigger_name: str = Field(..., description='Name of the created trigger')
    operation: str = Field(default='create-trigger', description='Creates a new trigger.')


class DeleteTriggerResponse(CallToolResult):
    """Response model for delete trigger operation."""

    trigger_name: str = Field(..., description='Name of the deleted trigger')
    operation: str = Field(
        default='delete-trigger',
        description='Deletes a specified trigger. If the trigger is not found, no exception is thrown.',
    )


class GetTriggerResponse(CallToolResult):
    """Response model for get trigger operation."""

    trigger_name: str = Field(..., description='Name of the trigger')
    trigger_details: Dict[str, Any] = Field(..., description='Complete trigger definition')
    operation: str = Field(
        default='get-trigger', description='Retrieves the definition of a trigger.'
    )


class GetTriggersResponse(CallToolResult):
    """Response model for get triggers operation."""

    triggers: List[Dict[str, Any]] = Field(..., description='List of triggers')
    next_token: Optional[str] = Field(None, description='Token for pagination')
    operation: str = Field(
        default='get-triggers', description='Gets all the triggers associated with a job.'
    )


class StartTriggerResponse(CallToolResult):
    """Response model for start trigger operation."""

    trigger_name: str = Field(..., description='Name of the trigger')
    operation: str = Field(default='start-trigger', description='Starts an existing trigger.')


class StopTriggerResponse(CallToolResult):
    """Response model for stop trigger operation."""

    trigger_name: str = Field(..., description='Name of the trigger')
    operation: str = Field(default='stop-trigger', description='Stops a specified trigger.')


# Response models for Sessions
class CreateSessionResponse(CallToolResult):
    """Response model for create session operation."""

    session_id: str = Field(..., description='ID of the created session')
    session: Optional[Dict[str, Any]] = Field(None, description='Complete session object')
    operation: str = Field(default='create-session', description='Created a new session.')


class DeleteSessionResponse(CallToolResult):
    """Response model for delete session operation."""

    session_id: str = Field(..., description='ID of the deleted session')
    operation: str = Field(default='delete-session', description='Deleted the session.')


class GetSessionResponse(CallToolResult):
    """Response model for get session operation."""

    session_id: str = Field(..., description='ID of the session')
    session: Optional[Dict[str, Any]] = Field(None, description='Complete session object')
    operation: str = Field(default='get-session', description='Retrieves the session.')


class ListSessionsResponse(CallToolResult):
    """Response model for list sessions operation."""

    sessions: List[Dict[str, Any]] = Field(..., description='List of sessions')
    ids: Optional[List[str]] = Field(None, description='List of session IDs')
    count: int = Field(..., description='Number of sessions found')
    next_token: Optional[str] = Field(None, description='Token for pagination')
    operation: str = Field(default='list-sessions', description='Retrieve a list of sessions.')


class StopSessionResponse(CallToolResult):
    """Response model for stop session operation."""

    session_id: str = Field(..., description='ID of the stopped session')
    operation: str = Field(default='stop-session', description='Stops the session.')


# Response models for Statements
class RunStatementResponse(CallToolResult):
    """Response model for run statement operation."""

    session_id: str = Field(..., description='ID of the session')
    statement_id: int = Field(..., description='ID of the statement')
    operation: str = Field(default='run-statement', description='Executes the statement.')


class CancelStatementResponse(CallToolResult):
    """Response model for cancel statement operation."""

    session_id: str = Field(..., description='ID of the session')
    statement_id: int = Field(..., description='ID of the canceled statement')
    operation: str = Field(default='cancel-statement', description='Cancels the statement.')


class GetStatementResponse(CallToolResult):
    """Response model for get statement operation."""

    session_id: str = Field(..., description='ID of the session')
    statement_id: int = Field(..., description='ID of the statement')
    statement: Optional[Dict[str, Any]] = Field(None, description='Complete statement definition')
    operation: str = Field(default='get-statement', description='Retrieves the statement.')


class ListStatementsResponse(CallToolResult):
    """Response model for list statements operation."""

    session_id: str = Field(..., description='ID of the session')
    statements: List[Dict[str, Any]] = Field(..., description='List of statements')
    count: int = Field(..., description='Number of statements found')
    next_token: Optional[str] = Field(None, description='Token for pagination')
    operation: str = Field(
        default='list-statements', description='Lists statements for the session.'
    )