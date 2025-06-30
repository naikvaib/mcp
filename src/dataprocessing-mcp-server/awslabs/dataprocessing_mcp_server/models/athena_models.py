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

# Response models for Query Management
from mcp.types import CallToolResult, TextContent
from pydantic import Field
from typing import Any, Dict, List, Optional


class BatchGetQueryExecutionResponse(CallToolResult):
    """Response model for batch get query execution operation."""

    isError: bool = Field(..., description='Whether the operation resulted in an error')
    content: List[TextContent] = Field(..., description='Content of the response')
    query_executions: List[Dict[str, Any]] = Field(..., description='List of query executions')
    unprocessed_query_execution_ids: List[Dict[str, Any]] = Field(
        ..., description='List of unprocessed query execution IDs'
    )
    operation: str = Field(default='batch-get-query-execution', description='Operation performed')


class GetQueryExecutionResponse(CallToolResult):
    """Response model for get query execution operation."""

    isError: bool = Field(..., description='Whether the operation resulted in an error')
    content: List[TextContent] = Field(..., description='Content of the response')
    query_execution_id: str = Field(..., description='ID of the query execution')
    query_execution: Dict[str, Any] = Field(
        ...,
        description='Query execution details including ID, SQL query, statement type, result configuration, execution context, status, statistics, and workgroup',
    )
    operation: str = Field(default='get-query-execution', description='Operation performed')


class GetQueryResultsResponse(CallToolResult):
    """Response model for get query results operation."""

    isError: bool = Field(..., description='Whether the operation resulted in an error')
    content: List[TextContent] = Field(..., description='Content of the response')
    query_execution_id: str = Field(..., description='ID of the query execution')
    result_set: Dict[str, Any] = Field(
        ...,
        description='Query result set containing column information and rows of data',
    )
    next_token: Optional[str] = Field(
        None, description='Token for pagination of large result sets'
    )
    update_count: Optional[int] = Field(
        None,
        description='Number of rows inserted with CREATE TABLE AS SELECT, INSERT INTO, or UPDATE statements',
    )
    operation: str = Field(default='get-query-results', description='Operation performed')


class GetQueryRuntimeStatisticsResponse(CallToolResult):
    """Response model for get query runtime statistics operation."""

    isError: bool = Field(..., description='Whether the operation resulted in an error')
    content: List[TextContent] = Field(..., description='Content of the response')
    query_execution_id: str = Field(..., description='ID of the query execution')
    statistics: Dict[str, Any] = Field(
        ...,
        description='Query runtime statistics including timeline, row counts, and execution stages',
    )
    operation: str = Field(
        default='get-query-runtime-statistics', description='Operation performed'
    )


class ListQueryExecutionsResponse(CallToolResult):
    """Response model for list query executions operation."""

    isError: bool = Field(..., description='Whether the operation resulted in an error')
    content: List[TextContent] = Field(..., description='Content of the response')
    query_execution_ids: List[str] = Field(..., description='List of query execution IDs')
    count: int = Field(..., description='Number of query executions found')
    next_token: Optional[str] = Field(None, description='Token for pagination')
    operation: str = Field(default='list-query-executions', description='Operation performed')


class StartQueryExecutionResponse(CallToolResult):
    """Response model for start query execution operation."""

    isError: bool = Field(..., description='Whether the operation resulted in an error')
    content: List[TextContent] = Field(..., description='Content of the response')
    query_execution_id: str = Field(..., description='ID of the started query execution')
    operation: str = Field(default='start-query-execution', description='Operation performed')


class StopQueryExecutionResponse(CallToolResult):
    """Response model for stop query execution operation."""

    isError: bool = Field(..., description='Whether the operation resulted in an error')
    content: List[TextContent] = Field(..., description='Content of the response')
    query_execution_id: str = Field(..., description='ID of the stopped query execution')
    operation: str = Field(default='stop-query-execution', description='Operation performed')


# Response models for Named Query Operations


class BatchGetNamedQueryResponse(CallToolResult):
    """Response model for batch get named query operation."""

    isError: bool = Field(..., description='Whether the operation resulted in an error')
    content: List[TextContent] = Field(..., description='Content of the response')
    named_queries: List[Dict[str, Any]] = Field(..., description='List of named queries')
    unprocessed_named_query_ids: List[Dict[str, Any]] = Field(
        ..., description='List of unprocessed named query IDs'
    )
    operation: str = Field(default='batch-get-named-query', description='Operation performed')


class CreateNamedQueryResponse(CallToolResult):
    """Response model for create named query operation."""

    isError: bool = Field(..., description='Whether the operation resulted in an error')
    content: List[TextContent] = Field(..., description='Content of the response')
    named_query_id: str = Field(..., description='ID of the created named query')
    operation: str = Field(default='create-named-query', description='Operation performed')


class DeleteNamedQueryResponse(CallToolResult):
    """Response model for delete named query operation."""

    isError: bool = Field(..., description='Whether the operation resulted in an error')
    content: List[TextContent] = Field(..., description='Content of the response')
    named_query_id: str = Field(..., description='ID of the deleted named query')
    operation: str = Field(default='delete-named-query', description='Operation performed')


class GetNamedQueryResponse(CallToolResult):
    """Response model for get named query operation."""

    isError: bool = Field(..., description='Whether the operation resulted in an error')
    content: List[TextContent] = Field(..., description='Content of the response')
    named_query_id: str = Field(..., description='ID of the named query')
    named_query: Dict[str, Any] = Field(
        ...,
        description='Named query details including name, description, database, query string, ID, and workgroup',
    )
    operation: str = Field(default='get-named-query', description='Operation performed')


class ListNamedQueriesResponse(CallToolResult):
    """Response model for list named queries operation."""

    isError: bool = Field(..., description='Whether the operation resulted in an error')
    content: List[TextContent] = Field(..., description='Content of the response')
    named_query_ids: List[str] = Field(..., description='List of named query IDs')
    count: int = Field(..., description='Number of named queries found')
    next_token: Optional[str] = Field(None, description='Token for pagination')
    operation: str = Field(default='list-named-queries', description='Operation performed')


class UpdateNamedQueryResponse(CallToolResult):
    """Response model for update named query operation."""

    isError: bool = Field(..., description='Whether the operation resulted in an error')
    content: List[TextContent] = Field(..., description='Content of the response')
    named_query_id: str = Field(..., description='ID of the updated named query')
    operation: str = Field(default='update-named-query', description='Operation performed')
