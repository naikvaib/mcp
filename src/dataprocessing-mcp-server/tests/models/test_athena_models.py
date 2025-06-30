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


from awslabs.dataprocessing_mcp_server.models.athena_models import (
    BatchGetNamedQueryResponse,
    BatchGetQueryExecutionResponse,
    CreateNamedQueryResponse,
    DeleteNamedQueryResponse,
    GetNamedQueryResponse,
    GetQueryExecutionResponse,
    GetQueryResultsResponse,
    GetQueryRuntimeStatisticsResponse,
    ListNamedQueriesResponse,
    ListQueryExecutionsResponse,
    StartQueryExecutionResponse,
    StopQueryExecutionResponse,
    UpdateNamedQueryResponse,
)
from mcp.types import TextContent


# Test data
sample_text_content = [TextContent(type='text', text='Test message')]
sample_dict = {'key': 'value'}
sample_list = [{'id': 1}, {'id': 2}]


class TestQueryExecutionResponses:
    """Test class for Athena query execution response models."""

    def test_batch_get_query_execution_response(self):
        """Test the BatchGetQueryExecutionResponse model."""
        response = BatchGetQueryExecutionResponse(
            isError=False,
            content=sample_text_content,
            query_executions=sample_list,
            unprocessed_query_execution_ids=[],
        )
        assert response.isError is False
        assert response.query_executions == sample_list
        assert response.unprocessed_query_execution_ids == []
        assert response.operation == 'batch-get-query-execution'

    def test_get_query_execution_response(self):
        """Test the GetQueryExecutionResponse model."""
        response = GetQueryExecutionResponse(
            isError=False,
            content=sample_text_content,
            query_execution_id='query-123',
            query_execution=sample_dict,
        )
        assert response.isError is False
        assert response.query_execution_id == 'query-123'
        assert response.query_execution == sample_dict
        assert response.operation == 'get-query-execution'

    def test_get_query_results_response(self):
        """Test the GetQueryResultsResponse model."""
        response = GetQueryResultsResponse(
            isError=False,
            content=sample_text_content,
            query_execution_id='query-123',
            result_set=sample_dict,
            next_token='next-page',
            update_count=10,
        )
        assert response.isError is False
        assert response.query_execution_id == 'query-123'
        assert response.result_set == sample_dict
        assert response.next_token == 'next-page'
        assert response.update_count == 10
        assert response.operation == 'get-query-results'

    def test_get_query_runtime_statistics_response(self):
        """Test the GetQueryRuntimeStatisticsResponse model."""
        response = GetQueryRuntimeStatisticsResponse(
            isError=False,
            content=sample_text_content,
            query_execution_id='query-123',
            statistics=sample_dict,
        )
        assert response.isError is False
        assert response.query_execution_id == 'query-123'
        assert response.statistics == sample_dict
        assert response.operation == 'get-query-runtime-statistics'

    def test_list_query_executions_response(self):
        """Test the ListQueryExecutionsResponse model."""
        response = ListQueryExecutionsResponse(
            isError=False,
            content=sample_text_content,
            query_execution_ids=['query-1', 'query-2'],
            count=2,
            next_token='next-page',
        )
        assert response.isError is False
        assert response.query_execution_ids == ['query-1', 'query-2']
        assert response.count == 2
        assert response.next_token == 'next-page'
        assert response.operation == 'list-query-executions'

    def test_start_query_execution_response(self):
        """Test the StartQueryExecutionResponse model."""
        response = StartQueryExecutionResponse(
            isError=False,
            content=sample_text_content,
            query_execution_id='query-123',
        )
        assert response.isError is False
        assert response.query_execution_id == 'query-123'
        assert response.operation == 'start-query-execution'

    def test_stop_query_execution_response(self):
        """Test the StopQueryExecutionResponse model."""
        response = StopQueryExecutionResponse(
            isError=False,
            content=sample_text_content,
            query_execution_id='query-123',
        )
        assert response.isError is False
        assert response.query_execution_id == 'query-123'
        assert response.operation == 'stop-query-execution'


class TestNamedQueryResponses:
    """Test class for Athena named query response models."""

    def test_batch_get_named_query_response(self):
        """Test the BatchGetNamedQueryResponse model."""
        response = BatchGetNamedQueryResponse(
            isError=False,
            content=sample_text_content,
            named_queries=sample_list,
            unprocessed_named_query_ids=[],
        )
        assert response.isError is False
        assert response.named_queries == sample_list
        assert response.unprocessed_named_query_ids == []
        assert response.operation == 'batch-get-named-query'

    def test_create_named_query_response(self):
        """Test the CreateNamedQueryResponse model."""
        response = CreateNamedQueryResponse(
            isError=False,
            content=sample_text_content,
            named_query_id='query-123',
        )
        assert response.isError is False
        assert response.named_query_id == 'query-123'
        assert response.operation == 'create-named-query'

    def test_delete_named_query_response(self):
        """Test the DeleteNamedQueryResponse model."""
        response = DeleteNamedQueryResponse(
            isError=False,
            content=sample_text_content,
            named_query_id='query-123',
        )
        assert response.isError is False
        assert response.named_query_id == 'query-123'
        assert response.operation == 'delete-named-query'

    def test_get_named_query_response(self):
        """Test the GetNamedQueryResponse model."""
        response = GetNamedQueryResponse(
            isError=False,
            content=sample_text_content,
            named_query_id='query-123',
            named_query=sample_dict,
        )
        assert response.isError is False
        assert response.named_query_id == 'query-123'
        assert response.named_query == sample_dict
        assert response.operation == 'get-named-query'

    def test_list_named_queries_response(self):
        """Test the ListNamedQueriesResponse model."""
        response = ListNamedQueriesResponse(
            isError=False,
            content=sample_text_content,
            named_query_ids=['query-1', 'query-2'],
            count=2,
            next_token='next-page',
        )
        assert response.isError is False
        assert response.named_query_ids == ['query-1', 'query-2']
        assert response.count == 2
        assert response.next_token == 'next-page'
        assert response.operation == 'list-named-queries'

    def test_update_named_query_response(self):
        """Test the UpdateNamedQueryResponse model."""
        response = UpdateNamedQueryResponse(
            isError=False,
            content=sample_text_content,
            named_query_id='query-123',
        )
        assert response.isError is False
        assert response.named_query_id == 'query-123'
        assert response.operation == 'update-named-query'


def test_error_responses():
    """Test error cases for various response types."""
    error_content = [TextContent(type='text', text='Error occurred')]

    # Test query execution error response
    query_error = StartQueryExecutionResponse(
        isError=True, content=error_content, query_execution_id='query-123'
    )
    assert query_error.isError is True
    assert query_error.content == error_content
    assert query_error.query_execution_id == 'query-123'

    # Test named query error response
    named_query_error = CreateNamedQueryResponse(
        isError=True, content=error_content, named_query_id='query-123'
    )
    assert named_query_error.isError is True
    assert named_query_error.content == error_content
    assert named_query_error.named_query_id == 'query-123'


def test_optional_fields():
    """Test responses with optional fields."""
    # Test response with optional next_token
    results_response = GetQueryResultsResponse(
        isError=False,
        content=sample_text_content,
        query_execution_id='query-123',
        result_set=sample_dict,
        next_token=None,
        update_count=None,
    )
    assert results_response.next_token is None
    assert results_response.update_count is None

    # Test response with optional next_token in list response
    list_response = ListQueryExecutionsResponse(
        isError=False,
        content=sample_text_content,
        query_execution_ids=['query-1', 'query-2'],
        count=2,
        next_token=None,
    )
    assert list_response.next_token is None

    # Test response with optional next_token in named queries list response
    named_list_response = ListNamedQueriesResponse(
        isError=False,
        content=sample_text_content,
        named_query_ids=['query-1', 'query-2'],
        count=2,
        next_token=None,
    )
    assert named_list_response.next_token is None


def test_complex_data_structures():
    """Test responses with more complex data structures."""
    # Complex query execution
    complex_execution = {
        'QueryExecutionId': 'query-123',
        'Query': 'SELECT * FROM table',
        'StatementType': 'DML',
        'ResultConfiguration': {'OutputLocation': 's3://bucket/path'},
        'QueryExecutionContext': {'Database': 'test_db'},
        'Status': {
            'State': 'SUCCEEDED',
            'SubmissionDateTime': '2023-01-01T00:00:00.000Z',
            'CompletionDateTime': '2023-01-01T00:01:00.000Z',
        },
        'Statistics': {
            'EngineExecutionTimeInMillis': 5000,
            'DataScannedInBytes': 1024,
            'TotalExecutionTimeInMillis': 6000,
        },
        'WorkGroup': 'primary',
    }

    # Complex result set
    complex_result_set = {
        'ResultSetMetadata': {
            'ColumnInfo': [
                {'Name': 'col1', 'Type': 'varchar'},
                {'Name': 'col2', 'Type': 'integer'},
            ]
        },
        'Rows': [
            {'Data': [{'VarCharValue': 'header1'}, {'VarCharValue': 'header2'}]},
            {'Data': [{'VarCharValue': 'value1'}, {'VarCharValue': '42'}]},
        ],
    }

    # Complex statistics
    complex_statistics = {
        'EngineExecutionTimeInMillis': 5000,
        'DataScannedInBytes': 1024,
        'TotalExecutionTimeInMillis': 6000,
        'QueryQueueTimeInMillis': 100,
        'ServiceProcessingTimeInMillis': 50,
        'QueryPlanningTimeInMillis': 200,
        'QueryStages': [
            {
                'StageId': 0,
                'State': 'SUCCEEDED',
                'OutputBytes': 1024,
                'OutputRows': 10,
                'InputBytes': 2048,
                'InputRows': 20,
                'ExecutionTime': 5000,
            }
        ],
    }

    # Test with complex query execution
    execution_response = GetQueryExecutionResponse(
        isError=False,
        content=sample_text_content,
        query_execution_id='query-123',
        query_execution=complex_execution,
    )
    assert execution_response.query_execution['Status']['State'] == 'SUCCEEDED'
    assert execution_response.query_execution['Statistics']['DataScannedInBytes'] == 1024

    # Test with complex result set
    results_response = GetQueryResultsResponse(
        isError=False,
        content=sample_text_content,
        query_execution_id='query-123',
        result_set=complex_result_set,
    )
    assert len(results_response.result_set['Rows']) == 2
    assert results_response.result_set['ResultSetMetadata']['ColumnInfo'][0]['Name'] == 'col1'

    # Test with complex statistics
    statistics_response = GetQueryRuntimeStatisticsResponse(
        isError=False,
        content=sample_text_content,
        query_execution_id='query-123',
        statistics=complex_statistics,
    )
    assert statistics_response.statistics['DataScannedInBytes'] == 1024
    assert statistics_response.statistics['QueryStages'][0]['OutputRows'] == 10
