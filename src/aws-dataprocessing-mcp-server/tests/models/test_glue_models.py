from awslabs.aws_dataprocessing_mcp_server.models.glue_models import (
    CreateSessionResponse,
    CreateTriggerResponse,
    CreateWorkflowResponse,
    GetSessionResponse,
    GetTriggersResponse,
    GetWorkflowResponse,
    ListSessionsResponse,
    ListWorkflowsResponse,
)
from mcp.types import TextContent


# Test data
sample_text_content = [TextContent(type='text', text='Test message')]
sample_dict = {'key': 'value'}
sample_list = [{'id': 1}, {'id': 2}]


class TestWorkflowResponses:
    """Test workflow response models."""

    def test_create_workflow_response(self):
        """Test create workflow response."""
        response = CreateWorkflowResponse(
            isError=False, content=sample_text_content, workflow_name='test-workflow'
        )
        assert response.isError is False
        assert response.workflow_name == 'test-workflow'
        assert response.operation == 'create-workflow'

    def test_get_workflow_response(self):
        """Test get workflow response."""
        response = GetWorkflowResponse(
            isError=False,
            content=sample_text_content,
            workflow_name='test-workflow',
            workflow_details=sample_dict,
        )
        assert response.isError is False
        assert response.workflow_name == 'test-workflow'
        assert response.workflow_details == sample_dict
        assert response.operation == 'get-workflow'


class TestTriggerResponses:
    """Test trigger response models."""

    def test_create_trigger_response(self):
        """Test create trigger response."""
        response = CreateTriggerResponse(
            isError=False, content=sample_text_content, trigger_name='test-trigger'
        )
        assert response.isError is False
        assert response.trigger_name == 'test-trigger'
        assert response.operation == 'create-trigger'

    def test_get_triggers_response(self):
        """Test get triggers response."""
        response = GetTriggersResponse(
            isError=False,
            content=sample_text_content,
            triggers=sample_list,
            next_token='next-page',
        )
        assert response.isError is False
        assert response.triggers == sample_list
        assert response.next_token == 'next-page'
        assert response.operation == 'get-triggers'


class TestSessionResponses:
    """Test session response models."""

    def test_create_session_response(self):
        """Test create session response."""
        response = CreateSessionResponse(
            isError=False,
            content=sample_text_content,
            session_id='session-123',
            session=sample_dict,
        )
        assert response.isError is False
        assert response.session_id == 'session-123'
        assert response.session == sample_dict
        assert response.operation == 'create-session'

    def test_list_sessions_response(self):
        """Test list sessions response."""
        response = ListSessionsResponse(
            isError=False,
            content=sample_text_content,
            sessions=sample_list,
            ids=['session-1', 'session-2'],
            count=2,
            next_token='next-page',
        )
        assert response.isError is False
        assert response.sessions == sample_list
        assert response.count == 2
        assert response.ids == ['session-1', 'session-2']
        assert response.next_token == 'next-page'
        assert response.operation == 'list-sessions'


def test_error_responses():
    """Test error cases for various response types."""
    error_content = [TextContent(type='text', text='Error occurred')]

    # Test workflow error response
    workflow_error = CreateWorkflowResponse(
        isError=True, content=error_content, workflow_name='test-workflow'
    )
    assert workflow_error.isError is True
    assert workflow_error.content == error_content


def test_optional_fields():
    """Test responses with optional fields."""
    # Test response with optional next_token
    list_response = ListWorkflowsResponse(
        isError=False, content=sample_text_content, workflows=sample_list, next_token=None
    )
    assert list_response.next_token is None

    # Test response with optional session
    session_response = GetSessionResponse(
        isError=False, content=sample_text_content, session_id='session-123', session=None
    )
    assert session_response.session is None
