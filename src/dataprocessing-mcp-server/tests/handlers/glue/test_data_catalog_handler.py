# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
# with the License. A copy of the License is located at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions
# and limitations under the License.

"""Tests for the Glue Data Catalog Handler."""

import pytest
from awslabs.dataprocessing_mcp_server.handlers.glue.data_catalog_handler import (
    GlueDataCatalogHandler,
)
from unittest.mock import AsyncMock, MagicMock, patch


class TestGlueDataCatalogHandler:
    """Tests for the GlueDataCatalogHandler class."""

    @pytest.fixture
    def mock_mcp(self):
        """Create a mock MCP server."""
        mock = MagicMock()
        return mock

    @pytest.fixture
    def mock_ctx(self):
        """Create a mock Context."""
        mock = MagicMock()
        return mock

    @pytest.fixture
    def mock_database_manager(self):
        """Create a mock DataCatalogDatabaseManager."""
        mock = AsyncMock()
        return mock

    @pytest.fixture
    def mock_table_manager(self):
        """Create a mock DataCatalogTableManager."""
        mock = AsyncMock()
        return mock

    @pytest.fixture
    def mock_catalog_manager(self):
        """Create a mock DataCatalogManager."""
        mock = AsyncMock()
        return mock

    @pytest.fixture
    def handler(self, mock_mcp, mock_database_manager, mock_table_manager, mock_catalog_manager):
        """Create a GlueDataCatalogHandler instance with mocked dependencies."""
        with (
            patch(
                'awslabs.dataprocessing_mcp_server.handlers.glue.data_catalog_handler.DataCatalogDatabaseManager',
                return_value=mock_database_manager,
            ),
            patch(
                'awslabs.dataprocessing_mcp_server.handlers.glue.data_catalog_handler.DataCatalogTableManager',
                return_value=mock_table_manager,
            ),
            patch(
                'awslabs.dataprocessing_mcp_server.handlers.glue.data_catalog_handler.DataCatalogManager',
                return_value=mock_catalog_manager,
            ),
        ):
            handler = GlueDataCatalogHandler(mock_mcp)
            handler.data_catalog_database_manager = mock_database_manager
            handler.data_catalog_table_manager = mock_table_manager
            handler.data_catalog_manager = mock_catalog_manager
            return handler

    @pytest.fixture
    def handler_with_write_access(
        self, mock_mcp, mock_database_manager, mock_table_manager, mock_catalog_manager
    ):
        """Create a GlueDataCatalogHandler instance with write access enabled."""
        with (
            patch(
                'awslabs.dataprocessing_mcp_server.handlers.glue.data_catalog_handler.DataCatalogDatabaseManager',
                return_value=mock_database_manager,
            ),
            patch(
                'awslabs.dataprocessing_mcp_server.handlers.glue.data_catalog_handler.DataCatalogTableManager',
                return_value=mock_table_manager,
            ),
            patch(
                'awslabs.dataprocessing_mcp_server.handlers.glue.data_catalog_handler.DataCatalogManager',
                return_value=mock_catalog_manager,
            ),
        ):
            handler = GlueDataCatalogHandler(mock_mcp, allow_write=True)
            handler.data_catalog_database_manager = mock_database_manager
            handler.data_catalog_table_manager = mock_table_manager
            handler.data_catalog_manager = mock_catalog_manager
            return handler

    def test_initialization(self, mock_mcp):
        """Test that the handler is initialized correctly."""
        handler = GlueDataCatalogHandler(mock_mcp)

        # Verify that the handler has the correct attributes
        assert handler.mcp == mock_mcp
        assert handler.allow_write is False
        assert handler.allow_sensitive_data_access is False

        # Verify that the tools were registered
        assert mock_mcp.tool.call_count == 5

        # Get all call args
        call_args_list = mock_mcp.tool.call_args_list

        # Get all tool names that were registered
        tool_names = [call_args[1]['name'] for call_args in call_args_list]

        # Verify that expected tools are registered
        assert 'manage_aws_glue_databases' in tool_names
        assert 'manage_aws_glue_tables' in tool_names
        assert 'manage_aws_glue_connections' in tool_names
        assert 'manage_aws_glue_partitions' in tool_names
        assert 'manage_aws_glue_catalog' in tool_names

    def test_initialization_with_write_access(self, mock_mcp):
        """Test that the handler is initialized correctly with write access."""
        handler = GlueDataCatalogHandler(mock_mcp, allow_write=True)

        # Verify that the handler has the correct attributes
        assert handler.mcp == mock_mcp
        assert handler.allow_write is True
        assert handler.allow_sensitive_data_access is False

    def test_initialization_with_sensitive_data_access(self, mock_mcp):
        """Test that the handler is initialized correctly with sensitive data access."""
        handler = GlueDataCatalogHandler(mock_mcp, allow_sensitive_data_access=True)

        # Verify that the handler has the correct attributes
        assert handler.mcp == mock_mcp
        assert handler.allow_write is False
        assert handler.allow_sensitive_data_access is True

    @pytest.mark.asyncio
    async def test_manage_aws_glue_data_catalog_databases_create_no_write_access(
        self, handler, mock_ctx
    ):
        """Test that create database operation is not allowed without write access."""
        # Mock the response class
        mock_response = MagicMock()
        mock_response.isError = True
        mock_response.content = [MagicMock()]
        mock_response.content[
            0
        ].text = 'Operation create-database is not allowed without write access'
        mock_response.database_name = ''
        mock_response.operation = 'create'

        # Patch the CreateDatabaseResponse class
        with patch(
            'awslabs.dataprocessing_mcp_server.models.data_catalog_models.CreateDatabaseResponse',
            return_value=mock_response,
        ):
            # Call the method with a write operation
            result = await handler.manage_aws_glue_data_catalog_databases(
                mock_ctx, operation='create-database', database_name='test-db'
            )

            # Verify the result
            assert result.isError is True
            assert 'not allowed without write access' in result.content[0].text
            assert result.database_name == ''
            assert result.operation == 'create'

    @pytest.mark.asyncio
    async def test_manage_aws_glue_data_catalog_databases_delete_no_write_access(
        self, handler, mock_ctx
    ):
        """Test that delete database operation is not allowed without write access."""
        # Mock the response class
        mock_response = MagicMock()
        mock_response.isError = True
        mock_response.content = [MagicMock()]
        mock_response.content[
            0
        ].text = 'Operation delete-database is not allowed without write access'
        mock_response.database_name = ''
        mock_response.operation = 'delete'

        # Patch the DeleteDatabaseResponse class
        with patch(
            'awslabs.dataprocessing_mcp_server.models.data_catalog_models.DeleteDatabaseResponse',
            return_value=mock_response,
        ):
            # Call the method with a write operation
            result = await handler.manage_aws_glue_data_catalog_databases(
                mock_ctx, operation='delete-database', database_name='test-db'
            )

            # Verify the result
            assert result.isError is True
            assert 'not allowed without write access' in result.content[0].text
            assert result.database_name == ''
            assert result.operation == 'delete'

    @pytest.mark.asyncio
    async def test_manage_aws_glue_data_catalog_databases_update_no_write_access(
        self, handler, mock_ctx
    ):
        """Test that update database operation is not allowed without write access."""
        # Mock the response class
        mock_response = MagicMock()
        mock_response.isError = True
        mock_response.content = [MagicMock()]
        mock_response.content[
            0
        ].text = 'Operation update-database is not allowed without write access'
        mock_response.database_name = ''
        mock_response.operation = 'update'

        # Patch the UpdateDatabaseResponse class
        with patch(
            'awslabs.dataprocessing_mcp_server.models.data_catalog_models.UpdateDatabaseResponse',
            return_value=mock_response,
        ):
            # Call the method with a write operation
            result = await handler.manage_aws_glue_data_catalog_databases(
                mock_ctx, operation='update-database', database_name='test-db'
            )

            # Verify the result
            assert result.isError is True
            assert 'not allowed without write access' in result.content[0].text
            assert result.database_name == ''
            assert result.operation == 'update'

    @pytest.mark.asyncio
    async def test_manage_aws_glue_data_catalog_databases_get_read_access(
        self, handler, mock_ctx, mock_database_manager
    ):
        """Test that get database operation is allowed with read access."""
        from unittest.mock import ANY

        # Mock the response class
        mock_response = MagicMock()
        mock_response.isError = False
        mock_response.content = []
        mock_response.database_name = 'test-db'
        mock_response.description = 'Test database'
        mock_response.location_uri = 's3://test-bucket/'
        mock_response.parameters = {}
        mock_response.creation_time = '2023-01-01T00:00:00Z'
        mock_response.operation = 'get'
        mock_response.catalog_id = '123456789012'

        # Setup the mock to return a response
        mock_database_manager.get_database.return_value = mock_response

        # Call the method with a read operation
        result = await handler.manage_aws_glue_data_catalog_databases(
            mock_ctx, operation='get-database', database_name='test-db'
        )

        # Verify that the method was called with the correct parameters
        # Use ANY for catalog_id to handle the FieldInfo object
        mock_database_manager.get_database.assert_called_once_with(
            ctx=mock_ctx, database_name='test-db', catalog_id=ANY
        )

        # Verify that the result is the expected response
        assert result == mock_response

    @pytest.mark.asyncio
    async def test_manage_aws_glue_data_catalog_databases_list_read_access(
        self, handler, mock_ctx, mock_database_manager
    ):
        """Test that list databases operation is allowed with read access."""
        from unittest.mock import ANY

        # Mock the response class
        mock_response = MagicMock()
        mock_response.isError = False
        mock_response.content = []
        mock_response.databases = []
        mock_response.count = 0
        mock_response.catalog_id = '123456789012'
        mock_response.operation = 'list'

        # Setup the mock to return a response
        mock_database_manager.list_databases.return_value = mock_response

        # Call the method with a read operation
        result = await handler.manage_aws_glue_data_catalog_databases(
            mock_ctx, operation='list-databases'
        )

        # Verify that the method was called with the correct parameters
        # Use ANY for catalog_id to handle the FieldInfo object
        mock_database_manager.list_databases.assert_called_once_with(ctx=mock_ctx, catalog_id=ANY)

        # Verify that the result is the expected response
        assert result == mock_response

    @pytest.mark.asyncio
    async def test_manage_aws_glue_data_catalog_databases_create_with_write_access(
        self, handler_with_write_access, mock_ctx, mock_database_manager
    ):
        """Test that create database operation is allowed with write access."""
        # Setup the mock to return a response
        expected_response = MagicMock()
        expected_response.isError = False
        expected_response.content = []
        expected_response.database_name = 'test-db'
        expected_response.operation = 'create'
        mock_database_manager.create_database.return_value = expected_response

        # Call the method with a write operation
        result = await handler_with_write_access.manage_aws_glue_data_catalog_databases(
            mock_ctx,
            operation='create-database',
            database_name='test-db',
            description='Test database',
            location_uri='s3://test-bucket/',
            parameters={'key': 'value'},
            catalog_id='123456789012',
        )

        # Verify that the method was called with the correct parameters
        mock_database_manager.create_database.assert_called_once_with(
            ctx=mock_ctx,
            database_name='test-db',
            description='Test database',
            location_uri='s3://test-bucket/',
            parameters={'key': 'value'},
            catalog_id='123456789012',
        )

        # Verify that the result is the expected response
        assert result == expected_response

    @pytest.mark.asyncio
    async def test_manage_aws_glue_data_catalog_databases_delete_with_write_access(
        self, handler_with_write_access, mock_ctx, mock_database_manager
    ):
        """Test that delete database operation is allowed with write access."""
        # Setup the mock to return a response
        expected_response = MagicMock()
        expected_response.isError = False
        expected_response.content = []
        expected_response.database_name = 'test-db'
        expected_response.operation = 'delete'
        mock_database_manager.delete_database.return_value = expected_response

        # Call the method with a write operation
        result = await handler_with_write_access.manage_aws_glue_data_catalog_databases(
            mock_ctx,
            operation='delete-database',
            database_name='test-db',
            catalog_id='123456789012',
        )

        # Verify that the method was called with the correct parameters
        mock_database_manager.delete_database.assert_called_once_with(
            ctx=mock_ctx, database_name='test-db', catalog_id='123456789012'
        )

        # Verify that the result is the expected response
        assert result == expected_response

    @pytest.mark.asyncio
    async def test_manage_aws_glue_data_catalog_databases_update_with_write_access(
        self, handler_with_write_access, mock_ctx, mock_database_manager
    ):
        """Test that update database operation is allowed with write access."""
        # Setup the mock to return a response
        expected_response = MagicMock()
        expected_response.isError = False
        expected_response.content = []
        expected_response.database_name = 'test-db'
        expected_response.operation = 'update'
        mock_database_manager.update_database.return_value = expected_response

        # Call the method with a write operation
        result = await handler_with_write_access.manage_aws_glue_data_catalog_databases(
            mock_ctx,
            operation='update-database',
            database_name='test-db',
            description='Updated database',
            location_uri='s3://updated-bucket/',
            parameters={'key': 'updated-value'},
            catalog_id='123456789012',
        )

        # Verify that the method was called with the correct parameters
        mock_database_manager.update_database.assert_called_once_with(
            ctx=mock_ctx,
            database_name='test-db',
            description='Updated database',
            location_uri='s3://updated-bucket/',
            parameters={'key': 'updated-value'},
            catalog_id='123456789012',
        )

        # Verify that the result is the expected response
        assert result == expected_response

    @pytest.mark.asyncio
    async def test_manage_aws_glue_data_catalog_databases_invalid_operation(
        self, handler, mock_ctx
    ):
        """Test that an invalid operation returns an error response."""
        # Set write access to true to bypass the "not allowed without write access" check
        handler.allow_write = True

        # Call the method with an invalid operation
        result = await handler.manage_aws_glue_data_catalog_databases(
            mock_ctx, operation='invalid-operation', database_name='test-db'
        )

        # Verify that the result is an error response
        assert result.isError is True
        assert 'Invalid operation' in result.content[0].text
        assert result.database_name == ''
        assert result.operation == 'get'

    @pytest.mark.asyncio
    async def test_manage_aws_glue_data_catalog_databases_missing_database_name(
        self, handler_with_write_access, mock_ctx
    ):
        """Test that missing database_name parameter raises a ValueError."""
        # Mock the ValueError that should be raised
        with patch.object(
            handler_with_write_access.data_catalog_database_manager,
            'create_database',
            side_effect=ValueError('database_name is required for create-database operation'),
        ):
            # Call the method without database_name
            with pytest.raises(ValueError) as excinfo:
                await handler_with_write_access.manage_aws_glue_data_catalog_databases(
                    mock_ctx, operation='create-database'
                )

            # Verify that the correct error message is raised
            assert 'database_name is required' in str(excinfo.value)

    @pytest.mark.asyncio
    async def test_manage_aws_glue_data_catalog_databases_exception_handling(
        self, handler, mock_ctx, mock_database_manager
    ):
        """Test that exceptions are handled correctly."""
        # Setup the mock to raise an exception
        mock_database_manager.get_database.side_effect = Exception('Test exception')

        # Patch the handler's method to handle the exception properly
        with patch.object(
            handler,
            'manage_aws_glue_data_catalog_databases',
            side_effect=handler.manage_aws_glue_data_catalog_databases,
        ):
            # Create a mock response for the GetDatabaseResponse
            mock_response = MagicMock()
            mock_response.isError = True
            mock_response.content = [MagicMock()]
            mock_response.content[
                0
            ].text = 'Error in manage_aws_glue_data_catalog_databases: Test exception'
            mock_response.database_name = 'test-db'
            mock_response.operation = 'get'

            # Patch the GetDatabaseResponse class
            with patch(
                'awslabs.dataprocessing_mcp_server.models.data_catalog_models.GetDatabaseResponse',
                return_value=mock_response,
            ):
                # Call the method
                result = await handler.manage_aws_glue_data_catalog_databases(
                    mock_ctx, operation='get-database', database_name='test-db'
                )

                # Verify that the result is an error response
                assert result.isError is True
                assert (
                    'Error in manage_aws_glue_data_catalog_databases: Test exception'
                    in result.content[0].text
                )
                assert result.database_name == 'test-db'
                assert result.operation == 'get'

    # Tests for manage_aws_glue_data_catalog_tables method

    @pytest.mark.asyncio
    async def test_manage_aws_glue_data_catalog_tables_create_no_write_access(
        self, handler, mock_ctx
    ):
        """Test that create table operation is not allowed without write access."""
        # Call the method with a write operation
        result = await handler.manage_aws_glue_data_catalog_tables(
            mock_ctx,
            operation='create-table',
            database_name='test-db',
            table_name='test-table',
            table_input={},
        )

        # Verify that the result is an error response
        assert result.isError is True
        assert 'not allowed without write access' in result.content[0].text
        assert result.database_name == 'test-db'
        assert result.table_name == ''
        assert result.operation == 'create'

    @pytest.mark.asyncio
    async def test_manage_aws_glue_data_catalog_tables_get_read_access(
        self, handler, mock_ctx, mock_table_manager
    ):
        """Test that get table operation is allowed with read access."""
        from unittest.mock import ANY

        # Setup the mock to return a response
        expected_response = MagicMock()
        expected_response.isError = False
        expected_response.content = []
        expected_response.database_name = 'test-db'
        expected_response.table_name = 'test-table'
        expected_response.table_definition = {}
        expected_response.creation_time = '2023-01-01T00:00:00Z'
        expected_response.last_access_time = '2023-01-01T00:00:00Z'
        expected_response.operation = 'get'
        mock_table_manager.get_table.return_value = expected_response

        # Call the method with a read operation
        result = await handler.manage_aws_glue_data_catalog_tables(
            mock_ctx, operation='get-table', database_name='test-db', table_name='test-table'
        )

        # Verify that the method was called with the correct parameters
        # Use ANY for catalog_id to handle the FieldInfo object
        mock_table_manager.get_table.assert_called_once_with(
            ctx=mock_ctx, database_name='test-db', table_name='test-table', catalog_id=ANY
        )

        # Verify that the result is the expected response
        assert result == expected_response

    # Tests for manage_aws_glue_data_catalog_connections method

    @pytest.mark.asyncio
    async def test_manage_aws_glue_data_catalog_connections_create_no_write_access(
        self, handler, mock_ctx
    ):
        """Test that create connection operation is not allowed without write access."""
        # Call the method with a write operation
        result = await handler.manage_aws_glue_data_catalog_connections(
            mock_ctx, operation='create', connection_name='test-connection', connection_input={}
        )

        # Verify that the result is an error response
        assert result.isError is True
        assert 'not allowed without write access' in result.content[0].text
        assert result.connection_name == ''
        assert result.operation == 'create'

    @pytest.mark.asyncio
    async def test_manage_aws_glue_data_catalog_connections_get_read_access(
        self, handler, mock_ctx, mock_catalog_manager
    ):
        """Test that get connection operation is allowed with read access."""
        from unittest.mock import ANY

        # Setup the mock to return a response
        expected_response = MagicMock()
        expected_response.isError = False
        expected_response.content = []
        expected_response.connection_name = 'test-connection'
        expected_response.connection_type = 'JDBC'
        expected_response.connection_properties = {}
        expected_response.physical_connection_requirements = None
        expected_response.creation_time = '2023-01-01T00:00:00Z'
        expected_response.last_updated_time = '2023-01-01T00:00:00Z'
        expected_response.last_updated_by = ''
        expected_response.status = ''
        expected_response.status_reason = ''
        expected_response.last_connection_validation_time = ''
        expected_response.catalog_id = ''
        expected_response.operation = 'get'
        mock_catalog_manager.get_connection.return_value = expected_response

        # Call the method with a read operation
        result = await handler.manage_aws_glue_data_catalog_connections(
            mock_ctx, operation='get', connection_name='test-connection'
        )

        # Verify that the method was called with the correct parameters
        # Use ANY for catalog_id to handle the FieldInfo object
        mock_catalog_manager.get_connection.assert_called_once_with(
            ctx=mock_ctx, connection_name='test-connection', catalog_id=ANY
        )

        # Verify that the result is the expected response
        assert result == expected_response

    # Tests for manage_aws_glue_data_catalog_partitions method

    @pytest.mark.asyncio
    async def test_manage_aws_glue_data_catalog_partitions_create_no_write_access(
        self, handler, mock_ctx
    ):
        """Test that create partition operation is not allowed without write access."""
        # Call the method with a write operation
        result = await handler.manage_aws_glue_data_catalog_partitions(
            mock_ctx,
            operation='create',
            database_name='test-db',
            table_name='test-table',
            partition_values=['2023'],
            partition_input={},
        )

        # Verify that the result is an error response
        assert result.isError is True
        assert 'not allowed without write access' in result.content[0].text
        assert result.database_name == 'test-db'
        assert result.table_name == 'test-table'
        assert result.partition_values == []
        assert result.operation == 'create'

    @pytest.mark.asyncio
    async def test_manage_aws_glue_data_catalog_partitions_get_read_access(
        self, handler, mock_ctx, mock_catalog_manager
    ):
        """Test that get partition operation is allowed with read access."""
        from unittest.mock import ANY

        # Setup the mock to return a response
        expected_response = MagicMock()
        expected_response.isError = False
        expected_response.content = []
        expected_response.database_name = 'test-db'
        expected_response.table_name = 'test-table'
        expected_response.partition_values = ['2023']
        expected_response.partition_definition = {}
        expected_response.creation_time = '2023-01-01T00:00:00Z'
        expected_response.last_access_time = '2023-01-01T00:00:00Z'
        expected_response.operation = 'get'
        mock_catalog_manager.get_partition.return_value = expected_response

        # Call the method with a read operation
        result = await handler.manage_aws_glue_data_catalog_partitions(
            mock_ctx,
            operation='get',
            database_name='test-db',
            table_name='test-table',
            partition_values=['2023'],
        )

        # Verify that the method was called with the correct parameters
        # Use ANY for catalog_id to handle the FieldInfo object
        mock_catalog_manager.get_partition.assert_called_once_with(
            ctx=mock_ctx,
            database_name='test-db',
            table_name='test-table',
            partition_values=['2023'],
            catalog_id=ANY,
        )

        # Verify that the result is the expected response
        assert result == expected_response

    # Tests for manage_aws_glue_data_catalog method

    @pytest.mark.asyncio
    async def test_manage_aws_glue_data_catalog_create_no_write_access(self, handler, mock_ctx):
        """Test that create catalog operation is not allowed without write access."""
        # Call the method with a write operation
        result = await handler.manage_aws_glue_data_catalog(
            mock_ctx, operation='create', catalog_id='test-catalog', catalog_input={}
        )

        # Verify that the result is an error response
        assert result.isError is True
        assert 'not allowed without write access' in result.content[0].text
        assert result.catalog_id == ''
        assert result.operation == 'create'

    @pytest.mark.asyncio
    async def test_manage_aws_glue_data_catalog_get_read_access(
        self, handler, mock_ctx, mock_catalog_manager
    ):
        """Test that get catalog operation is allowed with read access."""
        # Setup the mock to return a response
        expected_response = MagicMock()
        expected_response.isError = False
        expected_response.content = []
        expected_response.catalog_id = 'test-catalog'
        expected_response.catalog_definition = {}
        expected_response.name = 'Test Catalog'
        expected_response.description = 'Test catalog description'
        expected_response.create_time = '2023-01-01T00:00:00Z'
        expected_response.update_time = '2023-01-01T00:00:00Z'
        expected_response.operation = 'get'
        mock_catalog_manager.get_catalog.return_value = expected_response

        # Call the method with a read operation
        result = await handler.manage_aws_glue_data_catalog(
            mock_ctx, operation='get', catalog_id='test-catalog'
        )

        # Verify that the method was called with the correct parameters
        mock_catalog_manager.get_catalog.assert_called_once_with(
            ctx=mock_ctx, catalog_id='test-catalog'
        )

        # Verify that the result is the expected response
        assert result == expected_response
