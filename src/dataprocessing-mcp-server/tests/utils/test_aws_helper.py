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

"""Tests for the AwsHelper class."""

import os
from awslabs.dataprocessing_mcp_server.utils.aws_helper import AwsHelper
from awslabs.dataprocessing_mcp_server.utils.consts import (
    MCP_CREATION_TIME_TAG_KEY,
    MCP_MANAGED_TAG_KEY,
    MCP_MANAGED_TAG_VALUE,
    MCP_RESOURCE_TYPE_TAG_KEY,
)
from botocore.config import Config
from botocore.exceptions import ClientError
from datetime import datetime
from unittest.mock import MagicMock, patch


class TestAwsHelper:
    """Tests for the AwsHelper class."""

    def setup_method(self):
        """Reset the cached AWS account ID before each test."""
        # Reset the cached AWS account ID
        AwsHelper._aws_account_id = None

    def test_get_aws_region_with_env_var(self):
        """Test that get_aws_region returns the region from the environment variable."""
        with patch.dict(os.environ, {'AWS_REGION': 'us-west-2'}):
            assert AwsHelper.get_aws_region() == 'us-west-2'

    def test_get_aws_region_without_env_var(self):
        """Test that get_aws_region returns None when the environment variable is not set."""
        with patch.dict(os.environ, {}, clear=True):
            assert AwsHelper.get_aws_region() is None

    def test_get_aws_profile_with_env_var(self):
        """Test that get_aws_profile returns the profile from the environment variable."""
        with patch.dict(os.environ, {'AWS_PROFILE': 'test-profile'}):
            assert AwsHelper.get_aws_profile() == 'test-profile'

    def test_get_aws_profile_without_env_var(self):
        """Test that get_aws_profile returns None when the environment variable is not set."""
        with patch.dict(os.environ, {}, clear=True):
            assert AwsHelper.get_aws_profile() is None

    def test_get_aws_account_id_cached(self):
        """Test that get_aws_account_id returns the cached account ID if available."""
        # Set the cached account ID
        AwsHelper._aws_account_id = '123456789012'

        # Verify that the cached account ID is returned without calling STS
        with patch('boto3.client') as mock_boto3_client:
            account_id = AwsHelper.get_aws_account_id()
            assert account_id == '123456789012'
            mock_boto3_client.assert_not_called()

    def test_get_aws_account_id_uncached(self):
        """Test that get_aws_account_id calls STS when the account ID is not cached."""
        # Mock the STS client
        mock_sts_client = MagicMock()
        mock_sts_client.get_caller_identity.return_value = {'Account': '123456789012'}

        # Mock boto3.client to return our mock STS client
        with patch('boto3.client', return_value=mock_sts_client) as mock_boto3_client:
            account_id = AwsHelper.get_aws_account_id()
            assert account_id == '123456789012'
            mock_boto3_client.assert_called_once_with('sts')
            mock_sts_client.get_caller_identity.assert_called_once()

        # Verify that the account ID is now cached
        assert AwsHelper._aws_account_id == '123456789012'

    def test_get_aws_account_id_exception(self):
        """Test that get_aws_account_id returns a placeholder when STS call fails."""
        # Mock the STS client to raise an exception
        mock_sts_client = MagicMock()
        mock_sts_client.get_caller_identity.side_effect = Exception('STS error')

        # Mock boto3.client to return our mock STS client
        with patch('boto3.client', return_value=mock_sts_client) as mock_boto3_client:
            account_id = AwsHelper.get_aws_account_id()
            assert account_id == 'current-account'
            mock_boto3_client.assert_called_once_with('sts')
            mock_sts_client.get_caller_identity.assert_called_once()

        # Verify that the account ID is not cached
        assert AwsHelper._aws_account_id is None

    def test_create_boto3_client_with_region(self):
        """Test that create_boto3_client creates a client with the specified region."""
        # Mock boto3.client
        mock_client = MagicMock()
        with patch('boto3.client', return_value=mock_client) as mock_boto3_client:
            client = AwsHelper.create_boto3_client('s3', region_name='us-west-2')
            assert client == mock_client
            mock_boto3_client.assert_called_once()
            # Verify that the region was passed
            args, kwargs = mock_boto3_client.call_args
            assert kwargs['region_name'] == 'us-west-2'
            # Verify that the config was passed with the user agent suffix
            assert isinstance(kwargs['config'], Config)
            assert (
                kwargs['config'].user_agent_extra == 'awslabs/mcp/dataprocessing-mcp-server/0.1.0'
            )

    def test_create_boto3_client_with_env_region(self):
        """Test that create_boto3_client uses the region from the environment if not specified."""
        # Mock boto3.client
        mock_client = MagicMock()
        with patch('boto3.client', return_value=mock_client) as mock_boto3_client:
            with patch.dict(os.environ, {'AWS_REGION': 'us-east-1'}):
                client = AwsHelper.create_boto3_client('s3')
                assert client == mock_client
                mock_boto3_client.assert_called_once()
                # Verify that the region was passed from the environment
                args, kwargs = mock_boto3_client.call_args
                assert kwargs['region_name'] == 'us-east-1'

    def test_create_boto3_client_with_profile(self):
        """Test that create_boto3_client creates a client with the specified profile."""
        # Mock boto3.Session
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client

        with patch('boto3.Session', return_value=mock_session) as mock_boto3_session:
            with patch.dict(os.environ, {'AWS_PROFILE': 'test-profile'}):
                client = AwsHelper.create_boto3_client('s3')
                assert client == mock_client
                mock_boto3_session.assert_called_once_with(profile_name='test-profile')
                mock_session.client.assert_called_once()
                # Verify that the config was passed with the user agent suffix
                args, kwargs = mock_session.client.call_args
                assert isinstance(kwargs['config'], Config)
                assert (
                    kwargs['config'].user_agent_extra
                    == 'awslabs/mcp/dataprocessing-mcp-server/0.1.0'
                )

    def test_create_boto3_client_with_profile_and_region(self):
        """Test that create_boto3_client creates a client with both profile and region."""
        # Mock boto3.Session
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client

        with patch('boto3.Session', return_value=mock_session) as mock_boto3_session:
            with patch.dict(os.environ, {'AWS_PROFILE': 'test-profile'}):
                client = AwsHelper.create_boto3_client('s3', region_name='us-west-2')
                assert client == mock_client
                mock_boto3_session.assert_called_once_with(profile_name='test-profile')
                mock_session.client.assert_called_once()
                # Verify that the region was passed
                args, kwargs = mock_session.client.call_args
                assert kwargs['region_name'] == 'us-west-2'

    def test_prepare_resource_tags(self):
        """Test that prepare_resource_tags returns the correct tags."""
        # Mock datetime.utcnow to return a fixed time
        mock_now = datetime(2023, 1, 1, 0, 0, 0)
        with patch('awslabs.dataprocessing_mcp_server.utils.aws_helper.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = mock_now

            # Test with no additional tags
            tags = AwsHelper.prepare_resource_tags('TestResource')
            assert tags[MCP_MANAGED_TAG_KEY] == MCP_MANAGED_TAG_VALUE
            assert tags[MCP_RESOURCE_TYPE_TAG_KEY] == 'TestResource'
            assert tags[MCP_CREATION_TIME_TAG_KEY] == '2023-01-01T00:00:00'

            # Test with additional tags
            additional_tags = {'tag1': 'value1', 'tag2': 'value2'}
            tags = AwsHelper.prepare_resource_tags('TestResource', additional_tags)
            assert tags[MCP_MANAGED_TAG_KEY] == MCP_MANAGED_TAG_VALUE
            assert tags[MCP_RESOURCE_TYPE_TAG_KEY] == 'TestResource'
            assert tags[MCP_CREATION_TIME_TAG_KEY] == '2023-01-01T00:00:00'
            assert tags['tag1'] == 'value1'
            assert tags['tag2'] == 'value2'

    def test_get_resource_tags_glue_job(self):
        """Test that get_resource_tags_glue_job returns the correct tags."""
        mock_glue_client = MagicMock()
        mock_glue_client.get_tags.return_value = {
            'Tags': {MCP_MANAGED_TAG_KEY: MCP_MANAGED_TAG_VALUE}
        }

        result = AwsHelper.get_resource_tags_glue_job(mock_glue_client, 'jobname')
        assert result[MCP_MANAGED_TAG_KEY] == MCP_MANAGED_TAG_VALUE

    def test_get_resource_tags_for_untagged_glue_job(self):
        """Test that get_resource_tags_glue_job returns an empty dict when get-tags returns no tags."""
        mock_glue_client = MagicMock()
        mock_glue_client.get_tags.return_value = {'Tags': {}}

        result = AwsHelper.get_resource_tags_glue_job(mock_glue_client, 'jobname')
        assert len(result) == 0

    def test_get_resource_tags_for_glue_job_client_error(self):
        """Test that get_resource_tags_glue_job returns an empty dict when get-tags returns a ClientError."""
        mock_glue_client = MagicMock()
        mock_glue_client.get_tags.side_effect = ClientError(
            {'Error': {'Code': 'AccessDeniedException', 'Message': 'Access denied'}},
            'GetTags',
        )

        result = AwsHelper.get_resource_tags_glue_job(mock_glue_client, 'jobname')
        assert len(result) == 0

    def test_is_resource_mcp_managed_with_tags(self):
        """Test that is_resource_mcp_managed returns True when the resource has the MCP managed tag."""
        # Mock the Glue client
        mock_glue_client = MagicMock()
        mock_glue_client.get_tags.return_value = {
            'Tags': {MCP_MANAGED_TAG_KEY: MCP_MANAGED_TAG_VALUE}
        }

        # Test with a resource that has the MCP managed tag
        result = AwsHelper.is_resource_mcp_managed(
            mock_glue_client, 'arn:aws:glue:us-west-2:123456789012:database/test-db'
        )
        assert result is True
        mock_glue_client.get_tags.assert_called_once_with(
            ResourceArn='arn:aws:glue:us-west-2:123456789012:database/test-db'
        )

    def test_is_resource_mcp_managed_without_tags(self):
        """Test that is_resource_mcp_managed returns False when the resource doesn't have the MCP managed tag."""
        # Mock the Glue client
        mock_glue_client = MagicMock()
        mock_glue_client.get_tags.return_value = {'Tags': {}}

        # Test with a resource that doesn't have the MCP managed tag
        result = AwsHelper.is_resource_mcp_managed(
            mock_glue_client, 'arn:aws:glue:us-west-2:123456789012:database/test-db'
        )
        assert result is False
        mock_glue_client.get_tags.assert_called_once_with(
            ResourceArn='arn:aws:glue:us-west-2:123456789012:database/test-db'
        )

    def test_is_resource_mcp_managed_with_parameters(self):
        """Test that is_resource_mcp_managed checks parameters when tag check fails."""
        # Mock the Glue client to raise an exception when getting tags
        mock_glue_client = MagicMock()
        mock_glue_client.get_tags.side_effect = ClientError(
            {'Error': {'Code': 'AccessDeniedException', 'Message': 'Access denied'}},
            'GetTags',
        )

        # Test with parameters that have the MCP managed tag
        parameters = {MCP_MANAGED_TAG_KEY: MCP_MANAGED_TAG_VALUE}
        result = AwsHelper.is_resource_mcp_managed(
            mock_glue_client,
            'arn:aws:glue:us-west-2:123456789012:database/test-db',
            parameters=parameters,
        )
        assert result is True
        mock_glue_client.get_tags.assert_called_once()

    def test_is_resource_mcp_managed_without_parameters(self):
        """Test that is_resource_mcp_managed returns False when tag check fails and no parameters are provided."""
        # Mock the Glue client to raise an exception when getting tags
        mock_glue_client = MagicMock()
        mock_glue_client.get_tags.side_effect = ClientError(
            {'Error': {'Code': 'AccessDeniedException', 'Message': 'Access denied'}},
            'GetTags',
        )

        # Test without parameters
        result = AwsHelper.is_resource_mcp_managed(
            mock_glue_client, 'arn:aws:glue:us-west-2:123456789012:database/test-db'
        )
        assert result is False
        mock_glue_client.get_tags.assert_called_once()

    def test_is_resource_mcp_managed_with_parameters_not_managed(self):
        """Test that is_resource_mcp_managed returns False when parameters don't have the MCP managed tag."""
        # Mock the Glue client to raise an exception when getting tags
        mock_glue_client = MagicMock()
        mock_glue_client.get_tags.side_effect = ClientError(
            {'Error': {'Code': 'AccessDeniedException', 'Message': 'Access denied'}},
            'GetTags',
        )

        # Test with parameters that don't have the MCP managed tag
        parameters = {'some_key': 'some_value'}
        result = AwsHelper.is_resource_mcp_managed(
            mock_glue_client,
            'arn:aws:glue:us-west-2:123456789012:database/test-db',
            parameters=parameters,
        )
        assert result is False
        mock_glue_client.get_tags.assert_called_once()
