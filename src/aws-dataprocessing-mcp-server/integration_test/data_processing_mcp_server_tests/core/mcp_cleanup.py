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

import json
import time
from data_processing_mcp_server_tests.models.clean_up import CleanUper
from data_processing_mcp_server_tests.utils.logger import get_logger
from typing import Any, Dict, Optional


logger = get_logger(__name__)


class DeleteAWSResources(CleanUper):
    """Generic cleaner that deletes any AWS resource based on a given delete API and parameters."""

    def __init__(
        self,
        delete_api: str,
        delete_params: Dict[str, Any] = None,
        boto_client=None,
        resource_field: Optional[str] = None,
        target_param_key: Optional[str] = None,
        param_is_list: bool = False,
    ):
        """Initialize DeleteAWSResources cleaner."""
        self.delete_api = delete_api
        self.delete_params = delete_params or {}
        self.boto_client = boto_client

        self.resource_field = resource_field
        self.target_param_key = target_param_key
        self.param_is_list = param_is_list

    def _convert_to_boto_keys(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Convert snake_case keys to camelCase for boto3 compatibility."""

        def is_snake_case(s: str) -> bool:
            return '_' in s and s.lower() == s

        def camel_case(s):
            parts = s.split('_')
            return ''.join(p.capitalize() for p in parts)

        converted = {}
        for k, v in params.items():
            if is_snake_case(k):
                converted[camel_case(k)] = v
            else:
                converted[k] = v
        return converted

    def clean_up(self, _: Dict[str, Any], actual_response: Dict[str, Any]):
        """Clean up AWS resources."""
        if not self.boto_client:
            raise ValueError('No AWS boto client provided')

        if not hasattr(self.boto_client, self.delete_api):
            raise ValueError(f'AWS client does not support method: {self.delete_api}')

        # Prepare delete params (copy to avoid mutating original)
        delete_params = dict(self.delete_params)

        # If resource_field and target_param_key are provided, extract the resource ID from the response of MCP tool execution
        if self.resource_field and self.target_param_key:
            try:
                content_list = actual_response['result']['content']
                first_text = content_list[0]['text']
                parsed = json.loads(first_text)
                resource_id = parsed.get(self.resource_field)
                if not resource_id:
                    raise ValueError(f'Cannot extract {self.resource_field} from response')

                if self.param_is_list:
                    delete_params[self.target_param_key] = [resource_id]
                else:
                    delete_params[self.target_param_key] = resource_id

                logger.info(
                    f'[Cleanup] Successfully parsed resource ID: {resource_id} to delete params'
                )
            except Exception as e:
                logger.warning(f'[Cleanup Warning] Failed to parse resource ID: {e}')

        try:
            # Handle glue session wait before deletion
            if self.delete_api == 'delete_session':
                session_id = delete_params.get('Id')
                if session_id:
                    for _ in range(60):  # wait max 60s
                        try:
                            describe_response = self.boto_client.get_session(Id=session_id)
                            state = describe_response.get('Session', {}).get('Status')
                            if state in ['READY', 'FAILED', 'TIMEOUT', 'STOPPED']:
                                time.sleep(5)  # wait a bit before deletion
                                break
                            time.sleep(1)
                        except Exception as e:
                            logger.warning(f'[Cleanup] Error checking session status: {e}')
                            break
                    else:
                        logger.warning(
                            f'[Cleanup Warning] Timeout waiting for session {session_id} to be deletable'
                        )

            delete_fn = getattr(self.boto_client, self.delete_api)
            boto_params = self._convert_to_boto_keys(delete_params)
            delete_fn(**boto_params)
        except Exception as e:
            logger.error(f'[Cleanup Error] Failed to call {self.delete_api}: {e}')
            return

        # Handle job run cleanup waiting logic
        if self.delete_api == 'batch_stop_job_run':
            job_name = delete_params.get('JobName')
            job_run_ids = delete_params.get('JobRunIds', [])
            if job_name and job_run_ids:
                for job_run_id in job_run_ids:
                    logger.info(f'[Cleanup] Waiting for job run {job_run_id} to stop...')
                    for _ in range(60):  # maximum wait time of 60 seconds
                        try:
                            response = self.boto_client.get_job_run(
                                JobName=job_name, RunId=job_run_id
                            )
                            state = response['JobRun']['JobRunState']
                            logger.info(f'[Cleanup] Current state of {job_run_id}: {state}')
                            if state in ['STOPPED', 'FAILED', 'SUCCEEDED', 'TIMEOUT']:
                                logger.info(f'[Cleanup] Job run {job_run_id} has stopped.')
                                break
                            time.sleep(1)
                        except Exception as e:
                            logger.warning(f'[Cleanup] Error while checking job run status: {e}')
                            break

                logger.info(
                    'Sleeping for 30 seconds to allow job run state transitions to complete...'
                )
                time.sleep(30)  # Wait for 30 seconds to ensure the status is actually changed
