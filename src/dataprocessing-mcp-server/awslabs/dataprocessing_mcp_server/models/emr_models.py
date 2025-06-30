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


"""Response models for EMR operations."""

from mcp.types import CallToolResult, TextContent
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional


# Create a base model to avoid inheritance issues with CallToolResult
class EMRResponseBase(BaseModel):
    """Base model for EMR responses."""

    cluster_id: str = Field(..., description='ID of the cluster')


# Response models for EMR Instance Operations


class AddInstanceFleetResponseModel(EMRResponseBase):
    """Model for add instance fleet operation response."""

    instance_fleet_id: str = Field(..., description='ID of the added instance fleet')
    cluster_arn: Optional[str] = Field(None, description='ARN of the cluster')
    operation: str = Field(default='add_fleet', description='Operation performed')


class AddInstanceFleetResponse(CallToolResult):
    """Response model for add instance fleet operation."""

    # Factory method to create response
    @classmethod
    def create(
        cls, is_error: bool, content: List[TextContent], model: AddInstanceFleetResponseModel
    ) -> 'AddInstanceFleetResponse':
        """Create response from model."""
        return cls(
            isError=is_error,
            content=content,
            cluster_id=model.cluster_id,
            instance_fleet_id=model.instance_fleet_id,
            cluster_arn=model.cluster_arn,
            operation=model.operation,
        )


class AddInstanceGroupsResponseModel(EMRResponseBase):
    """Model for add instance groups operation response."""

    job_flow_id: Optional[str] = Field(None, description='Job flow ID (same as cluster ID)')
    instance_group_ids: List[str] = Field(..., description='IDs of the added instance groups')
    cluster_arn: Optional[str] = Field(None, description='ARN of the cluster')
    operation: str = Field(default='add_groups', description='Operation performed')


class AddInstanceGroupsResponse(CallToolResult):
    """Response model for add instance groups operation."""

    # Factory method to create response
    @classmethod
    def create(
        cls, is_error: bool, content: List[TextContent], model: AddInstanceGroupsResponseModel
    ) -> 'AddInstanceGroupsResponse':
        """Create response from model."""
        return cls(
            isError=is_error,
            content=content,
            cluster_id=model.cluster_id,
            job_flow_id=model.job_flow_id,
            instance_group_ids=model.instance_group_ids,
            cluster_arn=model.cluster_arn,
            operation=model.operation,
        )


class ModifyInstanceFleetResponseModel(EMRResponseBase):
    """Model for modify instance fleet operation response."""

    instance_fleet_id: str = Field(..., description='ID of the modified instance fleet')
    operation: str = Field(default='modify_fleet', description='Operation performed')


class ModifyInstanceFleetResponse(CallToolResult):
    """Response model for modify instance fleet operation."""

    # Factory method to create response
    @classmethod
    def create(
        cls, is_error: bool, content: List[TextContent], model: ModifyInstanceFleetResponseModel
    ) -> 'ModifyInstanceFleetResponse':
        """Create response from model."""
        return cls(
            isError=is_error,
            content=content,
            cluster_id=model.cluster_id,
            instance_fleet_id=model.instance_fleet_id,
            operation=model.operation,
        )


class ModifyInstanceGroupsResponseModel(EMRResponseBase):
    """Model for modify instance groups operation response."""

    instance_group_ids: List[str] = Field(..., description='IDs of the modified instance groups')
    operation: str = Field(default='modify_groups', description='Operation performed')


class ModifyInstanceGroupsResponse(CallToolResult):
    """Response model for modify instance groups operation."""

    # Factory method to create response
    @classmethod
    def create(
        cls, is_error: bool, content: List[TextContent], model: ModifyInstanceGroupsResponseModel
    ) -> 'ModifyInstanceGroupsResponse':
        """Create response from model."""
        return cls(
            isError=is_error,
            content=content,
            cluster_id=model.cluster_id,
            instance_group_ids=model.instance_group_ids,
            operation=model.operation,
        )


class ListInstanceFleetsResponseModel(EMRResponseBase):
    """Model for list instance fleets operation response."""

    instance_fleets: List[Dict[str, Any]] = Field(..., description='List of instance fleets')
    count: int = Field(..., description='Number of instance fleets found')
    marker: Optional[str] = Field(None, description='Token for pagination')


class ListInstanceFleetsResponse(CallToolResult):
    """Response model for list instance fleets operation."""

    # Factory method to create response
    @classmethod
    def create(
        cls, is_error: bool, content: List[TextContent], model: ListInstanceFleetsResponseModel
    ) -> 'ListInstanceFleetsResponse':
        """Create response from model."""
        return cls(
            isError=is_error,
            content=content,
            cluster_id=model.cluster_id,
            instance_fleets=model.instance_fleets,
            count=model.count,
            marker=model.marker,
        )


class ListInstancesResponseModel(EMRResponseBase):
    """Model for list instances operation response."""

    instances: List[Dict[str, Any]] = Field(..., description='List of instances')
    count: int = Field(..., description='Number of instances found')
    marker: Optional[str] = Field(None, description='Token for pagination')


class ListInstancesResponse(CallToolResult):
    """Response model for list instances operation."""

    # Factory method to create response
    @classmethod
    def create(
        cls, is_error: bool, content: List[TextContent], model: ListInstancesResponseModel
    ) -> 'ListInstancesResponse':
        """Create response from model."""
        return cls(
            isError=is_error,
            content=content,
            cluster_id=model.cluster_id,
            instances=model.instances,
            count=model.count,
            marker=model.marker,
        )


class ListSupportedInstanceTypesResponseModel(BaseModel):
    """Model for list supported instance types operation response."""

    instance_types: List[Dict[str, Any]] = Field(
        ..., description='List of supported instance types'
    )
    count: int = Field(..., description='Number of instance types found')
    marker: Optional[str] = Field(None, description='Token for pagination')
    release_label: str = Field(..., description='EMR release label')


class ListSupportedInstanceTypesResponse(CallToolResult):
    """Response model for list supported instance types operation."""

    # Factory method to create response
    @classmethod
    def create(
        cls,
        is_error: bool,
        content: List[TextContent],
        model: ListSupportedInstanceTypesResponseModel,
    ) -> 'ListSupportedInstanceTypesResponse':
        """Create response from model."""
        return cls(
            isError=is_error,
            content=content,
            instance_types=model.instance_types,
            count=model.count,
            marker=model.marker,
            release_label=model.release_label,
        )


# Response models for EMR Steps Operations


class AddStepsResponseModel(EMRResponseBase):
    """Model for add steps operation response."""

    step_ids: List[str] = Field(..., description='IDs of the added steps')
    count: int = Field(..., description='Number of steps added')
    operation: str = Field(default='add', description='Operation performed')


class AddStepsResponse(CallToolResult):
    """Response model for add steps operation."""

    # Factory method to create response
    @classmethod
    def create(
        cls, is_error: bool, content: List[TextContent], model: AddStepsResponseModel
    ) -> 'AddStepsResponse':
        """Create response from model."""
        return cls(
            isError=is_error,
            content=content,
            cluster_id=model.cluster_id,
            step_ids=model.step_ids,
            count=model.count,
            operation=model.operation,
        )


class CancelStepsResponseModel(EMRResponseBase):
    """Model for cancel steps operation response."""

    step_cancellation_info: List[Dict[str, Any]] = Field(
        ...,
        description='Information about cancelled steps with status (SUBMITTED/FAILED) and reason',
    )
    count: int = Field(..., description='Number of steps for which cancellation was attempted')
    operation: str = Field(default='cancel', description='Operation performed')


class CancelStepsResponse(CallToolResult):
    """Response model for cancel steps operation."""

    # Factory method to create response
    @classmethod
    def create(
        cls, is_error: bool, content: List[TextContent], model: CancelStepsResponseModel
    ) -> 'CancelStepsResponse':
        """Create response from model."""
        return cls(
            isError=is_error,
            content=content,
            cluster_id=model.cluster_id,
            step_cancellation_info=model.step_cancellation_info,
            count=model.count,
            operation=model.operation,
        )


class DescribeStepResponseModel(EMRResponseBase):
    """Model for describe step operation response."""

    step: Dict[str, Any] = Field(
        ...,
        description='Step details including ID, name, config, status, and execution role',
    )
    operation: str = Field(default='describe', description='Operation performed')


class DescribeStepResponse(CallToolResult):
    """Response model for describe step operation."""

    # Factory method to create response
    @classmethod
    def create(
        cls, is_error: bool, content: List[TextContent], model: DescribeStepResponseModel
    ) -> 'DescribeStepResponse':
        """Create response from model."""
        return cls(
            isError=is_error,
            content=content,
            cluster_id=model.cluster_id,
            step=model.step,
            operation=model.operation,
        )


class ListStepsResponseModel(EMRResponseBase):
    """Model for list steps operation response."""

    steps: List[Dict[str, Any]] = Field(
        ..., description='List of steps in reverse order (most recent first)'
    )
    count: int = Field(..., description='Number of steps found')
    marker: Optional[str] = Field(
        None, description='Pagination token for retrieving next set of results'
    )
    operation: str = Field(default='list', description='Operation performed')


class ListStepsResponse(CallToolResult):
    """Response model for list steps operation."""

    # Factory method to create response
    @classmethod
    def create(
        cls, is_error: bool, content: List[TextContent], model: ListStepsResponseModel
    ) -> 'ListStepsResponse':
        """Create response from model."""
        return cls(
            isError=is_error,
            content=content,
            cluster_id=model.cluster_id,
            steps=model.steps,
            count=model.count,
            marker=model.marker,
            operation=model.operation,
        )
