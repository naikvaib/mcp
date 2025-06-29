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

"""EMREc2StepsHandler for Data Processing MCP Server."""

import os
from awslabs.dataprocessing_mcp_server.utils.aws_helper import AwsHelper
from awslabs.dataprocessing_mcp_server.utils.logging_helper import (
    LogLevel,
    log_with_request_id,
)
from awslabs.dataprocessing_mcp_server.models.emr_models import (
    AddStepsResponse,
    CancelStepsResponse,
    DescribeStepResponse,
    ListStepsResponse,
)

from mcp.server.fastmcp import Context
from mcp.types import TextContent
from pydantic import Field, BaseModel
from typing import Dict, List, Optional, Tuple, Union, Any
from botocore.exceptions import ClientError


class EMREc2StepsHandler:
    """Handler for Amazon EMR EC2 Steps operations."""

    def __init__(
        self, mcp, allow_write: bool = False, allow_sensitive_data_access: bool = False
    ):
        """Initialize the EMR EC2 Steps handler.

        Args:
            mcp: The MCP server instance
            allow_write: Whether to enable write access (default: False)
            allow_sensitive_data_access: Whether to allow access to sensitive data (default: False)
        """
        self.mcp = mcp
        self.allow_write = allow_write
        self.allow_sensitive_data_access = allow_sensitive_data_access
        self.emr_client = AwsHelper.create_boto3_client("emr")

        # Register tools
        self.mcp.tool(name="manage_aws_emr_ec2_steps")(self.manage_aws_emr_ec2_steps)

    async def manage_aws_emr_ec2_steps(
        self,
        ctx: Context,
        operation: str = Field(
            ...,
            description="Operation to perform: add-steps, cancel-steps, describe-step, list-steps. Choose read-only operations when write access is disabled.",
        ),
        cluster_id: str = Field(
            ...,
            description="ID of the EMR cluster.",
        ),
        step_id: Optional[str] = Field(
            None,
            description="ID of the EMR step (required for describe-step).",
        ),
        step_ids: Optional[List[str]] = Field(
            None,
            description="List of EMR step IDs (required for cancel-steps, optional for list-steps).",
        ),
        steps: Optional[List[Dict[str, Any]]] = Field(
            None,
            description="List of steps to add to the cluster (required for add-steps). Each step should include Name, ActionOnFailure, and HadoopJarStep.",
        ),
        step_states: Optional[List[str]] = Field(
            None,
            description="The step state filters to apply when listing steps (optional for list-steps). Valid values: PENDING, CANCEL_PENDING, RUNNING, COMPLETED, CANCELLED, FAILED, INTERRUPTED.",
        ),
        marker: Optional[str] = Field(
            None,
            description="The pagination token for list-steps operation.",
        ),
    ) -> Union[
        AddStepsResponse,
        CancelStepsResponse,
        DescribeStepResponse,
        ListStepsResponse,
    ]:
        """Manage AWS EMR EC2 steps for processing data on EMR clusters.

        This tool provides comprehensive operations for managing EMR steps, which are units of work
        submitted to an EMR cluster for execution. Steps typically consist of Hadoop or Spark jobs
        that process and analyze data.

        ## Requirements
        - The server must be run with the `--allow-write` flag for add-steps and cancel-steps operations
        - Appropriate AWS permissions for EMR step operations

        ## Operations
        - **add-steps**: Add new steps to a running EMR cluster (max 256 steps per job flow)
        - **cancel-steps**: Cancel pending or running steps on an EMR cluster (EMR 4.8.0+ except 5.0.0)
        - **describe-step**: Get detailed information about a specific step's configuration and status
        - **list-steps**: List and filter steps for an EMR cluster with pagination support

        ## Usage Tips
        - Each step consists of a JAR file, its main class, and arguments
        - Steps are executed in the order listed and must exit with zero code to be considered complete
        - For cancel-steps, you can specify SEND_INTERRUPT (default) or TERMINATE_PROCESS as cancellation option
        - When listing steps, filter by step states: PENDING, CANCEL_PENDING, RUNNING, COMPLETED, CANCELLED, FAILED, INTERRUPTED
        - For large result sets, use pagination with marker parameter

        ## Example
        ```
        # Add a Spark step to process data
        {
          "operation": "add-steps",
          "cluster_id": "j-2AXXXXXXGAPLF",
          "steps": [
            {
              "Name": "Spark Data Processing",
              "ActionOnFailure": "CONTINUE",
              "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": ["spark-submit", "--class", "com.example.SparkProcessor", "s3://mybucket/myapp.jar", "arg1", "arg2"]
              }
            }
          ]
        }
        ```

        Args:
            ctx: MCP context
            operation: Operation to perform
            cluster_id: ID of the EMR cluster
            step_id: ID of the EMR step
            step_ids: List of EMR step IDs
            steps: List of steps to add to the cluster
            step_states: The step state filters to apply when listing steps
            marker: The pagination token for list-steps operation

        Returns:
            Union of response types specific to the operation performed
        """
        try:
            if not self.allow_write and operation in [
                "add-steps",
                "cancel-steps",
            ]:
                error_message = (
                    f"Operation {operation} is not allowed without write access"
                )
                log_with_request_id(ctx, LogLevel.ERROR, error_message)

                if operation == "add-steps":
                    return AddStepsResponse(
                        isError=True,
                        content=[TextContent(type="text", text=error_message)],
                        cluster_id=cluster_id,
                        step_ids=[],
                        count=0,
                        operation="add",
                    )
                elif operation == "cancel-steps":
                    return CancelStepsResponse(
                        isError=True,
                        content=[TextContent(type="text", text=error_message)],
                        cluster_id=cluster_id,
                        step_cancellation_info=[],
                        count=0,
                        operation="cancel",
                    )

            if operation == "add-steps":
                if steps is None:
                    raise ValueError("steps is required for add-steps operation")

                # Prepare parameters
                params = {
                    "JobFlowId": cluster_id,
                    "Steps": steps,
                }

                # Add ExecutionRoleArn if provided in any step
                for step in steps:
                    if "ExecutionRoleArn" in step:
                        params["ExecutionRoleArn"] = step["ExecutionRoleArn"]
                        break

                # Add steps to the cluster
                response = self.emr_client.add_job_flow_steps(**params)

                step_ids = response.get("StepIds", [])
                steps_count = len(steps or [])
                return AddStepsResponse(
                    isError=False,
                    content=[
                        TextContent(
                            type="text",
                            text=f"Successfully added {steps_count} steps to EMR cluster {cluster_id}",
                        )
                    ],
                    cluster_id=cluster_id,
                    step_ids=step_ids,
                    count=len(step_ids),
                    operation="add",
                )

            elif operation == "cancel-steps":
                if step_ids is None:
                    raise ValueError("step_ids is required for cancel-steps operation")

                # Prepare parameters
                params = {
                    "ClusterId": cluster_id,
                    "StepIds": step_ids,
                }

                # Add StepCancellationOption if provided
                if "StepCancellationOption" in ctx.request.json:
                    step_cancellation_option = ctx.request.json.get(
                        "StepCancellationOption"
                    )
                    if step_cancellation_option in [
                        "SEND_INTERRUPT",
                        "TERMINATE_PROCESS",
                    ]:
                        params["StepCancellationOption"] = step_cancellation_option

                # Cancel steps
                response = self.emr_client.cancel_steps(**params)

                step_cancellation_info = response.get("CancelStepsInfoList", [])
                step_ids_count = len(step_ids or [])
                return CancelStepsResponse(
                    isError=False,
                    content=[
                        TextContent(
                            type="text",
                            text=f"Successfully initiated cancellation for {step_ids_count} steps on EMR cluster {cluster_id}",
                        )
                    ],
                    cluster_id=cluster_id,
                    step_cancellation_info=step_cancellation_info,
                    count=len(step_cancellation_info),
                    operation="cancel",
                )

            elif operation == "describe-step":
                if step_id is None:
                    raise ValueError("step_id is required for describe-step operation")

                # Describe step
                response = self.emr_client.describe_step(
                    ClusterId=cluster_id,
                    StepId=step_id,
                )

                return DescribeStepResponse(
                    isError=False,
                    content=[
                        TextContent(
                            type="text",
                            text=f"Successfully described step {step_id} on EMR cluster {cluster_id}",
                        )
                    ],
                    cluster_id=cluster_id,
                    step=response.get("Step", {}),
                    operation="describe",
                )

            elif operation == "list-steps":
                # Prepare parameters
                params = {
                    "ClusterId": cluster_id,
                }

                # Note: StepStates and StepIds parameters are optional filters
                # If they cause type errors, we'll skip them for now
                # TODO: Investigate proper handling of list parameters in boto3

                if marker is not None:
                    params["Marker"] = marker

                # Add StepStates if provided
                if step_states is not None:
                    params["StepStates"] = step_states

                # Add StepIds if provided
                if step_ids is not None:
                    params["StepIds"] = step_ids

                # List steps
                response = self.emr_client.list_steps(**params)

                steps = response.get("Steps", [])
                return ListStepsResponse(
                    isError=False,
                    content=[
                        TextContent(
                            type="text",
                            text=f"Successfully listed steps for EMR cluster {cluster_id}",
                        )
                    ],
                    cluster_id=cluster_id,
                    steps=steps,
                    count=len(steps),
                    marker=response.get("Marker"),
                    operation="list",
                )

            else:
                error_message = f"Invalid operation: {operation}. Must be one of: add-steps, cancel-steps, describe-step, list-steps"
                log_with_request_id(ctx, LogLevel.ERROR, error_message)
                return DescribeStepResponse(
                    isError=True,
                    content=[TextContent(type="text", text=error_message)],
                    cluster_id=cluster_id,
                    step={},
                    operation="describe",
                )

        except ValueError as e:
            log_with_request_id(
                ctx, LogLevel.ERROR, f"Parameter validation error: {str(e)}"
            )
            raise
        except Exception as e:
            error_message = f"Error in manage_aws_emr_ec2_steps: {str(e)}"
            log_with_request_id(ctx, LogLevel.ERROR, error_message)
            return DescribeStepResponse(
                isError=True,
                content=[TextContent(type="text", text=error_message)],
                cluster_id=cluster_id,
                step={},
                operation="describe",
            )
