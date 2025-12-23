"""
Pydantic models for Spark Declarative Pipelines tool inputs.
"""
from typing import Optional, List, Dict, Any
from pydantic import Field

from .base import MCPInputModel
from .common import PipelineLibraryModel


# Pipeline Management
class CreatePipelineInput(MCPInputModel):
    """Input model for creating a new Spark Declarative Pipeline."""
    name: str = Field(..., description="Pipeline name")
    root_path: str = Field(
        ...,
        description="Root storage location (e.g., 'dbfs:/pipelines/my_pipeline')"
    )
    catalog: str = Field(..., description="Unity Catalog name")
    schema: str = Field(..., description="Schema name for output tables")
    workspace_notebook_paths: List[str] = Field(
        ...,
        description="List of workspace notebook paths",
        min_length=1
    )
    serverless: bool = Field(True, description="Use serverless compute")


class GetPipelineInput(MCPInputModel):
    """Input model for getting pipeline details."""
    pipeline_id: str = Field(..., description="Pipeline ID")


class UpdatePipelineInput(MCPInputModel):
    """Input model for updating a pipeline."""
    pipeline_id: str = Field(..., description="Pipeline ID to update")
    name: Optional[str] = Field(None, description="New pipeline name")
    root_path: Optional[str] = Field(None, description="New storage location")
    catalog: Optional[str] = Field(None, description="New catalog name")
    schema: Optional[str] = Field(None, description="New schema name")
    workspace_notebook_paths: Optional[List[str]] = Field(
        None,
        description="New list of workspace notebook paths"
    )
    serverless: Optional[bool] = Field(
        None,
        description="Enable/disable serverless compute"
    )


class DeletePipelineInput(MCPInputModel):
    """Input model for deleting a pipeline."""
    pipeline_id: str = Field(..., description="Pipeline ID to delete")


class StartPipelineUpdateInput(MCPInputModel):
    """Input model for starting a pipeline update."""
    pipeline_id: str = Field(..., description="Pipeline ID")
    refresh_selection: Optional[List[str]] = Field(None, description="Tables to refresh")
    full_refresh: bool = Field(False, description="Perform full refresh")
    full_refresh_selection: Optional[List[str]] = Field(None, description="Tables for full refresh")


class ValidatePipelineInput(MCPInputModel):
    """Input model for validating a pipeline (dry-run)."""
    pipeline_id: str = Field(..., description="Pipeline ID")
    refresh_selection: Optional[List[str]] = Field(None, description="Tables to validate")
    full_refresh: bool = Field(False, description="Validate with full refresh")
    full_refresh_selection: Optional[List[str]] = Field(None, description="Tables for full refresh validation")


class GetPipelineUpdateStatusInput(MCPInputModel):
    """Input model for getting pipeline update status."""
    pipeline_id: str = Field(..., description="Pipeline ID")
    update_id: str = Field(..., description="Update ID")


class StopPipelineInput(MCPInputModel):
    """Input model for stopping a running pipeline."""
    pipeline_id: str = Field(..., description="Pipeline ID to stop")


class GetPipelineEventsInput(MCPInputModel):
    """Input model for getting pipeline events."""
    pipeline_id: str = Field(..., description="Pipeline ID")
    max_results: int = Field(100, description="Maximum number of events to return", ge=1, le=1000)


# Workspace File Operations
class ListPipelineFilesInput(MCPInputModel):
    """Input model for listing files in pipeline workspace directory."""
    path: str = Field(..., description="Workspace directory path")


class GetPipelineFileStatusInput(MCPInputModel):
    """Input model for getting pipeline file metadata."""
    path: str = Field(..., description="Workspace file path")


class ReadPipelineFileInput(MCPInputModel):
    """Input model for reading pipeline file contents."""
    path: str = Field(..., description="Workspace file path")


class WritePipelineFileInput(MCPInputModel):
    """Input model for writing or updating a pipeline file."""
    path: str = Field(..., description="Workspace file path")
    content: str = Field(..., description="File content")
    language: str = Field("PYTHON", description="Programming language")
    overwrite: bool = Field(True, description="Overwrite if file exists")


class CreatePipelineDirectoryInput(MCPInputModel):
    """Input model for creating a pipeline workspace directory."""
    path: str = Field(..., description="Workspace directory path to create")


class DeletePipelinePathInput(MCPInputModel):
    """Input model for deleting a pipeline file or directory."""
    path: str = Field(..., description="Workspace path to delete")
    recursive: bool = Field(False, description="Recursively delete directory contents")
