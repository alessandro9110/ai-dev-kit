"""
Spark Declarative Pipelines - Pipeline Management

Functions for managing SDP pipeline lifecycle using Databricks Pipelines API.
"""
from typing import List, Optional, Dict
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import (
    CreatePipelineResponse,
    GetPipelineResponse,
    PipelineLibrary,
    NotebookLibrary,
    PipelineCluster,
    PipelineEvent,
    GetUpdateResponse,
    StartUpdateResponse
)


def create_pipeline(
    name: str,
    root_path: str,
    catalog: str,
    schema: str,
    workspace_notebook_paths: List[str],
    serverless: bool = True
) -> CreatePipelineResponse:
    """
    Create a new Spark Declarative Pipeline.

    Simplified, prescriptive interface with sensible defaults:
    - Serverless compute by default
    - Triggered mode (not continuous)
    - Simple notebook path list (no complex PipelineLibrary objects)

    Args:
        name: Pipeline name
        root_path: Root storage location (e.g., "dbfs:/pipelines/my_pipeline")
        catalog: Unity Catalog name
        schema: Schema name for output tables
        workspace_notebook_paths: List of workspace notebook paths
                   Example: ["/Workspace/Users/user@example.com/notebook.py"]
        serverless: Use serverless compute (default: True)

    Returns:
        CreatePipelineResponse with pipeline_id

    Raises:
        DatabricksError: If API request fails
    """
    w = WorkspaceClient()

    # Build libraries from simple notebook paths
    libraries = [
        PipelineLibrary(notebook=NotebookLibrary(path=path))
        for path in workspace_notebook_paths
    ]

    return w.pipelines.create(
        name=name,
        storage=root_path,
        target=f"{catalog}.{schema}",
        libraries=libraries,
        continuous=False,
        serverless=serverless
    )


def get_pipeline(pipeline_id: str) -> GetPipelineResponse:
    """
    Get pipeline details and configuration.

    Args:
        pipeline_id: Pipeline ID

    Returns:
        GetPipelineResponse with full pipeline configuration and state

    Raises:
        DatabricksError: If API request fails
    """
    w = WorkspaceClient()
    return w.pipelines.get(pipeline_id=pipeline_id)


def update_pipeline(
    pipeline_id: str,
    name: Optional[str] = None,
    root_path: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    workspace_notebook_paths: Optional[List[str]] = None,
    serverless: Optional[bool] = None
) -> None:
    """
    Update pipeline configuration (not code files).

    Simplified interface matching create_pipeline.

    Args:
        pipeline_id: Pipeline ID
        name: New pipeline name
        root_path: New storage location
        catalog: New catalog name
        schema: New schema name
        workspace_notebook_paths: New list of notebook paths
        serverless: New serverless setting

    Raises:
        DatabricksError: If API request fails
    """
    w = WorkspaceClient()

    # Build kwargs conditionally
    kwargs = {"pipeline_id": pipeline_id}

    if name:
        kwargs["name"] = name
    if root_path:
        kwargs["storage"] = root_path
    if catalog and schema:
        kwargs["target"] = f"{catalog}.{schema}"
    if workspace_notebook_paths:
        kwargs["libraries"] = [
            PipelineLibrary(notebook=NotebookLibrary(path=path))
            for path in workspace_notebook_paths
        ]
    if serverless is not None:
        kwargs["serverless"] = serverless

    w.pipelines.update(**kwargs)


def delete_pipeline(pipeline_id: str) -> None:
    """
    Delete a pipeline.

    Args:
        pipeline_id: Pipeline ID

    Raises:
        DatabricksError: If API request fails
    """
    w = WorkspaceClient()
    w.pipelines.delete(pipeline_id=pipeline_id)


def start_update(
    pipeline_id: str,
    refresh_selection: Optional[List[str]] = None,
    full_refresh: bool = False,
    full_refresh_selection: Optional[List[str]] = None,
    validate_only: bool = False
) -> str:
    """
    Start a pipeline update or dry-run validation.

    Args:
        pipeline_id: Pipeline ID
        refresh_selection: List of table names to refresh
        full_refresh: If True, performs full refresh
        full_refresh_selection: List of table names for full refresh
        validate_only: If True, performs dry-run validation
                      without updating datasets

    Returns:
        Update ID for polling status

    Raises:
        DatabricksError: If API request fails
    """
    w = WorkspaceClient()

    response = w.pipelines.start_update(
        pipeline_id=pipeline_id,
        refresh_selection=refresh_selection,
        full_refresh=full_refresh,
        full_refresh_selection=full_refresh_selection,
        validate_only=validate_only
    )

    return response.update_id


def get_update(pipeline_id: str, update_id: str) -> GetUpdateResponse:
    """
    Get pipeline update status and results.

    Args:
        pipeline_id: Pipeline ID
        update_id: Update ID from start_update

    Returns:
        GetUpdateResponse with update status
        (state: QUEUED, RUNNING, COMPLETED, FAILED, etc.)

    Raises:
        DatabricksError: If API request fails
    """
    w = WorkspaceClient()
    return w.pipelines.get_update(
        pipeline_id=pipeline_id,
        update_id=update_id
    )


def stop_pipeline(pipeline_id: str) -> None:
    """
    Stop a running pipeline.

    Args:
        pipeline_id: Pipeline ID

    Raises:
        DatabricksError: If API request fails
    """
    w = WorkspaceClient()
    # SDK's stop() returns Wait object, but we don't need to wait
    w.pipelines.stop(pipeline_id=pipeline_id)


def get_pipeline_events(
    pipeline_id: str,
    max_results: int = 100
) -> List[PipelineEvent]:
    """
    Get pipeline events, issues, and error messages.

    Args:
        pipeline_id: Pipeline ID
        max_results: Maximum number of events to return

    Returns:
        List of PipelineEvent objects with error details

    Raises:
        DatabricksError: If API request fails
    """
    w = WorkspaceClient()
    # SDK returns iterator, convert to list with max_results limit
    events = w.pipelines.list_pipeline_events(
        pipeline_id=pipeline_id,
        max_results=max_results
    )
    return list(events)
