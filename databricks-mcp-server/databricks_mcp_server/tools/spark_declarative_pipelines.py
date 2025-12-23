"""
MCP tool wrappers for Spark Declarative Pipelines operations.
"""
from typing import Dict, Any
from pydantic import ValidationError
from databricks_mcp_core.spark_declarative_pipelines import (
    pipelines, workspace_files
)
import json

from ..models.base import MCPValidationError
from ..models.pipelines import (
    CreatePipelineInput,
    GetPipelineInput,
    UpdatePipelineInput,
    DeletePipelineInput,
    StartPipelineUpdateInput,
    ValidatePipelineInput,
    GetPipelineUpdateStatusInput,
    StopPipelineInput,
    GetPipelineEventsInput,
    ListPipelineFilesInput,
    GetPipelineFileStatusInput,
    ReadPipelineFileInput,
    WritePipelineFileInput,
    CreatePipelineDirectoryInput,
    DeletePipelinePathInput
)


# Pipeline Management Tools

def create_pipeline_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Create a new Spark Declarative Pipeline."""
    try:
        input = CreatePipelineInput(**arguments)

        result = pipelines.create_pipeline(
            name=input.name,
            root_path=input.root_path,
            catalog=input.catalog,
            schema=input.schema,
            workspace_notebook_paths=input.workspace_notebook_paths,
            serverless=input.serverless
        )

        pipeline_id = result.pipeline_id
        return {
            "content": [
                {
                    "type": "text",
                    "text": (
                        f"✅ Pipeline created successfully!\n\n"
                        f"Pipeline ID: {pipeline_id}\n"
                        f"Name: {input.name}\n"
                        f"Target: {input.catalog}.{input.schema}\n"
                        f"Storage: {input.root_path}\n"
                        f"Serverless: {input.serverless}"
                    )
                }
            ]
        }
    except ValidationError as e:
        return MCPValidationError(e).to_mcp_response()
    except Exception as e:
        return {
            "content": [{
                "type": "text",
                "text": f"❌ Error creating pipeline: {str(e)}"
            }],
            "isError": True
        }


def get_pipeline_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Get pipeline details and configuration."""
    try:
        input = GetPipelineInput(**arguments)

        result = pipelines.get_pipeline(input.pipeline_id)

        name = result.name if result.name else "unknown"
        state = result.state if result.state else "unknown"
        target = result.spec.target if result.spec else "unknown"

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"📊 Pipeline Details\n\n"
                           f"Name: {name}\n"
                           f"State: {state}\n"
                           f"Target: {target}\n"
                           f"Pipeline ID: {input.pipeline_id}\n\n"
                           f"Full configuration:\n{json.dumps(result.as_dict(), indent=2)}"
                }
            ]
        }
    except ValidationError as e:
        return MCPValidationError(e).to_mcp_response()
    except Exception as e:
        return {
            "content": [{
                "type": "text",
                "text": f"❌ Error getting pipeline: {str(e)}"
            }],
            "isError": True
        }


def update_pipeline_config_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Update pipeline configuration (not code files)."""
    try:
        input = UpdatePipelineInput(**arguments)

        pipelines.update_pipeline(
            pipeline_id=input.pipeline_id,
            name=input.name,
            root_path=input.root_path,
            catalog=input.catalog,
            schema=input.schema,
            workspace_notebook_paths=input.workspace_notebook_paths,
            serverless=input.serverless
        )

        return {
            "content": [
                {
                    "type": "text",
                    "text": (
                        "✅ Pipeline configuration updated successfully!\n\n"
                        f"Pipeline ID: {input.pipeline_id}"
                    )
                }
            ]
        }
    except ValidationError as e:
        return MCPValidationError(e).to_mcp_response()
    except Exception as e:
        return {
            "content": [{
                "type": "text",
                "text": f"❌ Error updating pipeline: {str(e)}"
            }],
            "isError": True
        }


def delete_pipeline_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Delete a pipeline."""
    try:
        input = DeletePipelineInput(**arguments)

        pipelines.delete_pipeline(input.pipeline_id)

        return {
            "content": [
                {
                    "type": "text",
                    "text": "✅ Pipeline deleted successfully!\n\n"
                           f"Pipeline ID: {input.pipeline_id}"
                }
            ]
        }
    except ValidationError as e:
        return MCPValidationError(e).to_mcp_response()
    except Exception as e:
        return {
            "content": [{
                "type": "text",
                "text": f"❌ Error deleting pipeline: {str(e)}"
            }],
            "isError": True
        }


def start_pipeline_update_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Start a pipeline update (full or incremental refresh)."""
    try:
        input = StartPipelineUpdateInput(**arguments)

        update_id = pipelines.start_update(
            pipeline_id=input.pipeline_id,
            refresh_selection=input.refresh_selection,
            full_refresh=input.full_refresh,
            full_refresh_selection=input.full_refresh_selection,
            validate_only=False
        )

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"✅ Pipeline update started!\n\n"
                           f"Pipeline ID: {input.pipeline_id}\n"
                           f"Update ID: {update_id}\n"
                           f"Full Refresh: {input.full_refresh}\n\n"
                           f"Use get_pipeline_update_status to poll."
                }
            ]
        }
    except ValidationError as e:
        return MCPValidationError(e).to_mcp_response()
    except Exception as e:
        return {
            "content": [{
                "type": "text",
                "text": f"❌ Error starting update: {str(e)}"
            }],
            "isError": True
        }


def validate_pipeline_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Perform dry-run validation without updating datasets."""
    try:
        input = ValidatePipelineInput(**arguments)

        update_id = pipelines.start_update(
            pipeline_id=input.pipeline_id,
            refresh_selection=input.refresh_selection,
            full_refresh=input.full_refresh,
            full_refresh_selection=input.full_refresh_selection,
            validate_only=True
        )

        return {
            "content": [
                {
                    "type": "text",
                    "text": "✅ Pipeline validation started (dry-run mode)!\n\n"
                           f"Pipeline ID: {input.pipeline_id}\n"
                           f"Update ID: {update_id}\n\n"
                           f"Use get_pipeline_update_status to check results."
                }
            ]
        }
    except ValidationError as e:
        return MCPValidationError(e).to_mcp_response()
    except Exception as e:
        return {
            "content": [{
                "type": "text",
                "text": f"❌ Error starting validation: {str(e)}"
            }],
            "isError": True
        }


def get_pipeline_update_status_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Get pipeline update status and results."""
    try:
        input = GetPipelineUpdateStatusInput(**arguments)

        result = pipelines.get_update(
            input.pipeline_id,
            input.update_id
        )

        state = result.update.state if result.update else "unknown"

        # Format based on state
        emoji = {
            "QUEUED": "⏳",
            "RUNNING": "🔄",
            "COMPLETED": "✅",
            "FAILED": "❌",
            "CANCELED": "🚫"
        }.get(state, "❓")

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"{emoji} Pipeline Update Status\n\n"
                           f"State: {state}\n"
                           f"Pipeline ID: {input.pipeline_id}\n"
                           f"Update ID: {input.update_id}\n\n"
                           f"Full status:\n{json.dumps(result.as_dict(), indent=2)}"
                }
            ]
        }
    except ValidationError as e:
        return MCPValidationError(e).to_mcp_response()
    except Exception as e:
        return {
            "content": [{
                "type": "text",
                "text": f"❌ Error getting update status: {str(e)}"
            }],
            "isError": True
        }


def stop_pipeline_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Stop a running pipeline."""
    try:
        input = StopPipelineInput(**arguments)

        pipelines.stop_pipeline(input.pipeline_id)

        return {
            "content": [
                {
                    "type": "text",
                    "text": "✅ Pipeline stop request sent!\n\n"
                           f"Pipeline ID: {input.pipeline_id}"
                }
            ]
        }
    except ValidationError as e:
        return MCPValidationError(e).to_mcp_response()
    except Exception as e:
        return {
            "content": [{
                "type": "text",
                "text": f"❌ Error stopping pipeline: {str(e)}"
            }],
            "isError": True
        }


def get_pipeline_events_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Get pipeline events, issues, and error messages."""
    try:
        input = GetPipelineEventsInput(**arguments)

        events = pipelines.get_pipeline_events(
            input.pipeline_id,
            input.max_results
        )

        event_count = len(events)
        # Count errors - events have event_type property
        error_count = sum(
            1 for e in events
            if e.event_type and e.event_type.endswith("_failed")
        )

        # Convert to dict for JSON serialization
        events_dict = [e.as_dict() for e in events]

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"📋 Pipeline Events\n\n"
                           f"Pipeline ID: {input.pipeline_id}\n"
                           f"Total Events: {event_count}\n"
                           f"Error Events: {error_count}\n\n"
                           f"Events:\n{json.dumps(events_dict, indent=2)}"
                }
            ]
        }
    except ValidationError as e:
        return MCPValidationError(e).to_mcp_response()
    except Exception as e:
        return {
            "content": [{
                "type": "text",
                "text": f"❌ Error getting pipeline events: {str(e)}"
            }],
            "isError": True
        }


# Workspace File Tools

def list_pipeline_files_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """List files in pipeline workspace directory."""
    try:
        input = ListPipelineFilesInput(**arguments)

        files = workspace_files.list_files(input.path)

        file_count = len(files)
        return {
            "content": [
                {
                    "type": "text",
                    "text": f"📁 Files in {input.path}\n\n"
                           f"Total: {file_count}\n\n"
                           + "\n".join(
                               f"- {f.path} ({f.object_type})"
                               for f in files
                           )
                }
            ]
        }
    except ValidationError as e:
        return MCPValidationError(e).to_mcp_response()
    except Exception as e:
        return {
            "content": [{
                "type": "text",
                "text": f"❌ Error listing files: {str(e)}"
            }],
            "isError": True
        }


def get_pipeline_file_status_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Get pipeline file metadata."""
    try:
        input = GetPipelineFileStatusInput(**arguments)

        status = workspace_files.get_file_status(input.path)

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"📄 File Status\n\n"
                           f"Path: {status.path}\n"
                           f"Type: {status.object_type}\n"
                           f"Language: {status.language or 'N/A'}\n"
                           f"Size: {status.size or 'N/A'} bytes"
                }
            ]
        }
    except ValidationError as e:
        return MCPValidationError(e).to_mcp_response()
    except Exception as e:
        return {
            "content": [{
                "type": "text",
                "text": f"❌ Error getting file status: {str(e)}"
            }],
            "isError": True
        }


def read_pipeline_file_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Read pipeline file contents."""
    try:
        input = ReadPipelineFileInput(**arguments)

        content = workspace_files.read_file(input.path)

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"📄 File: {input.path}\n\n"
                           f"```\n{content}\n```"
                }
            ]
        }
    except ValidationError as e:
        return MCPValidationError(e).to_mcp_response()
    except Exception as e:
        return {
            "content": [{
                "type": "text",
                "text": f"❌ Error reading file: {str(e)}"
            }],
            "isError": True
        }


def write_pipeline_file_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Write or update pipeline file."""
    try:
        input = WritePipelineFileInput(**arguments)

        workspace_files.write_file(
            path=input.path,
            content=input.content,
            language=input.language,
            overwrite=input.overwrite
        )

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"✅ File written successfully!\n\n"
                           f"Path: {input.path}\n"
                           f"Language: {input.language}"
                }
            ]
        }
    except ValidationError as e:
        return MCPValidationError(e).to_mcp_response()
    except Exception as e:
        return {
            "content": [{
                "type": "text",
                "text": f"❌ Error writing file: {str(e)}"
            }],
            "isError": True
        }


def create_pipeline_directory_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Create pipeline workspace directory."""
    try:
        input = CreatePipelineDirectoryInput(**arguments)

        workspace_files.create_directory(input.path)

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"✅ Directory created successfully!\n\n"
                           f"Path: {input.path}"
                }
            ]
        }
    except ValidationError as e:
        return MCPValidationError(e).to_mcp_response()
    except Exception as e:
        return {
            "content": [{
                "type": "text",
                "text": f"❌ Error creating directory: {str(e)}"
            }],
            "isError": True
        }


def delete_pipeline_path_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Delete pipeline file or directory."""
    try:
        input = DeletePipelinePathInput(**arguments)

        workspace_files.delete_path(
            input.path,
            input.recursive
        )

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"✅ Path deleted successfully!\n\n"
                           f"Path: {input.path}"
                }
            ]
        }
    except ValidationError as e:
        return MCPValidationError(e).to_mcp_response()
    except Exception as e:
        return {
            "content": [{
                "type": "text",
                "text": f"❌ Error deleting path: {str(e)}"
            }],
            "isError": True
        }


# Tool handler mapping
TOOL_HANDLERS = {
    "create_pipeline": create_pipeline_tool,
    "get_pipeline": get_pipeline_tool,
    "update_pipeline_config": update_pipeline_config_tool,
    "delete_pipeline": delete_pipeline_tool,
    "start_pipeline_update": start_pipeline_update_tool,
    "validate_pipeline": validate_pipeline_tool,
    "get_pipeline_update_status": get_pipeline_update_status_tool,
    "stop_pipeline": stop_pipeline_tool,
    "get_pipeline_events": get_pipeline_events_tool,
    "list_pipeline_files": list_pipeline_files_tool,
    "get_pipeline_file_status": get_pipeline_file_status_tool,
    "read_pipeline_file": read_pipeline_file_tool,
    "write_pipeline_file": write_pipeline_file_tool,
    "create_pipeline_directory": create_pipeline_directory_tool,
    "delete_pipeline_path": delete_pipeline_path_tool,
}


def get_tool_definitions():
    """Return MCP tool definitions for SDP operations."""
    return [
        {
            "name": "create_pipeline",
            "description": "Create a new Spark Declarative Pipeline with simplified, prescriptive interface",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Pipeline name"
                    },
                    "root_path": {
                        "type": "string",
                        "description": "Root storage location (e.g., 'dbfs:/pipelines/my_pipeline')"
                    },
                    "catalog": {
                        "type": "string",
                        "description": "Unity Catalog name"
                    },
                    "schema": {
                        "type": "string",
                        "description": "Schema name for output tables"
                    },
                    "workspace_notebook_paths": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of workspace notebook paths"
                    },
                    "serverless": {
                        "type": "boolean",
                        "description": "Use serverless compute",
                        "default": True
                    }
                },
                "required": ["name", "root_path", "catalog", "schema", "workspace_notebook_paths"]
            }
        },
        {
            "name": "get_pipeline",
            "description": "Get pipeline details",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pipeline_id": {"type": "string"}
                },
                "required": ["pipeline_id"]
            }
        },
        {
            "name": "update_pipeline_config",
            "description": "Update pipeline configuration with simplified interface",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pipeline_id": {
                        "type": "string",
                        "description": "Pipeline ID to update"
                    },
                    "name": {
                        "type": "string",
                        "description": "New pipeline name"
                    },
                    "root_path": {
                        "type": "string",
                        "description": "New storage location"
                    },
                    "catalog": {
                        "type": "string",
                        "description": "New catalog name"
                    },
                    "schema": {
                        "type": "string",
                        "description": "New schema name"
                    },
                    "workspace_notebook_paths": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "New list of workspace notebook paths"
                    },
                    "serverless": {
                        "type": "boolean",
                        "description": "Enable/disable serverless compute"
                    }
                },
                "required": ["pipeline_id"]
            }
        },
        {
            "name": "delete_pipeline",
            "description": "Delete a pipeline",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pipeline_id": {"type": "string"}
                },
                "required": ["pipeline_id"]
            }
        },
        {
            "name": "start_pipeline_update",
            "description": "Start pipeline update",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pipeline_id": {"type": "string"},
                    "refresh_selection": {"type": "array"},
                    "full_refresh": {"type": "boolean"},
                    "full_refresh_selection": {"type": "array"}
                },
                "required": ["pipeline_id"]
            }
        },
        {
            "name": "validate_pipeline",
            "description": "Validate pipeline (dry-run)",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pipeline_id": {"type": "string"},
                    "refresh_selection": {"type": "array"},
                    "full_refresh": {"type": "boolean"},
                    "full_refresh_selection": {"type": "array"}
                },
                "required": ["pipeline_id"]
            }
        },
        {
            "name": "get_pipeline_update_status",
            "description": "Get pipeline update status",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pipeline_id": {"type": "string"},
                    "update_id": {"type": "string"}
                },
                "required": ["pipeline_id", "update_id"]
            }
        },
        {
            "name": "stop_pipeline",
            "description": "Stop a running pipeline",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pipeline_id": {"type": "string"}
                },
                "required": ["pipeline_id"]
            }
        },
        {
            "name": "get_pipeline_events",
            "description": "Get pipeline events",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pipeline_id": {"type": "string"},
                    "max_results": {"type": "integer", "default": 100}
                },
                "required": ["pipeline_id"]
            }
        },
        {
            "name": "list_pipeline_files",
            "description": "List workspace files",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"}
                },
                "required": ["path"]
            }
        },
        {
            "name": "get_pipeline_file_status",
            "description": "Get file metadata",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"}
                },
                "required": ["path"]
            }
        },
        {
            "name": "read_pipeline_file",
            "description": "Read file contents",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"}
                },
                "required": ["path"]
            }
        },
        {
            "name": "write_pipeline_file",
            "description": "Write file contents",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "content": {"type": "string"},
                    "language": {"type": "string", "default": "PYTHON"},
                    "overwrite": {"type": "boolean", "default": True}
                },
                "required": ["path", "content"]
            }
        },
        {
            "name": "create_pipeline_directory",
            "description": "Create directory",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"}
                },
                "required": ["path"]
            }
        },
        {
            "name": "delete_pipeline_path",
            "description": "Delete file or directory",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "recursive": {"type": "boolean", "default": False}
                },
                "required": ["path"]
            }
        }
    ]
