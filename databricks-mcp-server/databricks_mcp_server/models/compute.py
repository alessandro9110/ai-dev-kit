"""
Pydantic models for Compute tool inputs.
"""
from pydantic import Field

from .base import MCPInputModel
from .common import LanguageEnum


class CreateContextInput(MCPInputModel):
    """Input model for creating an execution context."""
    cluster_id: str = Field(..., description="ID of the cluster")
    language: LanguageEnum = Field(LanguageEnum.PYTHON, description="Programming language for the context")


class ExecuteCommandWithContextInput(MCPInputModel):
    """Input model for executing code using an existing context."""
    cluster_id: str = Field(..., description="ID of the cluster")
    context_id: str = Field(..., description="Context ID from create_context")
    code: str = Field(..., description="Code to execute")


class DestroyContextInput(MCPInputModel):
    """Input model for destroying an execution context."""
    cluster_id: str = Field(..., description="ID of the cluster")
    context_id: str = Field(..., description="Context ID to destroy")


class DatabricksCommandInput(MCPInputModel):
    """Input model for executing a one-off command (creates and destroys context)."""
    cluster_id: str = Field(..., description="ID of the cluster")
    language: LanguageEnum = Field(LanguageEnum.PYTHON, description="Programming language for execution")
    code: str = Field(..., description="Code to execute")
    timeout: int = Field(120, description="Timeout in seconds", ge=1, le=3600)
