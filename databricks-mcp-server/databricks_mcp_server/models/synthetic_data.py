"""
Pydantic models for Synthetic Data Generation tool inputs.
"""
from typing import Optional
from pydantic import Field

from .base import MCPInputModel
from .common import TemplateTypeEnum


class GetSynthDataTemplateInput(MCPInputModel):
    """Input model for getting a synthetic data generation template."""
    template_type: TemplateTypeEnum = Field(
        TemplateTypeEnum.STORY,
        description="Template type: 'story' (reference implementation) or 'empty' (scaffold)"
    )
    template_root: Optional[str] = Field(None, description="Optional override for template folder root")


class WriteSynthDataScriptInput(MCPInputModel):
    """Input model for writing generate_data.py script to Databricks workspace."""
    workspace_path: str = Field(
        ...,
        description="Workspace path (e.g., '/Workspace/Users/user@example.com/generate_data.py')"
    )
    code: str = Field(..., description="Python code content for generate_data.py")
    overwrite: bool = Field(True, description="Overwrite if file exists")


class GenerateAndUploadSynthDataInput(MCPInputModel):
    """Input model for executing generate_data.py on cluster and writing to Volume."""
    cluster_id: str = Field(..., description="Databricks cluster ID")
    workspace_path: str = Field(..., description="Workspace path to generate_data.py script")
    catalog: str = Field(..., description="Unity Catalog name")
    schema: str = Field(..., description="Schema name")
    volume: str = Field(..., description="Volume name")
    scale_factor: float = Field(
        1.0,
        description="Scale factor for data generation (multiplier for row counts)",
        ge=0.1,
        le=100.0
    )
    remote_subfolder: str = Field("incoming_data", description="Subfolder within volume for output")
    clean: bool = Field(True, description="Clean target folder before generation")
    timeout_sec: int = Field(600, description="Execution timeout in seconds", ge=1, le=3600)
