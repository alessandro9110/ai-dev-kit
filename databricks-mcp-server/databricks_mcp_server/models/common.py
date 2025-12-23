"""
Common Pydantic models shared across multiple tool modules.
"""
from typing import Optional
from enum import Enum
from pydantic import Field
from databricks.sdk.service.catalog import ColumnInfo, ColumnTypeName, TableType
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary

from .base import MCPInputModel


# Enums
class TableTypeEnum(str, Enum):
    """Table type enum for Unity Catalog tables."""
    MANAGED = "MANAGED"
    EXTERNAL = "EXTERNAL"


class LanguageEnum(str, Enum):
    """Programming language enum for compute contexts."""
    PYTHON = "python"
    SQL = "sql"
    SCALA = "scala"
    R = "r"


class TemplateTypeEnum(str, Enum):
    """Template type enum for synthetic data generation."""
    STORY = "story"
    EMPTY = "empty"


# Shared Models
class ColumnModel(MCPInputModel):
    """Reusable column model for Unity Catalog tables."""
    name: str = Field(..., description="Column name")
    type_name: str = Field(..., description="Column data type (e.g., 'STRING', 'INT', 'DOUBLE')")
    comment: Optional[str] = Field(None, description="Optional column comment/description")

    def to_sdk(self) -> ColumnInfo:
        """Convert to Databricks SDK ColumnInfo object."""
        return ColumnInfo(
            name=self.name,
            type_name=ColumnTypeName(self.type_name),
            comment=self.comment
        )


class NotebookLibraryModel(MCPInputModel):
    """Notebook library reference for DLT pipelines."""
    path: str = Field(..., description="Workspace path to the notebook")


class PipelineLibraryModel(MCPInputModel):
    """Pipeline library specification (notebook, file, jar, maven, etc.)."""
    notebook: Optional[NotebookLibraryModel] = Field(None, description="Notebook library")
    # TODO: Add other library types (file, jar, maven, etc.) as needed

    def to_sdk(self) -> PipelineLibrary:
        """Convert to Databricks SDK PipelineLibrary object."""
        if self.notebook:
            return PipelineLibrary(
                notebook=NotebookLibrary(path=self.notebook.path)
            )
        raise ValueError("At least one library type must be specified")


def table_type_to_sdk(table_type: TableTypeEnum) -> TableType:
    """Convert TableTypeEnum to Databricks SDK TableType."""
    if table_type == TableTypeEnum.MANAGED:
        return TableType.MANAGED
    elif table_type == TableTypeEnum.EXTERNAL:
        return TableType.EXTERNAL
    else:
        raise ValueError(f"Unknown table type: {table_type}")
