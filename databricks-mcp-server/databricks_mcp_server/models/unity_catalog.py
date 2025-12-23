"""
Pydantic models for Unity Catalog tool inputs.
"""
from typing import Optional, List
from pydantic import Field, model_validator

from .base import MCPInputModel
from .common import ColumnModel, TableTypeEnum


# Catalog Operations
class ListCatalogsInput(MCPInputModel):
    """Input model for listing Unity Catalog catalogs (no parameters)."""
    pass


class GetCatalogInput(MCPInputModel):
    """Input model for getting a catalog."""
    catalog_name: str = Field(..., description="Name of the catalog")


class CreateCatalogInput(MCPInputModel):
    """Input model for creating a catalog."""
    catalog_name: str = Field(..., description="Name of the new catalog")
    comment: Optional[str] = Field(None, description="Optional catalog description")


class UpdateCatalogInput(MCPInputModel):
    """Input model for updating a catalog."""
    catalog_name: str = Field(..., description="Current name of the catalog")
    new_name: Optional[str] = Field(None, description="New catalog name (for renaming)")
    comment: Optional[str] = Field(None, description="New catalog description")

    @model_validator(mode='after')
    def validate_at_least_one_update(self):
        """Ensure at least one update field is provided."""
        if not any([self.new_name, self.comment]):
            raise ValueError("At least one of 'new_name' or 'comment' must be provided")
        return self


class DeleteCatalogInput(MCPInputModel):
    """Input model for deleting a catalog."""
    catalog_name: str = Field(..., description="Name of the catalog to delete")
    force: bool = Field(False, description="Force delete even if catalog contains schemas/tables")


# Schema Operations
class ListSchemasInput(MCPInputModel):
    """Input model for listing schemas in a catalog."""
    catalog_name: str = Field(..., description="Name of the catalog")


class CreateSchemaInput(MCPInputModel):
    """Input model for creating a schema."""
    catalog_name: str = Field(..., description="Name of the catalog")
    schema_name: str = Field(..., description="Name of the new schema")
    comment: Optional[str] = Field(None, description="Optional schema description")


class UpdateSchemaInput(MCPInputModel):
    """Input model for updating a schema."""
    catalog_name: str = Field(..., description="Name of the catalog")
    schema_name: str = Field(..., description="Current name of the schema")
    new_name: Optional[str] = Field(None, description="New schema name (for renaming)")
    comment: Optional[str] = Field(None, description="New schema description")

    @model_validator(mode='after')
    def validate_at_least_one_update(self):
        """Ensure at least one update field is provided."""
        if not any([self.new_name, self.comment]):
            raise ValueError("At least one of 'new_name' or 'comment' must be provided")
        return self


class DeleteSchemaInput(MCPInputModel):
    """Input model for deleting a schema."""
    catalog_name: str = Field(..., description="Name of the catalog")
    schema_name: str = Field(..., description="Name of the schema to delete")
    force: bool = Field(False, description="Force delete even if schema contains tables")


# Table Operations
class ListTablesInput(MCPInputModel):
    """Input model for listing tables in a schema."""
    catalog_name: str = Field(..., description="Name of the catalog")
    schema_name: str = Field(..., description="Name of the schema")


class CreateTableInput(MCPInputModel):
    """Input model for creating a Unity Catalog table."""
    catalog_name: str = Field(..., description="Name of the catalog")
    schema_name: str = Field(..., description="Name of the schema")
    table_name: str = Field(..., description="Name of the new table")
    columns: List[ColumnModel] = Field(..., description="List of column definitions", min_length=1)
    table_type: TableTypeEnum = Field(
        TableTypeEnum.MANAGED,
        description="Table type: 'MANAGED' or 'EXTERNAL'"
    )
    comment: Optional[str] = Field(None, description="Optional table description")
    storage_location: Optional[str] = Field(
        None,
        description="Storage location (required for EXTERNAL tables)"
    )

    @model_validator(mode='after')
    def validate_external_storage(self):
        """Ensure storage_location is provided for EXTERNAL tables."""
        if self.table_type == TableTypeEnum.EXTERNAL and not self.storage_location:
            raise ValueError("storage_location is required for EXTERNAL tables")
        return self
