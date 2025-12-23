"""
Base classes for MCP input validation.
"""
from functools import wraps
from pydantic import BaseModel, ValidationError, ConfigDict
from typing import Dict, Any, Callable, Type


class MCPValidationError(Exception):
    """Custom exception for MCP validation errors with formatted output."""

    def __init__(self, validation_error: ValidationError):
        self.validation_error = validation_error
        self.errors = validation_error.errors()
        super().__init__(str(validation_error))

    def to_mcp_response(self) -> Dict[str, Any]:
        """Convert ValidationError to MCP error response format."""
        error_msgs = []
        for error in self.errors:
            field = ".".join(str(loc) for loc in error["loc"])
            msg = error["msg"]
            error_type = error["type"]
            error_msgs.append(f"  - {field}: {msg} (type: {error_type})")

        return {
            "content": [{
                "type": "text",
                "text": "❌ Validation Error:\n" + "\n".join(error_msgs)
            }],
            "isError": True
        }


class MCPInputModel(BaseModel):
    """Base class for all MCP input models with common configuration."""

    model_config = ConfigDict(
        extra="ignore",  # Forward compatibility - ignore unknown fields
        use_enum_values=True,  # Automatically convert enums to their values
        validate_assignment=True,  # Validate on assignment
        str_strip_whitespace=True  # Strip whitespace from strings
    )


def validate_input(model: Type[MCPInputModel]):
    """
    Decorator to automatically validate MCP tool inputs with Pydantic models.

    This eliminates boilerplate validation code from tool handlers.

    Example:
        @validate_input(CreatePipelineInput)
        def create_pipeline_tool(input: CreatePipelineInput) -> Dict[str, Any]:
            # Input is already validated - just use it!
            return {"content": [...]}
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(arguments: Dict[str, Any]) -> Dict[str, Any]:
            try:
                validated_input = model(**arguments)
                return func(validated_input)
            except ValidationError as e:
                return MCPValidationError(e).to_mcp_response()
        return wrapper
    return decorator
