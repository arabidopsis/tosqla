from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from sqlalchemy import inspect
from sqlalchemy import String


class DataType(Enum):
    """Column data types."""

    INTEGER = "INTEGER"
    STRING = "STRING"
    ENUM = "ENUM"
    DECIMAL = "DECIMAL"
    FLOAT = "FLOAT"
    BLOB = "BLOB"
    DATETIME = "DATETIME"


@dataclass
class ColumnMetadata:
    """Metadata for a single column."""

    name: str
    data_type: DataType
    nullable: bool = True
    unique: bool = False
    indexed: bool = False
    primary_key: bool = False
    max_length: int = 0
    enum_values: list[str] | None = None
    default: Any = None


class DynamicSchema:
    """Dynamic schema for SQLAlchemy models."""

    def __init__(
        self,
        columns: dict[str, ColumnMetadata] | list[ColumnMetadata],
        columns_dict: dict[str, ColumnMetadata] | None = None,
    ):
        """Initialize schema with columns.

        Args:
            columns: Either a dict of name -> ColumnMetadata, or a list of ColumnMetadata
            columns_dict: If columns is a list, this dict of name -> ColumnMetadata is used
        """
        # Handle both interfaces:
        # 1. DynamicSchema(dict)
        # 2. DynamicSchema(list, dict)
        if isinstance(columns, dict):
            self.columns = columns
        elif isinstance(columns, list) and columns_dict is not None:
            self.columns = columns_dict
        elif isinstance(columns, list):
            # Convert list to dict
            self.columns = {col.name: col for col in columns}
        else:
            self.columns = columns or {}
        self._column_list: list[ColumnMetadata] | None = None

    @classmethod
    def from_model(cls, model_class: type) -> DynamicSchema:
        """Create schema from SQLAlchemy model class.

        Args:
            model_class: SQLAlchemy model class

        Returns:
            DynamicSchema instance
        """
        mapper = inspect(model_class)
        from .mysqla import column_name  # Import here to avoid circular import

        columns = {}

        for column in mapper.columns:
            col_name = column_name(column.name)
            data_type = cls._get_data_type(column)
            nullable = column.nullable
            unique = column.unique or False
            indexed = column.index or False
            primary_key = column.primary_key
            max_length = cls._get_max_length(column)
            enum_values = cls._get_enum_values(column)

            columns[col_name] = ColumnMetadata(
                name=col_name,
                data_type=data_type,
                nullable=nullable,
                unique=unique,
                indexed=indexed,
                primary_key=primary_key,
                max_length=max_length,
                enum_values=enum_values,
                default=column.default,
            )

        return cls(columns)

    def get_columns(self) -> list[ColumnMetadata]:
        """Get list of all columns."""
        if self._column_list is None:
            self._column_list = sorted(
                self.columns.values(),
                key=lambda c: (not c.primary_key, c.name),
            )
        return self._column_list

    def get_column_by_name(self, name: str) -> ColumnMetadata | None:
        """Get column metadata by name."""
        return self.columns.get(name)

    def get_required_columns(self) -> list[ColumnMetadata]:
        """Get non-nullable columns."""
        return [col for col in self.get_columns() if not col.nullable]

    def get_nullable_columns(self) -> list[ColumnMetadata]:
        """Get nullable columns."""
        return [col for col in self.get_columns() if col.nullable]

    def get_unique_columns(self) -> list[ColumnMetadata]:
        """Get columns with unique constraint."""
        return [col for col in self.get_columns() if col.unique]

    def get_enum_columns(self) -> list[ColumnMetadata]:
        """Get enum columns."""
        return [col for col in self.get_columns() if col.data_type == DataType.ENUM]

    def validate_value(self, column_name: str, value: Any) -> tuple[bool, str | None]:
        """Validate a value for a column.

        Args:
            column_name: Column name
            value: Value to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        col = self.get_column_by_name(column_name)
        if not col:
            return False, f"Column '{column_name}' not found in schema"

        # Type check
        if col.data_type == DataType.STRING:
            if not isinstance(value, str):
                return False, f"Expected string, got {type(value).__name__}"
            if col.max_length and len(value) > col.max_length:
                return False, f"String exceeds max length of {col.max_length}"
        elif col.data_type == DataType.INTEGER:
            try:
                int(value) if isinstance(value, str) else value
            except (ValueError, TypeError):
                return False, f"Expected integer, got {type(value).__name__}"
        elif col.data_type == DataType.DECIMAL or col.data_type == DataType.FLOAT:
            try:
                float(value) if isinstance(value, str) else value
            except (ValueError, TypeError):
                return False, f"Expected decimal/float, got {type(value).__name__}"
        elif col.data_type == DataType.ENUM:
            if col.enum_values and value not in col.enum_values:
                return False, f"Value must be one of {col.enum_values}"

        return True, None

    @classmethod
    def _get_data_type(cls, column: Any) -> DataType:
        """Get DataType from SQLAlchemy column type."""
        col_type = column.type
        type_name = type(col_type).__name__.upper()

        if "STRING" in type_name or "VARCHAR" in type_name:
            return DataType.STRING
        elif "INT" in type_name:
            return DataType.INTEGER
        elif "ENUM" in type_name:
            return DataType.ENUM
        elif "DECIMAL" in type_name:
            return DataType.DECIMAL
        elif "DATETIME" in type_name:
            return DataType.DATETIME
        elif "FLOAT" in type_name or "REAL" in type_name or "DOUBLE" in type_name:
            return DataType.FLOAT
        elif "BLOB" in type_name or "BINARY" in type_name or "BYTES" in type_name:
            return DataType.BLOB
        else:
            # Default to string for unknown types
            return DataType.STRING

    @classmethod
    def _get_max_length(cls, column: Any) -> int:
        """Get max length from String column type."""
        col_type = column.type
        if isinstance(col_type, String):
            return col_type.length or 0
        return 0

    @classmethod
    def _get_enum_values(cls, column: Any) -> list[str]:
        """Get enum values from Enum column type."""
        col_type = column.type
        type_name = type(col_type).__name__.upper()
        if "ENUM" not in type_name:
            return []

        # Extract enum values from SQLAlchemy Enum type
        if hasattr(col_type, "enums"):
            return list(col_type.enums)
        elif hasattr(col_type, "enum_class"):
            return [e.value for e in col_type.enum_class]

        return []
