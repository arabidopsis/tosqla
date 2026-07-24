from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any
from typing import TYPE_CHECKING

from sqlalchemy import inspect
from sqlalchemy import String

if TYPE_CHECKING:
    from sqlalchemy import Column
    from sqlalchemy.orm import Mapper
    from sqlalchemy.orm import DeclarativeBase


# change these in sqlamodels/templates/meta.py.tmplt as well
class DataType(Enum):
    """Column data types."""

    INTEGER = "INTEGER"
    STRING = "STRING"
    ENUM = "ENUM"
    DECIMAL = "DECIMAL"
    FLOAT = "FLOAT"
    BLOB = "BLOB"
    DATETIME = "DATETIME"
    DATE = "DATE"


# change these in sqlamodels/templates/meta.py.tmplt as well
@dataclass(slots=True, frozen=True)
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
        class_name: str,
        columns: dict[str, ColumnMetadata],
    ):
        """Initialize schema with class name and columns.

        Args:
            class_name: Name of the class
            columns: A dict of name -> ColumnMetadata
        """
        self.class_name = class_name
        self.columns = columns
        self._column_list: list[ColumnMetadata] | None = None

    @classmethod
    def from_model(
        cls,
        class_name: str,
        model_class: type[DeclarativeBase],
    ) -> DynamicSchema:
        """Create schema from SQLAlchemy model class.

        Args:
            model_class: SQLAlchemy model class

        Returns:
            DynamicSchema instance
        """
        from .mysqla import column_name  # Import here to avoid circular import

        mapper: Mapper = inspect(model_class)

        columns: dict[str, ColumnMetadata] = {}
        column: Column[Any]

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
                nullable=bool(nullable),
                unique=unique,
                indexed=bool(indexed),
                primary_key=primary_key,
                max_length=max_length,
                enum_values=enum_values,
                default=column.default,
            )

        return cls(class_name=class_name, columns=columns)

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
        return self.columns.get(name, None)

    @classmethod
    def _get_data_type(cls, column: Column) -> DataType:
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
        elif "DATE" in type_name:
            return DataType.DATE
        elif "FLOAT" in type_name or "REAL" in type_name or "DOUBLE" in type_name:
            return DataType.FLOAT
        elif "BLOB" in type_name or "BINARY" in type_name or "BYTES" in type_name:
            return DataType.BLOB
        else:
            # Default to string for unknown types
            return DataType.STRING

    @classmethod
    def _get_max_length(cls, column: Column) -> int:
        """Get max length from String column type."""
        col_type = column.type
        if isinstance(col_type, String):
            return col_type.length or 0
        return 0

    @classmethod
    def _get_enum_values(cls, column: Column) -> list[str]:
        """Get enum values from Enum column type."""
        col_type = column.type
        type_name = type(col_type).__name__.upper()
        if "ENUM" not in type_name:
            return []

        # Extract enum values from SQLAlchemy Enum type
        if hasattr(col_type, "enums"):
            return list(col_type.enums)  # type: ignore
        elif hasattr(col_type, "enum_class"):
            return [e.value for e in col_type.enum_class]  # type: ignore

        return []
