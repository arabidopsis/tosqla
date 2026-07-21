from __future__ import annotations

from enum import Enum
from typing import Any
from typing import IO

import click
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.inspection import inspect

from .mysqla import cli
# Schema metadata for SequenceInventoryAll model.
# This module provides detailed metadata about the SequenceInventoryAll table structure,
# including column types, constraints, and validation information.


class DataType(Enum):
    """Column data types."""

    INTEGER = "INTEGER"
    STRING = "STRING"
    ENUM = "ENUM"
    DECIMAL = "DECIMAL"
    BLOB = "BLOB"


class ColumnMetadata:
    """Metadata for a single column in SequenceInventoryAll."""

    def __init__(
        self,
        name: str,
        data_type: DataType,
        nullable: bool = True,
        unique: bool = False,
        indexed: bool = False,
        primary_key: bool = False,
        max_length: int | None = None,
        enum_values: list[str] | None = None,
        default: Any = None,
    ):
        """Initialize column metadata.

        Args:
            name: Column name
            data_type: Data type (from DataType enum)
            nullable: Whether NULL is allowed
            unique: Whether values must be unique
            indexed: Whether column is indexed
            primary_key: Whether this is primary key
            max_length: Maximum length for strings (None if not applicable)
            enum_values: Allowed enum values (None if not enum)
            default: Default value for column
        """
        self.name = name
        self.data_type = data_type
        self.nullable = nullable
        self.unique = unique
        self.indexed = indexed
        self.primary_key = primary_key
        self.max_length = max_length
        self.enum_values = enum_values or []
        self.default = default

    def __repr__(self) -> str:
        """Return string representation."""
        parts = [f"name={self.name!r}", f"type={self.data_type.value}"]

        if self.max_length is not None:
            parts.append(f"max_length={self.max_length}")

        if self.enum_values:
            parts.append(f"enum={self.enum_values}")

        nullable_str = "NULL" if self.nullable else "NOT NULL"
        parts.append(nullable_str)

        if self.unique:
            parts.append("UNIQUE")

        if self.indexed:
            parts.append("INDEXED")

        if self.primary_key:
            parts.append("PRIMARY_KEY")

        return f"ColumnMetadata({', '.join(parts)})"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "name": self.name,
            "data_type": self.data_type.value,
            "nullable": self.nullable,
            "unique": self.unique,
            "indexed": self.indexed,
            "primary_key": self.primary_key,
            "max_length": self.max_length,
            "enum_values": self.enum_values,
            "default": self.default,
        }


def generate_schema_from_model(
    model_class: type,
) -> tuple[list[ColumnMetadata], dict[str, ColumnMetadata]]:
    """Generate schema metadata from a SQLAlchemy model class.

    Introspects a SQLAlchemy model and extracts column metadata including
    types, constraints, string lengths, and enum values.

    Args:
        model_class: A SQLAlchemy declarative base model class

    Returns:
        Tuple of (columns_list, columns_dict) where:
        - columns_list: List of ColumnMetadata in column order
        - columns_dict: Dict mapping column names to ColumnMetadata

    Raises:
        ValueError: If the model is not a SQLAlchemy model class

    Example:
        >>> from anigozanthos.models import SequenceInventoryAll
        >>> cols_list, cols_dict = generate_schema_from_model(SequenceInventoryAll)
        >>> cols_list[0].name  # 'id'
        >>> cols_dict['species'].max_length  # 100
    """
    try:
        mapper = inspect(model_class)
    except Exception as e:
        raise ValueError(
            f"{model_class.__name__} is not a SQLAlchemy model: {e}",
        ) from e

    columns_list = []
    columns_dict = {}

    for col in mapper.columns:
        # Determine data type
        col_type = col.type
        python_type = col_type.python_type

        if python_type is int:
            data_type = DataType.INTEGER
        elif python_type is str:
            # Check if it's an enum
            if isinstance(col_type, SQLEnum):
                data_type = DataType.ENUM
            else:
                data_type = DataType.STRING
        else:
            # Default to STRING for unknown types
            data_type = DataType.STRING

        # Extract max length for string columns
        max_length = None
        if data_type == DataType.STRING and hasattr(col_type, "length"):
            max_length = col_type.length  # type: ignore[attr-defined]

        # Extract enum values
        enum_values = []
        if data_type == DataType.ENUM and isinstance(col_type, SQLEnum):
            enum_values = list(col_type.enums) if col_type.enums else []

        # Extract constraints
        is_primary = col.primary_key
        is_nullable = col.nullable
        assert isinstance(is_nullable, bool), (
            f"col.nullable is not bool: {col.nullable}"
        )
        is_unique = col.unique is True
        is_indexed = col.index is True

        # Create metadata
        metadata = ColumnMetadata(
            name=col.name,
            data_type=data_type,
            nullable=is_nullable,
            unique=is_unique,
            indexed=is_indexed,
            primary_key=is_primary,
            max_length=max_length,
            enum_values=enum_values,
            default=col.default,
        )

        columns_list.append(metadata)
        columns_dict[col.name] = metadata

    return columns_list, columns_dict


class DynamicSchema:
    """Dynamically generated schema from a SQLAlchemy model.

    This class wraps a dynamically introspected SQLAlchemy model and provides
    the same interface as SequenceInventoryAllSchema for querying and validating
    column metadata.
    """

    def __init__(
        self,
        columns_list: list[ColumnMetadata],
        columns_dict: dict[str, ColumnMetadata],
    ):
        """Initialize dynamic schema.

        Args:
            columns_list: Ordered list of ColumnMetadata
            columns_dict: Dict mapping column names to ColumnMetadata
        """
        self._columns_list = columns_list
        self._columns_dict = columns_dict

    @classmethod
    def from_model(cls, model_class: type) -> DynamicSchema:
        """Create a DynamicSchema from a SQLAlchemy model class.

        Args:
            model_class: SQLAlchemy declarative base model class

        Returns:
            DynamicSchema instance

        Example:
            >>> from anigozanthos.models import SequenceInventoryAll
            >>> schema = DynamicSchema.from_model(SequenceInventoryAll)
            >>> schema.get_column_by_name('species')
        """
        columns_list, columns_dict = generate_schema_from_model(model_class)
        return cls(columns_list, columns_dict)

    def get_columns(self) -> list[ColumnMetadata]:
        """Get list of all columns in order.

        Returns:
            List of ColumnMetadata objects in column order
        """
        return self._columns_list

    def get_column_by_name(self, name: str) -> ColumnMetadata | None:
        """Get column metadata by name.

        Args:
            name: Column name

        Returns:
            ColumnMetadata if found, None otherwise
        """
        return self._columns_dict.get(name)

    def get_required_columns(self) -> list[ColumnMetadata]:
        """Get columns that cannot be NULL.

        Returns:
            List of non-nullable ColumnMetadata objects
        """
        return [col for col in self._columns_list if not col.nullable]

    def get_nullable_columns(self) -> list[ColumnMetadata]:
        """Get columns that can be NULL.

        Returns:
            List of nullable ColumnMetadata objects
        """
        return [col for col in self._columns_list if col.nullable]

    def get_unique_columns(self) -> list[ColumnMetadata]:
        """Get columns with unique constraint.

        Returns:
            List of ColumnMetadata objects with unique=True
        """
        return [col for col in self._columns_list if col.unique]

    def get_indexed_columns(self) -> list[ColumnMetadata]:
        """Get indexed columns.

        Returns:
            List of ColumnMetadata objects with indexed=True
        """
        return [col for col in self._columns_list if col.indexed]

    def get_string_columns(self) -> list[ColumnMetadata]:
        """Get all string columns.

        Returns:
            List of ColumnMetadata objects with STRING data type
        """
        return [col for col in self._columns_list if col.data_type == DataType.STRING]

    def get_enum_columns(self) -> list[ColumnMetadata]:
        """Get all enum columns.

        Returns:
            List of ColumnMetadata objects with ENUM data type
        """
        return [col for col in self._columns_list if col.data_type == DataType.ENUM]

    def get_primary_key(self) -> ColumnMetadata | None:
        """Get primary key column.

        Returns:
            ColumnMetadata for primary key, or None if not found
        """
        for col in self._columns_list:
            if col.primary_key:
                return col
        return None

    def validate_value(self, column_name: str, value: Any) -> tuple[bool, str | None]:
        """Validate a value against column constraints.

        Args:
            column_name: Name of the column
            value: Value to validate

        Returns:
            Tuple of (is_valid, error_message)
            If valid, error_message is None
        """
        col = self.get_column_by_name(column_name)
        if col is None:
            return False, f"Column '{column_name}' not found in schema"

        if value is None:
            if not col.nullable:
                return False, f"Column '{column_name}' cannot be NULL"
            return True, None

        # Validate string columns
        if col.data_type == DataType.STRING:
            if not isinstance(value, str):
                return (
                    False,
                    f"Column '{column_name}' expects string, got {type(value).__name__}",
                )
            if col.max_length and len(value) > col.max_length:
                return (
                    False,
                    f"Column '{column_name}' exceeds max length {col.max_length}",
                )

        # Validate enum columns
        if col.data_type == DataType.ENUM:
            if col.enum_values and value not in col.enum_values:
                return (
                    False,
                    f"Column '{column_name}' must be one of {col.enum_values}",
                )

        # Validate integer columns
        if col.data_type == DataType.INTEGER:
            if not isinstance(value, int):
                return (
                    False,
                    f"Column '{column_name}' expects integer, got {type(value).__name__}",
                )

        return True, None

    def to_dict(self) -> dict[str, dict[str, Any]]:
        """Convert entire schema to dictionary.

        Returns:
            Dictionary mapping column names to their metadata dicts
        """
        return {col.name: col.to_dict() for col in self._columns_list}

    def generate_dataclass(
        self,
        stream: IO[str] | None = None,
        class_name: str = "GeneratedModel",
    ) -> str:
        """Generate Python dataclass code for this schema.

        Args:
            stream: Optional output stream to write to. If None, returns string.
            class_name: Name for the generated dataclass. Defaults to "GeneratedModel"

        Returns:
            The generated dataclass code as a string
        """
        code = self._generate_dataclass_string(class_name)

        if stream is not None:
            stream.write(code)
        return code

    def _generate_dataclass_string(self, class_name: str) -> str:
        """Generate dataclass code string."""
        lines = [
            "from __future__ import annotations",
            "",
            "from dataclasses import dataclass, field",
            "from typing import Optional",
            "",
            "",
            "@dataclass",
            f"class {class_name}:",
            f'    """{class_name} - Auto-generated dataclass from schema.',
            "",
            "    Attributes:",
        ]

        # Add attributes documentation
        for col in self._columns_list:
            if col.data_type.value == "ENUM":
                lines.append(
                    f"        {col.name}: Optional enum {col.enum_values if col.enum_values else '[]'}",
                )
            elif col.data_type.value == "STRING":
                if col.max_length:
                    lines.append(
                        f"        {col.name}: Optional string (max {col.max_length} chars)",
                    )
                else:
                    lines.append(f"        {col.name}: Optional string")
            else:
                lines.append(
                    f"        {col.name}: Required {col.data_type.value.lower()}",
                )

        lines.append('    """')
        lines.append("")

        # Add field definitions
        for col in self._columns_list:
            type_hint = self._get_type_hint(col)
            default_value = self._get_default_value(col)

            if default_value is not None:
                lines.append(f"    {col.name}: {type_hint} = {default_value}")
            else:
                lines.append(f"    {col.name}: {type_hint}")

        return "\n".join(lines)

    def _get_type_hint(self, col: ColumnMetadata) -> str:
        """Generate type hint for a column."""
        if col.data_type.value == "INTEGER":
            base_type = "int"
        elif col.data_type.value == "STRING":
            base_type = "str"
        else:
            base_type = "str"

        if col.nullable:
            return f"Optional[{base_type}]"
        return base_type

    def _get_default_value(self, col: ColumnMetadata) -> str | None:
        """Generate default value for a column."""
        if col.nullable:
            return "None"
        return None

    def generate_schema_code(
        self,
        stream: IO[str] | None = None,
        schema_class_name: str = "GeneratedSchema",
    ) -> str:
        """Generate Python code for a schema class equivalent to SequenceInventoryAllSchema.create().

        Creates a complete schema class definition with a create() method that
        instantiates the schema with all column metadata, matching the style
        of SequenceInventoryAllSchema.

        Args:
            stream: Optional output stream to write to. If None, returns string.
            schema_class_name: Name for the generated schema class. Defaults to "GeneratedSchema"

        Returns:
            The generated schema class code as a string

        Example:
            >>> from anigozanthos.models import SequenceInventoryAll
            >>> schema = DynamicSchema.from_model(SequenceInventoryAll)
            >>> code = schema.generate_schema_code(schema_class_name="SeqInventoryAllSchema")
            >>> print(code)  # Complete schema class definition
        """
        code = self._generate_schema_code_string(schema_class_name)

        if stream is not None:
            stream.write(code)
        return code

    def _generate_schema_code_string(self, schema_class_name: str) -> str:
        """Generate the complete schema class code."""
        lines = [
            "# Auto-generated schema class from SQLAlchemy model by sqlamodels",
            "from __future__ import annotations",
            "",
            "from dataclasses import dataclass",
            "from typing import Any",
            "",
            "from enum import Enum",
            "",
            "",
            "class DataType(Enum):",
            '    """Column data types."""',
            "",
            '    INTEGER = "INTEGER"',
            '    STRING = "STRING"',
            '    ENUM = "ENUM"',
            '    DECIMAL = "DECIMAL"',
            '    BLOB = "BLOB"',
            "",
            "",
            "class ColumnMetadata:",
            '    """Metadata for a single column."""',
            "",
            "    def __init__(",
            "        self,",
            "        name: str,",
            "        data_type: DataType,",
            "        nullable: bool = True,",
            "        unique: bool = False,",
            "        indexed: bool = False,",
            "        primary_key: bool = False,",
            "        max_length: int | None = None,",
            "        enum_values: list[str] | None = None,",
            "        default: Any = None,",
            "    ):",
            '        """Initialize column metadata."""',
            "        self.name = name",
            "        self.data_type = data_type",
            "        self.nullable = nullable",
            "        self.unique = unique",
            "        self.indexed = indexed",
            "        self.primary_key = primary_key",
            "        self.max_length = max_length",
            "        self.enum_values = enum_values or []",
            "        self.default = default",
            "",
            "",
            "@dataclass",
            f"class {schema_class_name}:",
            '    """Schema for table - Auto-generated from SQLAlchemy model."""',
            "",
        ]

        # Add fields for each column
        for col in self._columns_list:
            lines.append(f"    {col.name}: ColumnMetadata")

        lines.extend(
            [
                "",
                "    @classmethod",
                f"    def create(cls) -> {schema_class_name}:",
                '        """Create schema metadata."""',
                "        return cls(",
            ],
        )

        # Add each column instantiation
        for col in self._columns_list:
            lines.append(self._format_column_metadata(col))

        lines.extend(
            [
                "        )",
                "",
                "    def get_columns(self) -> list[ColumnMetadata]:",
                '        """Get list of all columns in order."""',
                "        return [",
            ],
        )

        # Add column references
        for col in self._columns_list:
            lines.append(f"            self.{col.name},")

        lines.extend(
            [
                "        ]",
                "",
                "    def get_column_by_name(self, name: str) -> ColumnMetadata | None:",
                '        """Get column metadata by name."""',
                "        for col in self.get_columns():",
                "            if col.name == name:",
                "                return col",
                "        return None",
                "",
                "    def get_required_columns(self) -> list[ColumnMetadata]:",
                '        """Get non-nullable columns."""',
                "        return [col for col in self.get_columns() if not col.nullable]",
                "",
                "    def get_nullable_columns(self) -> list[ColumnMetadata]:",
                '        """Get nullable columns."""',
                "        return [col for col in self.get_columns() if col.nullable]",
                "",
                "    def get_unique_columns(self) -> list[ColumnMetadata]:",
                '        """Get columns with unique constraint."""',
                "        return [col for col in self.get_columns() if col.unique]",
                "",
                "    def get_enum_columns(self) -> list[ColumnMetadata]:",
                '        """Get enum columns."""',
                "        return [col for col in self.get_columns() if col.data_type == DataType.ENUM]",
                "",
            ],
        )

        lines.append("\n\n")
        lines.append("# Create singleton instance for convenient access")
        lines.append(f"schema = {schema_class_name}.create()")

        return "\n".join(lines)

    def _format_column_metadata(self, col: ColumnMetadata) -> str:
        """Format a ColumnMetadata as an instantiation string."""
        parts = [f"            {col.name}=ColumnMetadata("]
        parts.append(f'                name="{col.name}",')
        parts.append(f"                data_type=DataType.{col.data_type.value},")
        parts.append(f"                nullable={col.nullable},")

        if col.unique:
            parts.append(f"                unique={col.unique},")

        if col.indexed:
            parts.append(f"                indexed={col.indexed},")

        if col.primary_key:
            parts.append(f"                primary_key={col.primary_key},")

        if col.max_length is not None:
            parts.append(f"                max_length={col.max_length},")

        if col.enum_values:
            parts.append(f"                enum_values={col.enum_values},")

        parts.append("            ),")

        return "\n".join(parts)

    def __repr__(self) -> str:
        """Return detailed string representation."""
        lines = ["DynamicSchema("]
        for col in self._columns_list:
            lines.append(f"  {col}")
        lines.append(")")
        return "\n".join(lines)


@cli.command(name="schema")
@click.option(
    "-o",
    "--out",
    type=click.File("w"),
    default=None,
    help="Output file for generated schema code",
)
@click.argument("model_class", type=str)
def generate_schema(model_class: str, out: IO[str] | None) -> None:
    """Generate schema code for a given SQLAlchemy model class.

    Args:
        model_class: Fully qualified name of the SQLAlchemy model class
                     (e.g., 'anigozanthos.models.SequenceInventoryAll')
    """
    import sys

    sys.path.insert(0, ".")  # Ensure current directory is in path
    # Dynamically import the model class
    module_name, class_name = model_class.rsplit(".", 1)
    module = __import__(module_name, fromlist=[class_name])
    model_cls = getattr(module, class_name)

    # Generate schema
    schema = DynamicSchema.from_model(model_cls)
    code = schema.generate_schema_code(
        stream=out,
        schema_class_name=f"{class_name}Schema",
    )
    if out is None:
        click.echo(code)
