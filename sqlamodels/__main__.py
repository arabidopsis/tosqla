from __future__ import annotations

from . import mysql_ui
from . import schema_ui
from .cli import cli

__all__ = ["schema_ui", "mysql_ui", "cli"]

if __name__ == "__main__":
    cli()
