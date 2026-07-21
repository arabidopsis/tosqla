from __future__ import annotations

from . import schema
from .mysqla import cli

__all__ = ["schema", "cli"]

if __name__ == "__main__":
    cli()
