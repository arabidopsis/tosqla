from __future__ import annotations

from typing import IO

import click

from .cli import cli

EXPLAIN = """
This error might be due to the fact that the module imports more than
sqlalchemy. i.e. is not a simple generated output of `sqlamodels models` command.
Try installing `sqlamodels` in the same environment where the module is located and run this command again.
"""


@cli.command(name="schema")
@click.option(
    "-o",
    "--out",
    type=click.File("w"),
    default=None,
    help="Output file for generated schema code",
)
@click.option(
    "--no-singleton",
    is_flag=True,
    default=False,
    help="Do not create a singleton instance of the schema",
)
@click.argument("model_class", type=str)
def schema_cmd(
    model_class: str,
    out: IO[str] | None,
    no_singleton: bool = False,
) -> None:
    """Generate schema code for a given SQLAlchemy model class.

    Args:
        model_class: Fully qualified name of the SQLAlchemy model class
                     (e.g., 'anigozanthos.models.SequenceInventoryAll')
    """
    import sys
    from importlib import import_module
    from sqlalchemy.exc import NoInspectionAvailable
    from sqlalchemy.orm import DeclarativeBase
    from .schema import DynamicSchema
    from .mysqla import get_env

    sys.path.insert(0, ".")  # Ensure current directory is in path
    # Dynamically import the model class
    module_name, class_name = model_class.rsplit(".", 1)
    try:
        module = import_module(module_name)

    except ImportError as e:
        click.secho(
            f"Error importing {module_name}: {e} ({EXPLAIN})",
            err=True,
            fg="red",
        )
        raise click.Abort()
    model_cls = getattr(module, class_name, None)
    if model_cls is None:
        click.secho(
            f"Error: {class_name} is not a valid class of module {module_name}",
            err=True,
            fg="red",
        )
        raise click.Abort()
    if not isinstance(model_cls, type) or not issubclass(model_cls, DeclarativeBase):
        click.secho(
            f"Error: {class_name} is not a subclass of DeclarativeBase in module {module_name}",
            err=True,
            fg="red",
        )
        raise click.Abort()
    # Generate schema
    try:
        schema = DynamicSchema.from_model(model_cls)

        txt = (
            get_env()
            .get_template("meta.py.tmplt")
            .render(
                schema=schema,
                class_name=f"{class_name}Schema",
                class_table=class_name,
                singleton=not no_singleton,
            )
        )
        if out is None:
            click.echo(txt)
        else:
            out.write(txt)
        click.secho(
            f"Schema code for {model_class} generated successfully.",
            err=True,
            fg="green",
            bold=True,
        )
    except NoInspectionAvailable as e:
        click.secho(f"Error: {e}", err=True, fg="red")
        raise click.Abort()
