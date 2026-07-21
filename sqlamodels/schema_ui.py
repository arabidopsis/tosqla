from __future__ import annotations

from typing import IO

import click

from .cli import cli


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
    from .schema import DynamicSchema
    from .mysqla import get_env

    sys.path.insert(0, ".")  # Ensure current directory is in path
    # Dynamically import the model class
    module_name, class_name = model_class.rsplit(".", 1)
    module = __import__(module_name, fromlist=[class_name])
    model_cls = getattr(module, class_name)

    # Generate schema
    schema = DynamicSchema.from_model(model_cls)

    txt = (
        get_env()
        .get_template("meta.py.tmplt")
        .render(
            schema=schema,
            class_name=f"{class_name}Schema",
            singleton=not no_singleton,
        )
    )
    if out is None:
        click.echo(txt)
    else:
        out.write(txt)
