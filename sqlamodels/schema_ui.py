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
    help="Do not create a singleton instances of the schema",
)
@click.option(
    "--module",
    "introspect_module",
    help="python module to introspect",
)
@click.argument("model_classes", type=str, nargs=-1, required=True)
def schema_cmd(
    model_classes: tuple[str, ...],
    introspect_module: str | None,
    out: IO[str] | None,
    no_singleton: bool = False,
) -> None:
    """Generate schema code for given SQLAlchemy model classes.

    Args:
        model_classes: Fully qualified names of the SQLAlchemy model classes
                       (e.g., 'anigozanthos.models.SequenceInventoryAll').
                       *OR* use the --module option to specify a module to introspect.
                       and just provide the class names (e.g., 'SequenceInventoryAll').
    """
    import sys
    from importlib import import_module
    from sqlalchemy.exc import NoInspectionAvailable
    from sqlalchemy.orm import DeclarativeBase
    from .schema import DynamicSchema
    from .mysqla import get_env

    sys.path.insert(0, ".")  # Ensure current directory is in path

    def get_modules():
        ret = []
        mdict = {}
        for model_class in model_classes:
            # Dynamically import the model class
            if introspect_module:
                module_name, class_name = introspect_module, model_class
            else:
                module_name, class_name = model_class.rsplit(".", 1)
            try:
                if module_name in mdict:
                    module = mdict[module_name]
                else:
                    module = import_module(module_name)
                    mdict[module_name] = module
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
            if not isinstance(model_cls, type) or not issubclass(
                model_cls,
                DeclarativeBase,
            ):
                click.secho(
                    f"Error: {class_name} is not a subclass of DeclarativeBase in module {module_name}",
                    err=True,
                    fg="red",
                )
                raise click.Abort()
            ret.append((class_name, model_cls))
        return ret

    try:
        schemas = [
            DynamicSchema.from_model(class_name, model_cls)
            for class_name, model_cls in get_modules()
        ]

        txt = (
            get_env()
            .get_template("meta.py.tmplt")
            .render(
                schemas=schemas,
                singleton=not no_singleton,
            )
        )
        if out is None:
            click.echo(txt)
        else:
            out.write(txt)
        click.secho(
            f"Schema code for {', '.join(model_classes)} generated successfully.",
            err=True,
            fg="green",
            bold=True,
        )
    except NoInspectionAvailable as e:
        click.secho(f"Error: {e}", err=True, fg="red")
        raise click.Abort()
