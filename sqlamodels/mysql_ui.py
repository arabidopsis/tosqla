from __future__ import annotations

import sys
from typing import IO
from typing import Sequence

import click

from .cli import cli


def shared_options(f):
    """Add an --engine option to a click command."""
    f = click.option(
        "--engine",
        "mysql_engine",
        help="mysql/sqlite engine to use for tables",
    )(f)
    f = click.option(
        "-o",
        "--out",
        type=click.File("w"),
        default=sys.stdout,
        help="output file or stdout",
    )(f)
    f = click.option("--abstract", is_flag=True, help="make classes abstract")(f)
    f = click.option(
        "-x",
        "--without-tablename",
        is_flag=True,
        help="don't add __tablename__",
    )(f)
    f = click.option(
        "-y",
        "--without-table-args",
        is_flag=True,
        help="don't add __table_args__",
    )(f)
    return f


@cli.command()
@shared_options
@click.argument("host", required=True)
@click.argument("tables", nargs=-1)
def models(
    host: str,
    out: IO[str],
    abstract: bool,
    mysql_engine: str | None,
    tables: Sequence[str],
    without_tablename: bool,
    without_table_args: bool,
):
    """Render tables into sqlalchemy DeclarativeBase classes."""
    from .mysqla import connect_mysql, ModelMaker, get_env

    # click.secho(f"# connecting to {host}", err=True)
    if abstract:
        without_tablename = True
    if not host.startswith("mysql"):
        without_table_args = True
    ttables = connect_mysql(host, tables)
    ModelMaker(
        env=get_env(),
        with_tablename=not without_tablename,
        engine=mysql_engine,
        table_args=not without_table_args,
    ).run_tables(
        ttables,
        out=out,
        abstract=abstract,
    )


@cli.command()
@click.option(
    "--name",
    default="{}_backup",
    help="format for new table name, use {} for original table name",
    show_default=True,
)
@click.option("--pk", default="id", help="name of new id column", show_default=True)
@shared_options
@click.argument("host", required=True)
@click.argument("tables", nargs=-1)
def backups(
    host: str,
    name: str | None,
    out: IO[str],
    pk: str,
    mysql_engine: str | None,
    abstract: bool,
    without_tablename: bool,
    tables: Sequence[str],
    without_table_args: bool,
):
    """Make a DeclarativeBase table that's a "backup" of another."""
    from .mysqla import connect_mysql, ModelMaker, get_env

    if abstract:
        without_tablename = True
    ttables = connect_mysql(host, tables)
    if not host.startswith("mysql"):
        without_table_args = True
    mm = ModelMaker(
        env=get_env(),
        engine=mysql_engine,
        with_tablename=not without_tablename,
        table_args=not without_table_args,
    )
    if not name:
        name = "{}_backup"
    # indexes = [insp.get_indexes(t.name) for t in tables]
    ttables = [mm.mkcopy(t, name.format(t.name), t.metadata, pk) for t in ttables]
    # print(indexes)

    mm.run_tables(ttables, out=out, abstract=abstract)


if __name__ == "__main__":
    cli()
