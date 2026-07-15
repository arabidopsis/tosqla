from __future__ import annotations

import re
import sys
from datetime import datetime
from keyword import iskeyword
from pathlib import Path
from typing import Any
from typing import IO
from typing import NotRequired
from typing import Sequence
from typing import TypedDict

import click
from jinja2 import Environment
from jinja2 import FileSystemLoader
from sqlalchemy import BINARY
from sqlalchemy import BLOB
from sqlalchemy import Boolean
from sqlalchemy import CHAR
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import DECIMAL
from sqlalchemy import Enum
from sqlalchemy import Float
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import JSON
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import Text
from sqlalchemy import TIMESTAMP
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.dialects.mysql import LONGBLOB
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.dialects.mysql import MEDIUMBLOB
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.dialects.mysql import SET as Set
from sqlalchemy.dialects.mysql import TEXT
from sqlalchemy.dialects.mysql import TINYTEXT
from sqlalchemy.dialects.mysql import YEAR


def get_env() -> Environment:
    return Environment(loader=FileSystemLoader(Path(__file__).parent / "templates"))


def pascal_case(name: str) -> str:
    name = NON_WORD.sub("_", name.strip())
    if name[0] in Number:
        name = Number[name[0]] + name[1:]
    name = "".join(n[0].upper() + n[1:] for n in name.split("_"))
    name = name.replace(".", "_")
    if iskeyword(name):
        name = name + "_"

    # avoid masking SQLAlchemy types
    if name in {
        "String",
        "Enum",
        "Integer",
        "Float",
        "Set",
        "DateTime",
        "Date",
        "Text",
        "JSON",
        "Column",
        "Table",
        "Base",
        "DeclarativeBase",
        "Mapped",
    }:
        name = name + "Class"
    return name


NON_WORD = re.compile(r"(\W)+")

Number = {
    "1": "one",
    "2": "two",
    "3": "three",
    "4": "four",
    "5": "five",
    "6": "six",
    "7": "seven",
    "8": "eight",
    "9": "nine",
    "0": "zero",
}


def column_name(name: str) -> str:
    if name.isidentifier() and not iskeyword(name):
        return name
    cname = NON_WORD.sub("_", name.strip())
    if iskeyword(cname):
        cname = cname + "_"
    if cname[0] in Number:
        cname = Number[cname[0]] + cname[1:]
    return cname


class ColDict(TypedDict):
    name: str
    type: str
    pytype: str
    nullable: bool | None
    pk: bool
    server_default: str | None
    index: bool | None
    unique: bool | None
    column_name: str
    max_length: NotRequired[int]


class TableDict(TypedDict):
    model: str
    name: str
    columns: list[ColDict]
    enums: list[tuple[str, str]]
    charset: str
    engine: str
    indexes: set[Index]
    abstract: bool
    with_tablename: bool


class ModelMaker:
    def __init__(
        self,
        env: Environment,
        with_tablename: bool = False,
        engine: str | None = None,
    ) -> None:
        self.env = env
        self.with_tablename = with_tablename
        self.engine = engine

    def column_name(self, name: str, table_name: str) -> str:
        ret = column_name(name)
        if not ret.isidentifier():
            click.secho(
                f"Warning: invalid column name {name} -> {ret} in table {table_name}",
                err=True,
                fg="yellow",
            )
        return ret

    def convert_table(
        self,
        table: Table,
    ) -> tuple[TableDict, set[str], set[str], set[str]]:
        mysql: set[str] = set()
        imports: set[str] = set()
        pyimports: set[str] = set()

        columns: list[ColDict] = []
        enums: dict[frozenset[str], str] = {}
        sets: dict[tuple[str, ...], str] = {}
        # indexes = insp.get_indexes(table.name) if insp else []
        indexes = table.indexes
        options = table.dialect_options["mysql"]
        charset = "utf8mb4"
        engine = "InnoDB"

        if "default charset" in options:
            charset = options["default charset"]
        if "engine" in options:
            engine = options["engine"]

        c: Column
        for c in table.columns:
            typ = c.type
            atyp = str(typ)
            server_default = None
            pytype = "str"

            if isinstance(typ, DOUBLE):
                atyp = "DOUBLE"
                pytype = "float"
                imports.add(atyp)
            elif isinstance(typ, Float):
                atyp = "Float"
                pytype = "float"
                imports.add(atyp)
            elif isinstance(typ, Integer):
                atyp = "Integer"
                pytype = "int"
                imports.add(atyp)
            elif isinstance(typ, DECIMAL):
                atyp = f"DECIMAL({typ.precision},{typ.scale})"
                imports.add("DECIMAL")
                pytype = "float"
            elif isinstance(typ, TIMESTAMP):
                atyp = "TIMESTAMP"
                server_default = 'text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")'
                imports.add(atyp)
                pytype = "datetime"
                pyimports.add("from datetime import datetime")
                imports.add("text")
            elif isinstance(typ, DateTime):
                atyp = "DateTime"
                # server_default = 'text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")'
                imports.add(atyp)
                pytype = "datetime"
                pyimports.add("from datetime import datetime")
            elif isinstance(typ, Date):
                atyp = "Date"
                pytype = "date"
                # server_default = 'text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")'
                pyimports.add("from datetime import date")
                imports.add(atyp)
            elif isinstance(typ, Set):
                if typ.values not in sets:
                    sets[typ.values] = "set_" + c.name
                atyp = sets[typ.values]
                mysql.add("SET")
            elif isinstance(typ, Enum):
                s = frozenset(typ.enums)
                if s not in enums:
                    enums[s] = "enum_" + c.name
                atyp = enums[s]
                imports.add("Enum")
            elif isinstance(typ, (Text, TEXT, MEDIUMTEXT, TINYTEXT, LONGTEXT)):
                name = typ.__class__.__name__
                usecharset = False
                cset = getattr(typ, "charset", None)
                if cset is not None:
                    if cset != charset:
                        usecharset = True
                        atyp = f'{name}(charset="{cset}")'
                    else:
                        atyp = f"{name}"
                else:
                    if name == "TEXT" and not usecharset:
                        name = "Text"
                    atyp = f"{name}"
                if name.startswith(("TINY", "LONG", "MEDIUM")) or name == "TEXT":
                    mysql.add(name)
                else:
                    imports.add(name)

            elif isinstance(typ, (String, CHAR)):
                name = typ.__class__.__name__
                cset = getattr(typ, "charset", None)
                if cset is not None and cset != charset:
                    atyp = f'{name}({typ.length}, charset="{cset}")'
                    mysql.add(name)
                else:
                    if name == "VARCHAR":
                        name = "String"
                    atyp = f"{name}({typ.length})"
                    imports.add(name)
            elif isinstance(typ, (BLOB, LONGBLOB, MEDIUMBLOB)):
                name = typ.__class__.__name__
                atyp = name
                pytype = "bytes"
                mysql.add(name)
            elif isinstance(typ, (BINARY,)):
                name = typ.__class__.__name__
                atyp = f"{name}({typ.length})"
                imports.add(name)
                pytype = "bytes"
            elif isinstance(typ, (JSON)):
                name = typ.__class__.__name__
                atyp = name
                imports.add(name)
            elif isinstance(typ, YEAR):
                name = typ.__class__.__name__
                atyp = f"{name}(4)"
                pytype = "int"
                imports.add(name)
            elif isinstance(typ, Boolean):
                # name = typ.__class__.__name__
                atyp = "Boolean"
                pytype = "bool"
                imports.add(atyp)

            else:
                raise RuntimeError(f'unknown field "{table.name}.{c.name}" {c.type}')
            if c.nullable:
                pytype = pytype + " | None"
            d = ColDict(
                name=c.name,
                type=atyp,
                pytype=pytype,
                nullable=c.nullable,
                pk=c.primary_key,
                server_default=server_default,
                index=c.index,
                unique=c.unique,
                column_name=self.column_name(c.name, table.name),
            )

            if hasattr(c.type, "length"):
                d["max_length"] = c.type.length  # type: ignore
            columns.append(d)
            for i in indexes:
                if len(i.columns) == 1:
                    if c.name in i.columns:
                        d["index"] = True
                        d["unique"] = i.unique
                        indexes.remove(i)
                        break

        elist: list[tuple[str, str]] = []
        for fs, name in enums.items():
            args = ", ".join(f'"{v}"' for v in fs)
            styp = f"Enum({args})"
            elist.append((name, styp))
        for e, name in sets.items():
            args = ", ".join(f'"{v}"' for v in e)
            styp = f"SET({args})"
            elist.append((name, styp))

        if indexes:
            imports.add("Index")

        data = TableDict(
            model=self.pascal_case(table.name),
            name=table.name,
            columns=columns,
            enums=elist,
            charset=charset,
            engine=engine,
            indexes=indexes,
            abstract=False,
            with_tablename=True,
        )

        return data, imports, mysql, pyimports

    def pascal_case(self, name: str) -> str:
        ret = pascal_case(name)
        if not ret.isidentifier():
            click.secho(
                f'Warning: invalid Table name "{name}" -> {ret}',
                err=True,
                fg="yellow",
            )
        return ret

    def run_tables(
        self,
        tables: Sequence[Table],
        *,
        out: IO[str] = sys.stdout,
        abstract: bool = False,
    ) -> None:
        mysql = set()
        imports = set()
        pyimports = set()
        ret: list[str] = []
        enums_seen: set[tuple[str, str]] = set()
        for table in tables:
            data, i, m, pi = self.convert_table(table)
            imports |= i
            mysql |= m
            pyimports |= pi
            e = []
            for k in data["enums"]:
                if k not in enums_seen:
                    e.append(k)
                enums_seen.add(k)
            data["enums"] = e
            data["abstract"] = abstract
            data["with_tablename"] = self.with_tablename
            if self.engine is not None:
                data["engine"] = self.engine

            txt = self.render_table(data)

            ret.append(txt)

        self.gen_tables(
            tables,
            ret,
            imports,
            mysql,
            pyimports,
            out=out,
        )

    def render_table(self, data: TableDict) -> str:
        txt = self.env.get_template("body.py.tmplt").render(**data)
        return txt

    def gen_tables(
        self,
        tables: Sequence[Table],
        models: list[str],
        imports: set[str],
        mysql: set[str],
        pyimports: set[str],
        out: IO[str] = sys.stdout,
    ) -> None:
        print(f"# generated by tosqla on {datetime.now()}", file=out)
        PyImports = self.env.get_template("imports-one.py.tmplt")
        print(
            PyImports.render(imports=imports, mysql=mysql, pyimports=pyimports),
            file=out,
        )
        for t in models:
            print(file=out)
            print(t, file=out)

    def mkcopy(
        self,
        table: Table,
        name: str,
        meta: MetaData,
        pkname: str = "id",
    ) -> Table:
        indexes = table.indexes
        names = {c.key for c in table.c}
        pks = list(table.primary_key.columns)
        cols = [c.copy() for c in table.c if c not in pks]
        pks = [c.copy() for c in pks]
        for pk in pks:
            pk.primary_key = False
        o = pkname
        if pkname in names:
            while pkname in names:
                pkname = pkname + "_"
        if o != pkname:
            click.secho(
                f"Warning: table {table.name} already has a column named {o}, using {pkname} instead",
                err=True,
                fg="yellow",
            )

        args: list[Column[Any] | Index] = pks + cols  # type: ignore
        i = Index("fk_" + name, *(p.name for p in pks), unique=False)
        args.append(i)
        if indexes:
            for i in indexes:
                args.append(
                    Index(i.name, *(c.name for c in i.columns), unique=i.unique),
                )

        return Table(
            name,
            meta,
            Column(pkname, Integer, primary_key=True),
            *args,
            **table.kwargs,
        )


def connect_mysql(host: str, tables: Sequence[str] | None = None) -> list[Table]:
    import sqlalchemy.exc

    """Connect to a MySQL database."""
    if host.startswith("mysql://"):
        host = "mysql+pymysql://" + host[8:]
    engine = create_engine(host)

    meta = MetaData()

    try:
        if tables:
            meta.reflect(only=tables, bind=engine)
        else:
            meta.reflect(bind=engine)
            tables = list(meta.tables.keys())
        return [meta.tables[t] for t in sorted(tables)]

    except sqlalchemy.exc.OperationalError as e:
        click.secho(f"Error connecting to {host}: {e}", err=True, fg="red")
        raise click.Abort()


@click.group()
def cli():
    pass


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
):
    """Render tables into sqlalchemy DeclarativeBase classes."""

    # click.secho(f"# connecting to {host}", err=True)
    if abstract:
        without_tablename = True
    ttables = connect_mysql(host, tables)
    ModelMaker(
        env=get_env(),
        with_tablename=not without_tablename,
        engine=mysql_engine,
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
):
    """Make a DeclarativeBase table that's a "backup" of another."""
    if abstract:
        without_tablename = True
    ttables = connect_mysql(host, tables)
    mm = ModelMaker(
        env=get_env(),
        engine=mysql_engine,
        with_tablename=not without_tablename,
    )
    if not name:
        name = "{}_backup"
    # indexes = [insp.get_indexes(t.name) for t in tables]
    ttables = [mm.mkcopy(t, name.format(t.name), t.metadata, pk) for t in ttables]
    # print(indexes)

    mm.run_tables(ttables, out=out, abstract=abstract)


if __name__ == "__main__":
    cli()
