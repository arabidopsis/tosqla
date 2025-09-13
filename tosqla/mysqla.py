from __future__ import annotations

import re
import sys
from datetime import datetime
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
    name = "".join(n[0].upper() + n[1:] for n in name.split("_"))
    if name.endswith("s"):
        name = name[:-1]
    name = name.replace(".", "_")
    if name in {"Column", "Table", "Integer"}:
        name = name + "Class"
    return name


NAMES = {"class": "class_"}


CNAMES = re.compile("[_ -()/]+")

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


class ColDict(TypedDict):
    name: str
    type: str
    pytype: str
    otype: str
    nullable: bool
    pk: Column
    server_default: str | None
    index: bool
    unique: bool
    column_name: str
    max_length: NotRequired[int]


class TableDict(TypedDict):
    model: str
    name: str
    columns: list[ColDict]
    enums: list[tuple[str, str]]
    charset: str
    indexes: set[str]
    abstract: bool
    with_tablename: bool


def column_name(name: str) -> str:
    cname = CNAMES.sub("_", name)
    cname = NAMES.get(cname, cname)
    if cname[0] in Number:
        cname = Number[cname[0]] + cname[1:]
    return cname


class ModelMaker:
    def __init__(self, env: Environment, with_tablename: bool = False):
        self.env = env
        self.with_tablename = with_tablename

    def column_name(self, name: str) -> str:
        return column_name(name)

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

        charset = table.dialect_options["mysql"]["default charset"]
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
                if hasattr(typ, "charset") and typ.charset:
                    if typ.charset != charset:
                        usecharset = True
                        atyp = f'{name}(charset="{typ.charset}")'
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
                if hasattr(typ, "charset") and typ.charset and typ.charset != charset:
                    atyp = f'{name}({typ.length}, charset="{typ.charset}")'
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
                atyp = atyp = f"{name}(4)"
                pytype = "int"
                imports.add(name)

            else:
                raise RuntimeError(f'unknown field "{table.name}.{c.name}" {c.type}')
            if c.nullable:
                pytype = pytype + " | None"
            d = ColDict(
                name=c.name,
                type=atyp,
                pytype=pytype,
                otype=c.type.__class__.__name__,
                nullable=c.nullable,
                pk=c.primary_key,
                server_default=server_default,
                index=c.index,
                unique=c.unique,
                column_name=self.column_name(c.name),
            )

            if hasattr(c.type, "length"):
                d["max_length"] = c.type.length
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
            indexes=indexes,
            abstract=False,
            with_tablename=True,
        )

        return data, imports, mysql, pyimports

    def pascal_case(self, name: str) -> str:
        return pascal_case(name)

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
        # insp = inspect(engine) if engine else None
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

            data.update(
                dict(
                    abstract=abstract,
                    with_tablename=self.with_tablename,
                ),
            )
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
        print(f"# generated by {__file__} on {datetime.now()}", file=out)
        PyImports = self.env.get_template("imports.py.tmplt")
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

        if pkname in names:
            while pkname in names:
                pkname = pkname + "_"

        args: list[Column[Any] | Index] = pks + cols  # type: ignore
        i = Index("fk_index", *(p.name for p in pks), unique=False)
        args.append(i)
        if indexes:
            # print(indexes)
            for i in indexes:
                # print(i, dir(i))
                args.append(
                    Index(i.name, *(c.name for c in i.columns), unique=i.unique),
                )
        # args.append(i)
        # print('HERE', i)

        return Table(
            name,
            meta,
            Column(pkname, Integer, primary_key=True),
            *args,
            **table.kwargs,
        )


@click.group()
def cli():
    pass


@cli.command()
@click.option("--host", metavar="URL", required=True)
@click.option("--abstract", is_flag=True, help="make classes abstract")
@click.option("-w", "--with-tablename", is_flag=True, help="add __tablename__")
@click.option(
    "-o",
    "--out",
    type=click.File("w"),
    default=sys.stdout,
    help="output file or stdout",
)
@click.argument("tables", nargs=-1)
def tosqla(
    host: str | None,
    out: IO[str],
    abstract: bool,
    tables: Sequence[str],
    with_tablename: bool,
):
    """Render tables into sqlalchemy.ext.declarative classes."""
    if not host:
        raise click.BadParameter("please specify --host", param_hint="host")
    engine = create_engine(host)
    meta = MetaData()
    if tables:
        meta.reflect(only=tables, bind=engine)
    else:
        meta.reflect(bind=engine)
        tables = meta.tables.keys()

    tables = [meta.tables[t] for t in sorted(tables)]

    ModelMaker(env=get_env(), with_tablename=with_tablename).run_tables(
        tables,
        out=out,
        abstract=abstract,
    )


# pylint: disable=too-many-arguments
@cli.command()
@click.option("--host", metavar="URL")
@click.option("--postfix", default="_backup", help="added postfix to new table name")
@click.option("--abstract", is_flag=True, help="make classes abstract")
@click.option(
    "-o",
    "--out",
    type=click.File("w"),
    default=sys.stdout,
    help="output file or stdout",
)
@click.option("--pk", default="id", help="name of new id column", show_default=True)
@click.argument("tables", nargs=-1)
def backups(
    host: str | None,
    postfix: str | None,
    out: IO[str],
    pk: str,
    abstract: bool,
    tables: Sequence[str],
):
    """Make a table that's a "backup" of another."""
    if not host:
        raise RuntimeError("please specify --host")
    engine = create_engine(host)
    meta = MetaData()

    if tables:
        meta.reflect(only=tables, bind=engine)
    else:
        meta.reflect(bind=engine)
        tables = meta.tables.keys()
    # insp = inspect(engine)

    ttables = [meta.tables[t] for t in sorted(tables)]
    mm = ModelMaker(env=get_env())
    # indexes = [insp.get_indexes(t.name) for t in tables]
    ttables = [mm.mkcopy(t, t.name + postfix, meta, pk) for t in ttables]
    # print(indexes)

    mm.run_tables(ttables, out=out, abstract=abstract)


if __name__ == "__main__":
    cli()
