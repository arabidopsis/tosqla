from pathlib import Path
import re
import sys
from datetime import datetime
from typing import Any, TextIO
import click
from jinja2 import Environment, FileSystemLoader
from sqlalchemy import (
    BINARY,
    BLOB,
    CHAR,
    DECIMAL,
    JSON,
    TIMESTAMP,
    Column,
    Date,
    DateTime,
    Enum,
    Float,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
)
from sqlalchemy.dialects.mysql import DOUBLE, LONGBLOB, LONGTEXT, MEDIUMBLOB, MEDIUMTEXT
from sqlalchemy.dialects.mysql import SET as Set
from sqlalchemy.dialects.mysql import TEXT, TINYTEXT, YEAR

ENV = Environment(loader=FileSystemLoader(Path(__file__).parent / "templates"))


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


def column_name(name):
    cname = CNAMES.sub("_", name)
    cname = NAMES.get(cname, cname)
    if cname[0] in Number:
        cname = Number[cname[0]] + cname[1:]
    return cname


class ModelMaker:
    def __init__(self, with_tablename: bool = False):
        self.with_tablename = with_tablename

    def column_name(self, name: str) -> str:
        return column_name(name)

    # pylint: disable=too-many-statements
    def convert_table(
        self, table: Table
    ) -> tuple[dict[str, Any], set[str], set[str], set[str]]:  # noqa:
        # pylint: disable=too-many-branches
        mysql: set[str] = set()
        imports: set[str] = set()
        pyimports: set[str] = set()

        columns = []
        enums = {}
        sets = {}
        # indexes = insp.get_indexes(table.name) if insp else []
        indexes = table.indexes

        charset = table.dialect_options["mysql"]["default charset"]

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
            d = dict(
                name=c.name,
                type=atyp,
                pytype=pytype,
                otype=c.type.__class__.__name__,
                nullable=c.nullable,
                pk=c.primary_key,
                server_default=server_default,
            )
            d["index"] = c.index
            d["unique"] = c.unique
            d["column_name"] = self.column_name(c.name)
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

        elist = []
        for e, name in enums.items():
            args = ", ".join(f'"{v}"' for v in e)
            styp = f"Enum({args})"
            elist.append((name, styp))
        for e, name in sets.items():
            args = ", ".join(f'"{v}"' for v in e)
            styp = f"SET({args})"
            elist.append((name, styp))

        if indexes:
            imports.add("Index")

        data = dict(
            model=self.pascal_case(table.name),
            name=table.name,
            columns=columns,
            enums=elist,
            charset=charset,
            indexes=indexes,
        )

        return data, imports, mysql, pyimports

    def pascal_case(self, name: str) -> str:
        return pascal_case(name)

    def run_tables(
        self,
        tables: list[Table],
        *,
        out: TextIO = sys.stdout,
        abstract: bool = False,
    ) -> None:
        mysql = set()
        imports = set()
        pyimports = set()
        ret = []
        # insp = inspect(engine) if engine else None
        enums_seen = set()
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
                    wt=self.with_tablename,
                )
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

    def render_table(self, data: dict[str, Any]) -> str:
        txt = ENV.get_template("body.py.tmplt").render(**data)
        return txt

    # pylint: disable=too-many-arguments
    def gen_tables(
        self,
        tables: list[Table],
        models: list[str],
        imports: set[str],
        mysql: set[str],
        pyimports: set[str],
        out: TextIO = sys.stdout,
    ) -> None:
        print(f"# generated by {__file__} on {datetime.now()}", file=out)
        PyImports = ENV.get_template("imports.py.tmplt")
        print(
            PyImports.render(imports=imports, mysql=mysql, pyimports=pyimports),
            file=out,
        )
        for t in models:
            print(file=out)
            print(t, file=out)

    def mkcopy(
        self, table: Table, name: str, meta: MetaData, pkname: str = "id"
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
                    Index(i.name, *(c.name for c in i.columns), unique=i.unique)
                )
        # args.append(i)
        # print('HERE', i)

        return Table(
            name, meta, Column(pkname, Integer, primary_key=True), *args, **table.kwargs
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
def tosqla(host, out, abstract, tables, with_tablename):
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

    ModelMaker(with_tablename=with_tablename).run_tables(
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
def backups(host, postfix, out, pk, abstract, tables):
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

    tables = [meta.tables[t] for t in sorted(tables)]
    mm = ModelMaker()
    # indexes = [insp.get_indexes(t.name) for t in tables]
    tables = [mm.mkcopy(t, t.name + postfix, meta, pk) for t in tables]
    # print(indexes)

    mm.run_tables(tables, out=out, abstract=abstract)


if __name__ == "__main__":
    cli()
