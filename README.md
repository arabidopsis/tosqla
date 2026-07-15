# Generate Sqlalchemy `DeclarativeBase` Models from Database Tables

install with `uv tool install https://github.com/arabidopsis/sqlamodels.git`

This gives a commandline executable `sqlamodels`.

run as:

`sqlamodels models -o models.py mysql://{user}:{password}@localhost/database`


(Works with `sqlite:///` tables too.)
