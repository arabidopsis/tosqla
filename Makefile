install:
	uv build && XDG_DATA_HOME=~/.local/share uv tool install sqlamodels --force --find-links dist/
	rm -rf dist/
