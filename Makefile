install:
	uv build && XDG_DATA_HOME=~/.local/share uv tool install tosqla --force --find-links dist/
	rm -rf dist/
