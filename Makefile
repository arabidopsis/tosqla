install:
	uv build && uv tool install tosqla --find-links dist/
	rm -rf dist/
