format:
	@echo "🖨️ Format code: Running ruff"
	@uvx ruff format

pack:
	@echo "🗂️ Packaging code into flatfile - use as knowledge base for Claude/aider/etc."
	@uvx repopack "$(CURDIR)" --ignore *lock*,*.json,*.ipynb,codebase.txt,*.csv,.github/*,.mypy_cache/*,architecture-diagram*,*.svg,data/* --output "codebase.txt"

mypy:
	@uv run mypy "$(CURDIR)"

azure_local:
	@echo "🔵 Testing code with local azure functools core"
	@uv run func start --verbose