.PHONY: dev check test test-ft lint typecheck build release-check redis

export CARGO_BUILD_JOBS ?= 2

# Compile the Rust extension and install it into .venv (editable).
dev:
	uv sync --locked

test: dev
	cargo test --locked
	uv run --no-sync pytest -q

# The suite on free-threaded CPython in .venv-ft; it fails if the GIL turns
# on. The dev shell's sysconfig variables are for its own Python, and pure
# psycopg loads libpq itself.
FT_ENV = env -u _PYTHON_SYSCONFIGDATA_NAME -u _PYTHON_HOST_PLATFORM -u PYTHONPATH -u PYTHON_GIL \
	UV_PROJECT_ENVIRONMENT=.venv-ft LD_LIBRARY_PATH=$(LIBPQ_DIR)

test-ft:
	$(FT_ENV) uv sync --locked -p $(FREETHREADED_PYTHON) --no-default-groups --group freethreaded
	$(FT_ENV) uv run --no-sync pytest -q

# cargo prefers subcommands in ~/.cargo/bin (rustup) over PATH, so call the
# dev shell's binaries directly to stay on the flake's toolchain.
lint: typecheck
	cargo-clippy clippy --locked --all-targets -- -D warnings
	cargo-fmt --check
	uv run --no-sync ruff check python tests build_backend.py scripts
	uv run --no-sync ruff format --check python tests build_backend.py scripts
	uv run --no-sync ty check

# mypy --strict over the package and tests/typing; stubtest keeps _core.pyi
# in sync with the compiled extension.
typecheck: dev
	uv run --no-sync mypy
	uv run --no-sync python -m mypy.stubtest pyasynq._core --allowlist stubtest-allowlist.txt

redis:
	redis-server --port 6379 --save "" --appendonly no

check: lint

# Build from the sdist by default, exercising the complete source manifest.
build:
	uv build --no-sources

release-check:
	uv run --no-project --python 3.13 scripts/release_check.py $(if $(TAG),--tag $(TAG),)
