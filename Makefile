.PHONY: dev test test-ft lint typecheck redis

# Compile the Rust extension and install it into .venv (editable).
dev:
	uv sync -q
	uv run --no-sync maturin develop --uv

test: dev
	cargo test
	uv run --no-sync pytest -q

# The suite on free-threaded CPython in .venv-ft; it fails if the GIL turns
# on. The dev shell's sysconfig variables are for its own Python, and pure
# psycopg loads libpq itself.
FT_ENV = env -u _PYTHON_SYSCONFIGDATA_NAME -u _PYTHON_HOST_PLATFORM -u PYTHONPATH -u PYTHON_GIL \
	UV_PROJECT_ENVIRONMENT=.venv-ft LD_LIBRARY_PATH=$(LIBPQ_DIR)

test-ft:
	$(FT_ENV) uv sync -q -p $(FREETHREADED_PYTHON) --no-default-groups --group freethreaded
	$(FT_ENV) uv run --no-sync maturin develop --uv
	$(FT_ENV) uv run --no-sync pytest -q

# cargo prefers subcommands in ~/.cargo/bin (rustup) over PATH, so call the
# dev shell's binaries directly to stay on the flake's toolchain.
lint: typecheck
	cargo-clippy clippy --all-targets -- -D warnings
	cargo-fmt --check
	uv run --no-sync ruff check python tests
	uv run --no-sync ruff format --check python tests
	uv run --no-sync ty check

# mypy --strict over the package and tests/typing; stubtest keeps _core.pyi
# in sync with the compiled extension.
typecheck: dev
	uv run --no-sync mypy
	uv run --no-sync python -m mypy.stubtest pyasynq._core --allowlist stubtest-allowlist.txt

redis:
	redis-server --port 6379 --save "" --appendonly no
