.PHONY: dev test lint typecheck redis

# Compile the Rust extension and install it into .venv (editable).
dev:
	uv sync -q
	uv run --no-sync maturin develop --uv

test: dev
	cargo test
	uv run --no-sync pytest -q

# cargo prefers subcommands in ~/.cargo/bin (rustup) over PATH, so call the
# dev shell's binaries directly to stay on the flake's toolchain.
lint: typecheck
	cargo-clippy clippy --all-targets -- -D warnings
	cargo-fmt --check
	ruff check python tests

# mypy --strict over the package and tests/typing; stubtest keeps _core.pyi
# in sync with the compiled extension.
typecheck: dev
	uv run --no-sync mypy
	uv run --no-sync python -m mypy.stubtest pyasynq._core --allowlist stubtest-allowlist.txt

redis:
	redis-server --port 6379 --save "" --appendonly no
