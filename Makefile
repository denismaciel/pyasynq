.PHONY: dev test lint redis

# Compile the Rust extension and install it into .venv (editable).
dev:
	uv sync -q
	uv run --no-sync maturin develop --uv

test: dev
	cargo test
	uv run --no-sync pytest -q

# cargo prefers subcommands in ~/.cargo/bin (rustup) over PATH, so call the
# dev shell's binaries directly to stay on the flake's toolchain.
lint:
	cargo-clippy clippy --all-targets -- -D warnings
	cargo-fmt --check
	ruff check python tests

redis:
	redis-server --port 6379 --save "" --appendonly no
