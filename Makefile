# Every check CI runs, in the order a reviewer would want them.
PYTHON ?= python3
CARGO_DECODER = rust/pump-decoder-kernel/Cargo.toml
CARGO_SEARCH = rust/growth-search/Cargo.toml
# Compiler warnings fail the build; Cargo.lock pins every dependency.
RUST_ENV = RUSTFLAGS="-D warnings"

.PHONY: install check lint types test demo rust parity preview

install:
	$(PYTHON) -m pip install -e '.[dev]'

check: lint types test demo rust parity

lint:
	ruff check .
	ruff format --check .

types:
	mypy

test:
	$(PYTHON) -m pytest

demo:
	$(PYTHON) examples/periods.py

rust:
	cargo fmt --check --manifest-path $(CARGO_DECODER)
	cargo fmt --check --manifest-path $(CARGO_SEARCH)
	$(RUST_ENV) cargo test --locked --manifest-path $(CARGO_DECODER)
	$(RUST_ENV) cargo test --locked --manifest-path $(CARGO_SEARCH)

parity:
	$(PYTHON) scripts/verify_rust_decoder_kernel.py --iterations 10 --fuzz-cases 200

preview:
	pumpquant preview
