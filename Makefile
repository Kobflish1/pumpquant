# Every check CI runs, in the order a reviewer would want them.
PYTHON ?= python3
CARGO_DECODER = rust/pump-decoder-kernel/Cargo.toml
CARGO_SEARCH = rust/m4-growth-search/Cargo.toml

.PHONY: install check lint types test demo rust parity preview

install:
	$(PYTHON) -m pip install -e '.[dev,data,research,archive]'

check: lint types test demo rust parity

lint:
	ruff check .
	ruff format --check .

types:
	mypy

test:
	$(PYTHON) -m pytest

demo:
	$(PYTHON) examples/reproduce_demo.py

rust:
	cargo fmt --check --manifest-path $(CARGO_DECODER)
	cargo fmt --check --manifest-path $(CARGO_SEARCH)
	cargo test --offline --manifest-path $(CARGO_DECODER)
	cargo test --manifest-path $(CARGO_SEARCH)

parity:
	$(PYTHON) scripts/verify_rust_decoder_kernel.py --iterations 10 --fuzz-cases 200

preview:
	pumpquant preview
