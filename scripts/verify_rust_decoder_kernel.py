#!/usr/bin/env python3
"""Build, parity-check, and benchmark the dependency-free Rust wire kernel.

This script only creates synthetic in-memory fixtures. It never accesses archive
or holdout data. The benchmark reports parser-only throughput after a one-time
binary build; compilation time is excluded.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import random
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
CRATE = ROOT / "rust" / "pump-decoder-kernel"


def binary_path() -> Path:
    """Resolve Cargo's effective target path, including a caller override."""

    raw_target = os.environ.get("CARGO_TARGET_DIR")
    target = Path(raw_target) if raw_target else CRATE / "target"
    if not target.is_absolute():
        target = ROOT / target
    return target.resolve() / "release" / "pump-decoder-kernel"


def shortvec(value: int) -> bytes:
    result = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            result.append(byte | 0x80)
        else:
            result.append(byte)
            return bytes(result)


def fixture_payload(
    *,
    version: int | None,
    seed: int,
    instruction_account: int | None = None,
) -> bytes:
    static_keys = (bytes([seed]) * 32, bytes([(seed + 1) & 0xFF]) * 32)
    message = bytearray()
    if version is not None:
        message.append(0x80 | version)
        message.extend((1, 0, 0))
    else:
        message.extend((1, 0, 0))
    message.extend(shortvec(len(static_keys)))
    for key in static_keys:
        message.extend(key)
    message.extend(bytes([7]) * 32)
    if instruction_account is None:
        message.extend(b"\x00")
    else:
        message.extend(b"\x01")
        message.append(2 if version == 0 else 1)
        message.extend(shortvec(1))
        message.append(instruction_account)
        message.extend(shortvec(1))
        message.append(seed)
    if version is not None:
        message.extend(b"\x01")
        message.extend(bytes([(seed + 2) & 0xFF]) * 32)
        message.extend(b"\x01\x02")
        message.extend(b"\x01\x03")
    payload = bytearray(b"\x01")
    payload.extend(bytes([9 ^ seed]) * 64)
    payload.extend(message)
    return bytes(payload)


def encoded(payload: bytes) -> str:
    return base64.b64encode(payload).decode("ascii")


def build_binary(skip_build: bool) -> Path:
    binary = binary_path()
    if skip_build:
        if not binary.exists():
            raise SystemExit(f"--skip-build requested but binary is missing: {binary}")
        return binary
    cargo = Path(os.environ.get("CARGO", "cargo"))
    if not cargo.exists():
        cargo = Path("cargo")
    environment = os.environ.copy()
    subprocess.run(
        [str(cargo), "build", "--release", "--manifest-path", str(CRATE / "Cargo.toml")],
        cwd=ROOT,
        env=environment,
        check=True,
    )
    if not binary.exists():
        raise RuntimeError(f"Cargo completed but the release binary is missing: {binary}")
    return binary


def run_kernel(binary: Path, values: list[str], *arguments: str) -> list[dict[str, Any]]:
    process = subprocess.run(
        [str(binary), *arguments],
        cwd=ROOT,
        input="".join(f"{value}\n" for value in values),
        text=True,
        capture_output=True,
        check=False,
    )
    if process.returncode != 0:
        raise RuntimeError(f"Rust kernel exited {process.returncode}: {process.stderr.strip()}")
    output = [line for line in process.stdout.splitlines() if line]
    if len(output) != len(values):
        raise RuntimeError(
            f"Rust kernel emitted {len(output)} lines for {len(values)} inputs: {process.stdout!r}"
        )
    return [json.loads(line) for line in output]


def load_python_reference() -> Any:
    sys.path.insert(0, str(ROOT / "src"))
    from pumpquant.decoding.solana_wire import (  # type: ignore[import-not-found]
        MalformedTransactionError,
        UnsupportedTransactionVersionError,
        parse_serialized_transaction,
    )

    return (
        MalformedTransactionError,
        UnsupportedTransactionVersionError,
        parse_serialized_transaction,
    )


def _comparable_result(result: dict[str, Any]) -> dict[str, Any]:
    if result.get("ok") is True:
        return {
            "ok": True,
            "first_signature": result.get("first_signature"),
            "static_account_keys": result.get("static_account_keys"),
        }
    return {"ok": False, "error_kind": result.get("error_kind")}


def _python_result(
    value: str,
    malformed_error: type[Exception],
    unsupported_error: type[Exception],
    parse_python: Any,
) -> dict[str, Any]:
    try:
        signature, keys = parse_python(value)
    except unsupported_error:
        return {"ok": False, "error_kind": "unsupported_transaction_version"}
    except malformed_error:
        return {"ok": False, "error_kind": "malformed_transaction"}
    return {
        "ok": True,
        "first_signature": signature,
        "static_account_keys": list(keys),
    }


def _differential_values(valid_values: list[str], count: int) -> list[str]:
    """Create deterministic wire/base64 edge cases without production data."""

    rng = random.Random(0x50554D505155414E54)
    base_payloads = [base64.b64decode(value, validate=True) for value in valid_values]
    values = [
        "",
        "A",
        "AA=",
        "AA===",
        "A A=",
        "_w==",
        "AB==",  # non-zero unused base64 pad bits
        encoded(b"\x80\x80\x04"),  # compact-u16 overflow
        encoded(b"\x81\x00"),  # noncanonical compact-u16
        encoded(b"\x80\x80\x80"),  # unterminated compact-u16
    ]
    case_index = 0
    while len(values) < count:
        base = base_payloads[case_index % len(base_payloads)]
        mode = case_index % 6
        if mode == 0:
            # Fully arbitrary canonical base64 explores cursor and shortvec
            # rejection while keeping the text envelope valid.
            payload = rng.randbytes(rng.randrange(0, 1_025))
            values.append(encoded(payload))
        elif mode == 1:
            cut = rng.randrange(0, len(base) + 1)
            values.append(encoded(base[:cut]))
        elif mode == 2:
            payload = bytearray(base)
            offset = rng.randrange(len(payload))
            payload[offset] ^= rng.randrange(1, 256)
            values.append(encoded(bytes(payload)))
        elif mode == 3:
            values.append(encoded(base + rng.randbytes(rng.randrange(1, 9))))
        elif mode == 4:
            value = valid_values[case_index % len(valid_values)]
            values.append(value.rstrip("="))
        else:
            value = valid_values[case_index % len(valid_values)]
            offset = rng.randrange(len(value) + 1)
            values.append(f"{value[:offset]} {value[offset:]}")
        case_index += 1
    return values[:count]


def parity_check(binary: Path, fuzz_cases: int) -> tuple[list[str], Any, int]:
    malformed_error, unsupported_error, parse_python = load_python_reference()
    valid_values: list[str] = []
    expected: list[dict[str, Any]] = []
    for index in range(16):
        version = 0 if index % 2 else None
        value = encoded(
            fixture_payload(
                version=version,
                seed=(index + 1) & 0xFF,
                instruction_account=2 if version == 0 else 1,
            )
        )
        signature, static_keys = parse_python(value)
        valid_values.append(value)
        expected.append(
            {
                "ok": True,
                "first_signature": signature,
                "static_account_keys": list(static_keys),
            }
        )

    actual = run_kernel(binary, valid_values, "--jsonl")
    if actual != expected:
        raise AssertionError(
            f"valid parity mismatch:\nexpected={expected[:2]}\nactual={actual[:2]}"
        )

    base_payload = fixture_payload(version=None, seed=31, instruction_account=1)
    cases = [
        (encoded(base_payload) + "!", "malformed_transaction"),
        (encoded(b"\x81\x00" + base_payload[1:]), "malformed_transaction"),
        (encoded(base_payload + b"\x00"), "malformed_transaction"),
        (encoded(fixture_payload(version=1, seed=32)), "unsupported_transaction_version"),
    ]
    case_values = [value for value, _ in cases]
    case_expected = []
    for value, kind in cases:
        try:
            parse_python(value)
        except unsupported_error:
            python_kind = "unsupported_transaction_version"
        except malformed_error:
            python_kind = "malformed_transaction"
        else:
            raise AssertionError(f"Python reference accepted negative fixture {value!r}")
        if python_kind != kind:
            raise AssertionError(f"Python classified {kind} fixture as {python_kind}")
        case_expected.append({"ok": False, "error_kind": kind})
    case_actual = run_kernel(binary, case_values, "--jsonl")
    for actual_case, expected_case in zip(case_actual, case_expected, strict=True):
        if (
            actual_case.get("ok") is not False
            or actual_case.get("error_kind") != expected_case["error_kind"]
        ):
            raise AssertionError(f"negative parity mismatch: {actual_case} != {expected_case}")
    differential_values = _differential_values(valid_values, fuzz_cases)
    differential_expected = [
        _python_result(value, malformed_error, unsupported_error, parse_python)
        for value in differential_values
    ]
    differential_actual = run_kernel(binary, differential_values, "--jsonl")
    comparable_actual = [_comparable_result(result) for result in differential_actual]
    if comparable_actual != differential_expected:
        for index, (expected_result, actual_result) in enumerate(
            zip(differential_expected, comparable_actual, strict=True)
        ):
            if expected_result != actual_result:
                raise AssertionError(
                    "differential parity mismatch at case "
                    f"{index}: expected={expected_result}, actual={actual_result}"
                )
        raise AssertionError("differential parity mismatch")
    return valid_values, parse_python, len(differential_values)


def benchmark(binary: Path, valid_values: list[str], parse_python: Any, iterations: int) -> None:
    start = time.perf_counter()
    checksum = 0
    for _ in range(iterations):
        for value in valid_values:
            signature, keys = parse_python(value)
            checksum += len(signature) + sum(map(len, keys))
    python_elapsed = time.perf_counter() - start

    process = subprocess.run(
        [str(binary), "--benchmark", "--iterations", str(iterations)],
        cwd=ROOT,
        input="".join(f"{value}\n" for value in valid_values),
        text=True,
        capture_output=True,
        check=False,
    )
    if process.returncode != 0:
        raise RuntimeError(f"Rust benchmark failed: {process.stderr.strip()}")
    report = process.stdout.strip()
    metrics = dict(re.findall(r"([a-z_]+)=([0-9.]+)", report))
    rust_elapsed = float(metrics["elapsed_seconds"])
    rust_total = int(metrics["total_parses"])
    python_total = len(valid_values) * iterations
    python_throughput = python_total / max(python_elapsed, sys.float_info.min)
    rust_throughput = rust_total / max(rust_elapsed, sys.float_info.min)
    print(
        "benchmark methodology: synthetic legacy/v0 wire fixtures; parser-only; "
        "release binary; build time excluded; each iteration reparses every record"
    )
    print(f"python_records={len(valid_values)} python_iterations={iterations}")
    print(f"python_elapsed_seconds={python_elapsed:.6f}")
    print(f"python_throughput_transactions_per_second={python_throughput:.2f}")
    print(f"rust_report={report}")
    print(f"rust_throughput_transactions_per_second={rust_throughput:.2f}")
    speedup = rust_throughput / max(python_throughput, sys.float_info.min)
    print(f"rust_over_python_speedup={speedup:.2f}")
    if checksum == 0:
        raise AssertionError("unexpected empty benchmark checksum")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=1_000)
    parser.add_argument("--fuzz-cases", type=int, default=512)
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    if args.iterations <= 0:
        parser.error("--iterations must be positive")
    if args.fuzz_cases <= 0:
        parser.error("--fuzz-cases must be positive")
    binary = build_binary(args.skip_build)
    valid_values, parse_python, differential_count = parity_check(binary, args.fuzz_cases)
    print(
        f"parity: {len(valid_values)} valid, 4 targeted malformed/version, and "
        f"{differential_count} deterministic differential fixtures matched"
    )
    benchmark(binary, valid_values, parse_python, args.iterations)


if __name__ == "__main__":
    main()
