from __future__ import annotations

import base64
from types import MappingProxyType
from unittest import TestCase

from pumpquant.decoding.instruction_intent import PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID
from pumpquant.decoding.program_events import (
    PUMP_COMPLETE_EVENT_DISCRIMINATOR,
    PUMP_COMPLETE_PUMP_AMM_MIGRATION_EVENT_DISCRIMINATOR,
    PUMP_CREATE_EVENT_DISCRIMINATOR,
    PUMP_TRADE_EVENT_DISCRIMINATOR,
    PUMPSWAP_BUY_EVENT_DISCRIMINATOR,
    PUMPSWAP_CREATE_POOL_EVENT_DISCRIMINATOR,
    PUMPSWAP_SELL_EVENT_DISCRIMINATOR,
    MalformedProgramEventError,
    PumpCompleteEvent,
    PumpCompletePumpAmmMigrationEvent,
    PumpCreateEvent,
    PumpSwapCreatePoolEvent,
    Shareholder,
    UnknownProgramEventError,
    decode_lifecycle_event,
    decode_trade_event,
    extract_lifecycle_event_logs,
    extract_trade_event_logs,
)


def _u64(value: int) -> bytes:
    return value.to_bytes(8, "little")


def _i64(value: int) -> bytes:
    return value.to_bytes(8, "little", signed=True)


def _i128(value: int) -> bytes:
    return value.to_bytes(16, "little", signed=True)


def _u16(value: int) -> bytes:
    return value.to_bytes(2, "little")


def _text(value: str) -> bytes:
    encoded = value.encode()
    return len(encoded).to_bytes(4, "little") + encoded


def _pubkey(seed: int) -> bytes:
    return bytes((seed,)) * 32


def _pump_event() -> bytes:
    return b"".join(
        (
            PUMP_TRADE_EVENT_DISCRIMINATOR,
            _pubkey(1),
            _u64(100),
            _u64(200),
            b"\x01",
            _pubkey(2),
            _i64(1_700_000_000),
            _u64(1_000),
            _u64(2_000),
            _u64(900),
            _u64(1_800),
            _pubkey(3),
            _u64(95),
            _u64(2),
            _pubkey(4),
            _u64(30),
            _u64(1),
            b"\x01",
            _u64(10),
            _u64(20),
            _u64(30),
            _i64(1_700_000_001),
            _text("buy_exact_quote_in"),
            b"\x00",
            _u64(5),
            _u64(1),
            _u64(10),
            _u64(1),
            (1).to_bytes(4, "little"),
            _pubkey(5),
            _u16(10_000),
            _pubkey(0),
            _u64(100),
            _u64(1_000),
            _u64(900),
        )
    )


def _pumpswap_buy_event() -> bytes:
    numeric_prefix = (10, 20, 30, 40, 50, 60, 70, 80, 20, 2, 5, 1, 82, 83)
    identities = b"".join(_pubkey(seed) for seed in range(1, 8))
    return b"".join(
        (
            PUMPSWAP_BUY_EVENT_DISCRIMINATOR,
            _i64(1_700_000_000),
            b"".join(_u64(value) for value in numeric_prefix[1:]),
            identities,
            _u64(5),
            _u64(1),
            b"\x01",
            _u64(10),
            _u64(20),
            _u64(30),
            _i64(1_700_000_001),
            _u64(19),
            _text("buy_exact_quote_in"),
            _u64(0),
            _u64(0),
            _u64(10),
            _u64(1),
        )
    )


def _pumpswap_sell_event() -> bytes:
    # Thirteen u64 values follow timestamp before the seven identities.
    before_identities = (20, 30, 40, 50, 60, 70, 80, 20, 2, 5, 1, 78, 77)
    identities = b"".join(_pubkey(seed) for seed in range(1, 8))
    return b"".join(
        (
            PUMPSWAP_SELL_EVENT_DISCRIMINATOR,
            _i64(1_700_000_000),
            b"".join(_u64(value) for value in before_identities),
            identities,
            _u64(5),
            _u64(1),
            _u64(0),
            _u64(0),
            _u64(10),
            _u64(1),
        )
    )


def _pump_create_event() -> bytes:
    return b"".join(
        (
            PUMP_CREATE_EVENT_DISCRIMINATOR,
            _text("Token Name"),
            _text("TKN"),
            _text("https://example.invalid/token.json"),
            _pubkey(1),
            _pubkey(2),
            _pubkey(3),
            _pubkey(4),
            _i64(1_700_000_000),
            _u64(1_000_000),
            _u64(2_000_000),
            _u64(900_000),
            _u64(1_000_000_000),
            _pubkey(5),
            b"\x01",
            b"\x00",
            _pubkey(6),
            _u64(3_000_000),
        )
    )


def _pump_complete_event() -> bytes:
    return b"".join(
        (
            PUMP_COMPLETE_EVENT_DISCRIMINATOR,
            _pubkey(1),
            _pubkey(2),
            _pubkey(3),
            _i64(1_700_000_001),
            _pubkey(6),
        )
    )


def _pump_migration_event() -> bytes:
    return b"".join(
        (
            PUMP_COMPLETE_PUMP_AMM_MIGRATION_EVENT_DISCRIMINATOR,
            _pubkey(1),
            _pubkey(2),
            _u64(100),
            _u64(200),
            _u64(3),
            _pubkey(4),
            _i64(1_700_000_002),
            _pubkey(5),
            _pubkey(6),
        )
    )


def _pumpswap_create_pool_event() -> bytes:
    return b"".join(
        (
            PUMPSWAP_CREATE_POOL_EVENT_DISCRIMINATOR,
            _i64(1_700_000_003),
            _u16(7),
            _pubkey(1),
            _pubkey(2),
            _pubkey(3),
            b"\x06",
            b"\x09",
            _u64(100),
            _u64(200),
            _u64(300),
            _u64(400),
            _u64(5),
            _u64(6),
            _u64(7),
            b"\x08",
            _pubkey(4),
            _pubkey(5),
            _pubkey(6),
            _pubkey(7),
            _pubkey(8),
            b"\x01",
        )
    )


class ProgramEventTests(TestCase):
    def test_decodes_all_pinned_lifecycle_event_layouts(self) -> None:
        create = decode_lifecycle_event(PUMP_PROGRAM_ID, _pump_create_event())
        complete = decode_lifecycle_event(PUMP_PROGRAM_ID, _pump_complete_event())
        migration = decode_lifecycle_event(PUMP_PROGRAM_ID, _pump_migration_event())
        pool = decode_lifecycle_event(PUMPSWAP_PROGRAM_ID, _pumpswap_create_pool_event())

        self.assertIsInstance(create, PumpCreateEvent)
        self.assertEqual(
            (create.name, create.symbol, create.uri),
            ("Token Name", "TKN", "https://example.invalid/token.json"),
        )
        self.assertEqual(
            (create.timestamp, create.is_mayhem_mode, create.is_cashback_enabled),
            (1_700_000_000, True, False),
        )
        self.assertEqual(create.values["virtual_quote_reserves"], 3_000_000)
        self.assertEqual(create.raw_data, _pump_create_event())

        self.assertIsInstance(complete, PumpCompleteEvent)
        self.assertEqual(
            (complete.timestamp, complete.quote_mint), (1_700_000_001, create.quote_mint)
        )

        self.assertIsInstance(migration, PumpCompletePumpAmmMigrationEvent)
        self.assertEqual(
            (migration.mint_amount, migration.sol_amount, migration.pool_migration_fee),
            (100, 200, 3),
        )
        self.assertEqual(migration.event_name, "CompletePumpAmmMigrationEvent")

        self.assertIsInstance(pool, PumpSwapCreatePoolEvent)
        self.assertEqual(
            (pool.index, pool.base_mint_decimals, pool.quote_mint_decimals, pool.pool_bump),
            (7, 6, 9, 8),
        )
        self.assertEqual(pool.is_mayhem_mode, True)
        self.assertEqual(pool.values["lp_token_amount_out"], 7)

    def test_rejects_malformed_lifecycle_payloads_fail_closed(self) -> None:
        create = _pump_create_event()
        with self.assertRaisesRegex(MalformedProgramEventError, "truncated"):
            decode_lifecycle_event(PUMP_PROGRAM_ID, create[:-1])
        with self.assertRaisesRegex(MalformedProgramEventError, "trailing"):
            decode_lifecycle_event(PUMP_PROGRAM_ID, create + b"x")

        # The first boolean follows the three strings, four pubkeys, a
        # timestamp, and four u64 values.
        bool_offset = 8
        bool_offset += sum(
            4 + len(value.encode())
            for value in ("Token Name", "TKN", "https://example.invalid/token.json")
        )
        bool_offset += 4 * 32 + 8 + 4 * 8 + 32
        malformed_bool = bytearray(create)
        malformed_bool[bool_offset] = 2
        with self.assertRaisesRegex(MalformedProgramEventError, "boolean"):
            decode_lifecycle_event(PUMP_PROGRAM_ID, bytes(malformed_bool))

        malformed_utf8 = bytearray(create)
        malformed_utf8[12] = 0xFF  # four-byte name length starts at offset 8
        with self.assertRaisesRegex(MalformedProgramEventError, "UTF-8"):
            decode_lifecycle_event(PUMP_PROGRAM_ID, bytes(malformed_utf8))

        malformed_length = bytearray(create)
        malformed_length[8:12] = (0xFFFF_FFFF).to_bytes(4, "little")
        with self.assertRaisesRegex(MalformedProgramEventError, "truncated"):
            decode_lifecycle_event(PUMP_PROGRAM_ID, bytes(malformed_length))

        with self.assertRaises(UnknownProgramEventError):
            decode_lifecycle_event(PUMPSWAP_PROGRAM_ID, create)

    def test_rejects_pumpswap_lifecycle_truncation_and_invalid_bool(self) -> None:
        pool = _pumpswap_create_pool_event()
        with self.assertRaisesRegex(MalformedProgramEventError, "truncated"):
            decode_lifecycle_event(PUMPSWAP_PROGRAM_ID, pool[:-1])
        malformed_bool = bytearray(pool)
        malformed_bool[-1] = 2
        with self.assertRaisesRegex(MalformedProgramEventError, "boolean"):
            decode_lifecycle_event(PUMPSWAP_PROGRAM_ID, bytes(malformed_bool))

    def test_extracts_lifecycle_logs_from_emitting_invocation(self) -> None:
        create = base64.b64encode(_pump_create_event()).decode()
        pool = base64.b64encode(_pumpswap_create_pool_event()).decode()
        logs = [
            f"Program {PUMP_PROGRAM_ID} invoke [1]",
            f"Program {PUMPSWAP_PROGRAM_ID} invoke [2]",
            f"Program data: {pool}",
            f"Program {PUMPSWAP_PROGRAM_ID} success",
            f"Program data: {create}",
            f"Program {PUMP_PROGRAM_ID} success",
        ]
        events = extract_lifecycle_event_logs(logs)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].invoke_depth, 2)
        self.assertEqual(events[0].log_index, 2)
        self.assertIsInstance(events[0].event, PumpSwapCreatePoolEvent)
        self.assertEqual(events[1].invoke_depth, 1)
        self.assertIsInstance(events[1].event, PumpCreateEvent)

    def test_decodes_pump_trade_event_and_shareholders(self) -> None:
        event = decode_trade_event(PUMP_PROGRAM_ID, _pump_event())
        self.assertEqual(event.event_name, "TradeEvent")
        self.assertEqual(event.side, "buy")
        self.assertEqual(event.values["sol_amount"], 100)
        self.assertEqual(event.values["token_amount"], 200)
        self.assertEqual(event.values["ix_name"], "buy_exact_quote_in")
        shareholders = event.values["shareholders"]
        self.assertIsInstance(shareholders, tuple)
        self.assertEqual(len(shareholders), 1)  # type: ignore[arg-type]
        self.assertIsInstance(shareholders[0], Shareholder)  # type: ignore[index]
        self.assertEqual(shareholders[0].share_bps, 10_000)  # type: ignore[index,union-attr]
        self.assertIsInstance(event.values, MappingProxyType)

    def test_decodes_pumpswap_buy_and_sell_events(self) -> None:
        buy = decode_trade_event(PUMPSWAP_PROGRAM_ID, _pumpswap_buy_event())
        sell = decode_trade_event(PUMPSWAP_PROGRAM_ID, _pumpswap_sell_event())
        self.assertEqual((buy.event_name, buy.side), ("BuyEvent", "buy"))
        self.assertEqual((sell.event_name, sell.side), ("SellEvent", "sell"))
        self.assertEqual(buy.values["base_amount_out"], 20)
        self.assertEqual(buy.values["user_quote_amount_in"], 83)
        self.assertEqual(buy.values["ix_name"], "buy_exact_quote_in")
        self.assertEqual(sell.values["base_amount_in"], 20)
        self.assertEqual(sell.values["user_quote_amount_out"], 77)

    def test_decodes_current_pumpswap_trade_suffix(self) -> None:
        buy = decode_trade_event(
            PUMPSWAP_PROGRAM_ID,
            _pumpswap_buy_event() + _i128(123_456_789) + b"\x01",
        )
        sell = decode_trade_event(
            PUMPSWAP_PROGRAM_ID,
            _pumpswap_sell_event() + _i128(-123_456_789) + b"\x00",
        )
        self.assertEqual(buy.values["virtual_quote_reserves"], 123_456_789)
        self.assertIs(buy.values["can_boost"], True)
        self.assertEqual(sell.values["virtual_quote_reserves"], -123_456_789)
        self.assertIs(sell.values["can_boost"], False)

    def test_decodes_latest_pumpswap_base_supply_suffix(self) -> None:
        buy = decode_trade_event(
            PUMPSWAP_PROGRAM_ID,
            _pumpswap_buy_event() + _i128(123_456_789) + b"\x01" + _u64(999_000_000),
        )
        self.assertEqual(buy.values["base_supply"], 999_000_000)

    def test_invocation_stack_attributes_only_emitting_program(self) -> None:
        encoded = base64.b64encode(_pump_event()).decode()
        unknown = base64.b64encode(b"unknown!" + b"payload").decode()
        logs = [
            f"Program {PUMP_PROGRAM_ID} invoke [1]",
            f"Program data: {unknown}",
            f"Program data: {encoded}",
            f"Program {PUMP_PROGRAM_ID} success",
            f"Program {PUMPSWAP_PROGRAM_ID} invoke [1]",
            f"Program data: {encoded}",
            f"Program {PUMPSWAP_PROGRAM_ID} success",
        ]
        events = extract_trade_event_logs(logs)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].log_index, 2)
        self.assertEqual(events[0].invoke_depth, 1)
        self.assertEqual(events[0].event.program_id, PUMP_PROGRAM_ID)

    def test_rejects_truncation_trailing_bytes_bad_bool_and_wrong_program(self) -> None:
        with self.assertRaisesRegex(MalformedProgramEventError, "truncated"):
            decode_trade_event(PUMP_PROGRAM_ID, _pump_event()[:-1])
        with self.assertRaisesRegex(MalformedProgramEventError, "trailing"):
            decode_trade_event(PUMP_PROGRAM_ID, _pump_event() + b"x")
        malformed_bool = bytearray(_pump_event())
        malformed_bool[8 + 32 + 8 + 8] = 2
        with self.assertRaisesRegex(MalformedProgramEventError, "boolean"):
            decode_trade_event(PUMP_PROGRAM_ID, bytes(malformed_bool))
        with self.assertRaises(UnknownProgramEventError):
            decode_trade_event(PUMPSWAP_PROGRAM_ID, _pump_event())

    def test_rejects_noncanonical_base64(self) -> None:
        encoded = base64.b64encode(_pump_event()).decode()
        with self.assertRaisesRegex(MalformedProgramEventError, "canonical base64"):
            decode_trade_event(PUMP_PROGRAM_ID, encoded + "\n")

    def test_real_chain_pump_trade_event_golden_vector(self) -> None:
        # Finalized slot 430036432, transaction index 322.
        encoded = (
            "vdt/007mYe4qVg7B2HkCtE48pdVjVRKCwKfqeCS9J7H4LRWa6nMT3+8UxgsAAAAAUKFitgUB"
            "AAABs4L9AVpCw2JYLzovsGvRjrp5J9dECyj/bt5L4197Vlz4r0RqAAAAAMKt3IgRAAAAAAQO"
            "w76EAQDCAbmMCgAAAABs+3YthgAASsL40N1cvJfjKJwZfLUGKlTz2Va5zm5RFfllZ6pcs+Zf"
            "AAAAAAAAAECiHAAAAAAAtZIlN5rGK3JVuQ67fcwusmto1L429KKVcHZkq7Psg3weAAAAAAAA"
            "ANEKCQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEgAAAGJ1eV9leGFj"
            "dF9xdW90ZV9pbgAAAAAAAAAAAAAAAAAAAAAAiBMAAAAAAAAgUQ4AAAAAAAEAAACY990EJa4T"
            "mL+14yyEVnahumUZlBZidNzraHjB9X+UvBAnAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            "AAAAAADvFMYLAAAAAMKt3IgRAAAAwgG5jAoAAAA="
        )
        event = decode_trade_event(PUMP_PROGRAM_ID, encoded)
        self.assertEqual(event.values["mint"], "3rGEXEFJVmMK6ss53CeyUhSrT6QW9tSzSN1iDz5epump")
        self.assertEqual(event.values["sol_amount"], 197_530_863)
        self.assertEqual(event.values["token_amount"], 1_124_046_381_392)
        self.assertEqual(event.values["quote_amount"], 197_530_863)
        self.assertEqual(event.values["virtual_quote_reserves"], 75_310_607_810)
