from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pumpquant.acquisition.pumpapi_replay import SCHEMA
from pumpquant.features.checkpoint_features import (
    EXTENDED_FEATURE_GROUPS,
    EXTENDED_FEATURES,
    FEATURE_GROUPS,
    FEATURES,
    V4_FEATURE_GROUPS,
    V4_FEATURES,
)
from pumpquant.features.checkpoint_states import materialize_checkpoint_states


def _row(**changes: object) -> dict[str, object]:
    row = {field.name: None for field in SCHEMA}
    row.update(
        {
            "source_hour_utc": 0,
            "archive_sequence": 1,
            "signature": "sig",
            "action": "create",
            "pool": "pump",
            "block": 1,
            "timestamp_ms": 1_000,
            "local_timestamp_ms": 1_001,
            "pool_id": "curve",
            "mint": "mint",
            "quote_mint": "wsol",
            "tx_signer": "creator",
            "tokensInPool": "793000000",
            "quoteInPool": "0",
            "vTokensInBondingCurve": "1073000000",
            "vQuoteInBondingCurve": "30",
            "price": "0.000000028",
            "marketCapQuote": "28",
            "poolFeeRate": "0.0125",
            "supply": "1000000000",
            "decimals": "6",
            "mayhem_mode": False,
            "cashback_enabled": False,
            "traders_involved_json": "{}",
            "programs_used_json": "[]",
        }
    )
    row.update(changes)
    return row


def test_materializes_strict_time_and_event_checkpoints(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    rows = [
        _row(signature="create-a"),
        _row(
            archive_sequence=2,
            signature="buy-a",
            action="buy",
            timestamp_ms=1_250,
            tx_signer="buyer",
            tokenAmount="100",
            quoteAmount="1",
            tokensInPool="792999900",
            quoteInPool="1",
            vTokensInBondingCurve="1072999900",
            vQuoteInBondingCurve="31",
            price="0.000000029",
            marketCapQuote="29",
        ),
        _row(
            archive_sequence=3,
            signature="sell-a",
            action="sell",
            timestamp_ms=1_400,
            tx_signer="creator",
            tokenAmount="50",
            quoteAmount="0.4",
            tokensInPool="792999950",
            quoteInPool="0.6",
            vTokensInBondingCurve="1072999950",
            vQuoteInBondingCurve="30.6",
            price="0.0000000285",
            marketCapQuote="28.5",
        ),
        _row(
            archive_sequence=4,
            signature="create-zero",
            mint="zero",
            pool_id="zero-curve",
            tx_signer="zero-creator",
            timestamp_ms=2_000,
        ),
    ]
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), source / "part.parquet")
    output = tmp_path / "output"
    result = materialize_checkpoint_states(
        str(source / "*.parquet"),
        output,
        time_checkpoints_ms=(250, 500),
        event_checkpoints=(1, 2),
        outcome_horizon_ms=300,
        threads=2,
    )
    assert result["launches"] == 2
    table = pq.read_table(output / "parts").to_pylist()
    by_key = {(row["mint"], row["checkpoint_kind"], row["checkpoint_value"]): row for row in table}
    # Strict elapsed-time boundary excludes the trade stamped exactly at 1250.
    assert by_key[("mint", "time_ms", 250)]["trade_count"] == 0
    assert by_key[("mint", "time_ms", 500)]["trade_count"] == 2
    assert by_key[("mint", "time_ms", 500)]["right_censored_300s"] is False
    assert by_key[("zero", "time_ms", 500)]["right_censored_300s"] is True
    # An event checkpoint includes the event that defines it.
    assert by_key[("mint", "trade_event", 1)]["trade_count"] == 1
    assert by_key[("mint", "trade_event", 2)]["trade_count"] == 2
    assert by_key[("mint", "trade_event", 2)]["creator_trade_count"] == 1
    second_trade = by_key[("mint", "trade_event", 2)]
    assert second_trade["buy_count_last_1s"] == 1
    assert second_trade["sell_count_last_1s"] == 1
    assert second_trade["maximum_buy_quote"] == 1
    assert second_trade["maximum_sell_quote"] == 0.4
    assert second_trade["current_buy_streak"] == 0
    assert second_trade["current_sell_streak"] == 1
    assert second_trade["milliseconds_since_buy"] == 150
    assert second_trade["quote_amount_sum_squares"] == pytest.approx(1.16)
    assert second_trade["signer_quote_sum_squares"] == pytest.approx(1.16)
    assert by_key[("zero", "time_ms", 500)]["trade_count"] == 0
    assert json.loads((output / "manifest.json").read_text())["status"] == "complete"

    # The shared discovery registry must remain executable against the causal
    # state schema; this catches feature drift before a large research run.
    import duckdb

    select = ", ".join(f"{expression} AS {name}" for name, expression in FEATURES.items())
    feature_row = (
        duckdb.connect()
        .execute(
            f"""
        SELECT {select}
        FROM read_parquet('{output / "parts" / "*.parquet"}')
        WHERE mint = 'mint' AND checkpoint_kind = 'trade_event'
          AND checkpoint_value = 2
        """
        )
        .fetchone()
    )
    assert feature_row is not None
    assert len(feature_row) == len(FEATURES)
    assert len(FEATURES) >= 90
    assert set().union(*(group.keys() for group in FEATURE_GROUPS.values())) == set(FEATURES)

    extended_select = ", ".join(
        f"{expression} AS {name}" for name, expression in EXTENDED_FEATURES.items()
    )
    extended_row = (
        duckdb.connect()
        .execute(
            f"""
        SELECT {extended_select}
        FROM read_parquet('{output / "parts" / "*.parquet"}')
        WHERE mint = 'mint' AND checkpoint_kind = 'trade_event'
          AND checkpoint_value = 2
        """
        )
        .fetchone()
    )
    assert extended_row is not None
    assert len(extended_row) == len(EXTENDED_FEATURES)
    assert len(EXTENDED_FEATURES) >= 250
    assert set().union(*(group.keys() for group in EXTENDED_FEATURE_GROUPS.values())) == set(
        EXTENDED_FEATURES
    )

    v4_select = ", ".join(f"{expression} AS {name}" for name, expression in V4_FEATURES.items())
    v4_row = (
        duckdb.connect()
        .execute(
            f"""
        SELECT {v4_select}
        FROM read_parquet('{output / "parts" / "*.parquet"}')
        WHERE mint = 'mint' AND checkpoint_kind = 'trade_event'
          AND checkpoint_value = 2
        """
        )
        .fetchone()
    )
    assert v4_row is not None
    assert len(V4_FEATURES) > len(EXTENDED_FEATURES)
    assert set().union(*(group.keys() for group in V4_FEATURE_GROUPS.values())) == set(V4_FEATURES)
