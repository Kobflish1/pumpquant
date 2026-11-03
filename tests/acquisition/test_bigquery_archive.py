from __future__ import annotations

import base64
import hashlib
import re
import shutil
import unittest
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest import TestCase

from pumpquant.acquisition.bigquery.archive import (
    ArchiveConflictError,
    OVHS3Uploader,
    QueryProvenance,
    archive_bigquery_pages,
    decode_bigquery_page,
    load_archive_manifest,
    verify_archive_manifest,
)

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - exercised in minimal installs
    pa = None
    pq = None


SCHEMA = {
    "fields": [
        {"name": "block_slot", "type": "INTEGER"},
        {"name": "block_timestamp", "type": "TIMESTAMP"},
        {"name": "amount", "type": "NUMERIC"},
        {"name": "huge", "type": "BIGNUMERIC"},
        {"name": "payload", "type": "BYTES"},
        {
            "name": "accounts",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "pubkey", "type": "STRING"},
                {"name": "signer", "type": "BOOL"},
            ],
        },
    ]
}


def _page() -> dict[str, object]:
    return {
        "schema": SCHEMA,
        "rows": [
            {
                "f": [
                    {"v": "7"},
                    {"v": "1710000000.123456"},
                    {"v": "1.230000000"},
                    {"v": "2.3"},
                    {"v": base64.b64encode(b"raw").decode()},
                    {"v": [{"v": {"f": [{"v": "wallet"}, {"v": "true"}]}}]},
                ]
            }
        ],
    }


def _bound_page(page_number: int, total_rows: int) -> dict[str, object]:
    return {
        **_page(),
        "job_id": "job-1",
        "page_number": page_number,
        "total_rows": total_rows,
        "total_bytes_processed": 10,
    }


def _provenance(total_rows: int) -> QueryProvenance:
    return QueryProvenance(
        plan_name="pump_instruction_discovery_v1",
        plan_sha256="a" * 64,
        request_id="0f0eeaca-7f6d-52ec-a733-22a2d86c436a",
        job_id="job-1",
        billing_project="billing-project",
        location="us-central1",
        source_tables=("bigquery-public-data.crypto_solana_mainnet_us.Instructions",),
        source_contract_sha256="b" * 64,
        start="2026-06-07T00:00:00Z",
        end="2026-06-08T00:00:00Z",
        dry_run_estimated_bytes=10,
        maximum_bytes_billed=100,
        actual_bytes_processed=10,
        total_rows=total_rows,
    )


class _Body:
    def __init__(self, payload: bytes) -> None:
        self._stream = BytesIO(payload)

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)

    def close(self) -> None:
        self._stream.close()


class _BrokenBody:
    def read(self, size: int = -1) -> bytes:
        del size
        raise ConnectionError("response stream interrupted")

    def close(self) -> None:
        return None


class _Missing(Exception):
    def __init__(self) -> None:
        self.response = {"Error": {"Code": "404"}}


class _Precondition(Exception):
    def __init__(self) -> None:
        self.response = {"Error": {"Code": "PreconditionFailed"}}


class _FakeS3:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, dict[str, str]]] = {}
        self.calls: list[tuple[str, str]] = []

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        del Bucket
        self.calls.append(("head", Key))
        if Key not in self.objects:
            raise _Missing()
        payload, metadata = self.objects[Key]
        return {"ContentLength": len(payload), "Metadata": metadata}

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: Any,
        Metadata: dict[str, str],
        ContentLength: int,
        IfNoneMatch: str,
    ) -> None:
        del Bucket
        self.assertions = (ContentLength, IfNoneMatch)
        if Key in self.objects:
            raise _Precondition()
        payload = Body.read()
        if len(payload) != ContentLength:
            raise AssertionError("wrong content length")
        key = Key
        self.calls.append(("put", key))
        self.objects[key] = (payload, Metadata)

    def get_bucket_location(self, *, Bucket: str) -> dict[str, object]:
        del Bucket
        return {"LocationConstraint": "gra"}

    def list_objects_v2(
        self, *, Bucket: str, Prefix: str, MaxKeys: int, **kwargs: object
    ) -> dict[str, object]:
        del Bucket, kwargs
        keys = [key for key in sorted(self.objects) if key.startswith(Prefix)][:MaxKeys]
        return {
            "Contents": [{"Key": key} for key in keys],
            "IsTruncated": False,
        }

    def get_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Range: str | None = None,
    ) -> dict[str, object]:
        del Bucket
        self.calls.append(("get", Key))
        payload, metadata = self.objects[Key]
        content_range: str | None = None
        if Range is not None:
            match = re.fullmatch(r"bytes=(\d+)-(\d+)", Range)
            if match is None:
                raise AssertionError("invalid test Range")
            start, end = (int(value) for value in match.groups())
            if not 0 <= start <= end < len(payload):
                raise AssertionError("out-of-bounds test Range")
            full_size = len(payload)
            payload = payload[start : end + 1]
            content_range = f"bytes {start}-{end}/{full_size}"
        return {
            "Body": _Body(payload),
            "ContentLength": len(payload),
            "ContentRange": content_range,
            "Metadata": metadata,
        }


class BigQueryArchiveTests(TestCase):
    def test_decoder_keeps_integer_decimal_bytes_and_nested_values(self) -> None:
        row = decode_bigquery_page(_page())[0]
        self.assertIs(type(row["block_slot"]), int)
        self.assertEqual(str(row["amount"]), "1.230000000")
        self.assertEqual(str(row["huge"]), "2.3")
        self.assertEqual(row["payload"], b"raw")
        self.assertEqual(row["accounts"], [{"pubkey": "wallet", "signer": True}])

    @unittest.skipUnless(pa is not None and pq is not None, "archive extra is not installed")
    def test_archive_is_sharded_and_atomic(self) -> None:
        assert pa is not None and pq is not None
        with TemporaryDirectory() as directory:
            root = Path(directory) / "archive"
            manifest = archive_bigquery_pages(
                [_page(), _page()],
                root,
                table="Transactions",
                max_rows_per_shard=1,
            )
            self.assertEqual(manifest.row_count, 2)
            self.assertEqual(len(manifest.shards), 2)
            self.assertEqual(manifest.shards[0].min_block_slot, 7)
            self.assertFalse(list(Path(directory).rglob("*.partial")))
            table = pq.read_table(root / manifest.shards[0].path)
            self.assertEqual(table.schema.field("amount").type, pa.decimal128(38, 9))
            self.assertEqual(table.schema.field("huge").type, pa.decimal256(76, 38))

    @unittest.skipUnless(pa is not None and pq is not None, "archive extra is not installed")
    def test_manifest_round_trip_binds_query_and_local_hashes(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "archive"
            manifest = archive_bigquery_pages(
                [_bound_page(0, 2), _bound_page(1, 2)],
                root,
                table="Instructions",
                max_rows_per_shard=1,
                provenance=_provenance(2),
            )
            loaded = load_archive_manifest(root / "manifest.json")
            self.assertEqual(loaded, manifest)
            verify_archive_manifest(loaded, root)
            (root / loaded.shards[0].path).write_bytes(b"tampered")
            with self.assertRaises(ArchiveConflictError):
                verify_archive_manifest(loaded, root)

    @unittest.skipUnless(pa is not None and pq is not None, "archive extra is not installed")
    def test_bigquery_total_rows_must_match_decoded_archive(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "archive"
            with self.assertRaisesRegex(ValueError, "totalRows"):
                archive_bigquery_pages(
                    [_bound_page(0, 2)],
                    root,
                    provenance=_provenance(2),
                )
            self.assertFalse(root.exists())
            self.assertFalse((Path(directory) / ".archive.partial").exists())

    @unittest.skipUnless(pa is not None and pq is not None, "archive extra is not installed")
    def test_provenance_rejects_duplicate_or_mixed_pages_without_orphans(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "archive"
            duplicate = [_bound_page(0, 2), _bound_page(0, 2)]
            with self.assertRaisesRegex(ValueError, "contiguous"):
                archive_bigquery_pages(
                    duplicate,
                    root,
                    provenance=_provenance(2),
                    max_rows_per_shard=1,
                )
            self.assertFalse(root.exists())
            self.assertFalse((Path(directory) / ".archive.partial").exists())

    def test_s3_upload_verifies_body_and_conflicts(self) -> None:
        fake = _FakeS3()
        uploader = OVHS3Uploader(
            "https://s3.gra.io.cloud.ovh.net/",
            "GRA",
            "bucket",
            "access",
            "secret",
            client=fake,
        )
        with TemporaryDirectory() as directory:
            path = Path(directory) / "part.parquet"
            path.write_bytes(b"immutable")
            result = uploader.upload_file(path, "part.parquet")
            self.assertFalse(result.skipped)
            self.assertEqual(result.sha256, hashlib.sha256(b"immutable").hexdigest())
            self.assertTrue(uploader.upload_file(path, "part.parquet").skipped)
            fake.objects["part.parquet"] = (b"changed", fake.objects["part.parquet"][1])
            with self.assertRaises(ArchiveConflictError):
                uploader.upload_file(path, "part.parquet")

    def test_s3_full_body_verification_retries_interrupted_get(self) -> None:
        class FlakyGetS3(_FakeS3):
            def __init__(self) -> None:
                super().__init__()
                self.break_next_get = True

            def get_object(
                self,
                *,
                Bucket: str,
                Key: str,
                Range: str | None = None,
            ) -> dict[str, object]:
                response = super().get_object(Bucket=Bucket, Key=Key, Range=Range)
                if self.break_next_get:
                    self.break_next_get = False
                    response["Body"] = _BrokenBody()
                return response

        fake = FlakyGetS3()
        uploader = OVHS3Uploader(
            "https://s3.gra.io.cloud.ovh.net/",
            "GRA",
            "bucket",
            "access",
            "secret",
            client=fake,
        )
        with TemporaryDirectory() as directory:
            path = Path(directory) / "part.parquet"
            path.write_bytes(b"immutable")
            result = uploader.upload_file(path, "part.parquet")
        self.assertFalse(result.skipped)
        self.assertEqual(
            [operation for operation, _ in fake.calls].count("get"),
            2,
        )

    def test_s3_upload_recovers_when_transport_fails_after_commit(self) -> None:
        class FlakyPutS3(_FakeS3):
            def __init__(self) -> None:
                super().__init__()
                self.fail_after_commit = True

            def put_object(self, **kwargs: Any) -> None:
                super().put_object(**kwargs)
                if self.fail_after_commit:
                    self.fail_after_commit = False
                    raise ConnectionError("response lost after immutable commit")

        fake = FlakyPutS3()
        uploader = OVHS3Uploader(
            "https://s3.gra.io.cloud.ovh.net/",
            "GRA",
            "bucket",
            "access",
            "secret",
            client=fake,
        )
        with TemporaryDirectory() as directory:
            path = Path(directory) / "part.parquet"
            path.write_bytes(b"immutable")
            result = uploader.upload_file(path, "part.parquet")
        self.assertTrue(result.skipped)
        self.assertEqual(
            [operation for operation, _ in fake.calls].count("put"),
            1,
        )

    def test_s3_verification_falls_back_when_head_is_broken(self) -> None:
        class BrokenHeadS3(_FakeS3):
            def __init__(self) -> None:
                super().__init__()
                self.head_failures = 0

            def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
                if Key in self.objects:
                    self.head_failures += 1
                    raise ConnectionError("persistent HEAD interruption")
                return super().head_object(Bucket=Bucket, Key=Key)

        fake = BrokenHeadS3()
        uploader = OVHS3Uploader(
            "https://s3.gra.io.cloud.ovh.net/",
            "GRA",
            "bucket",
            "access",
            "secret",
            client=fake,
        )
        with TemporaryDirectory() as directory:
            path = Path(directory) / "part.parquet"
            path.write_bytes(b"immutable")
            uploader.upload_file(path, "part.parquet")
        self.assertGreaterEqual(fake.head_failures, 1)

    def test_s3_full_body_verification_uses_bounded_ranges(self) -> None:
        fake = _FakeS3()
        uploader = OVHS3Uploader(
            "https://s3.gra.io.cloud.ovh.net/",
            "GRA",
            "bucket",
            "access",
            "secret",
            client=fake,
        )
        with TemporaryDirectory() as directory:
            path = Path(directory) / "part.parquet"
            path.write_bytes(b"x" * (4 * 1024 * 1024 + 17))
            uploader.upload_file(path, "part.parquet")
        self.assertEqual(
            [operation for operation, _ in fake.calls].count("get"),
            2,
        )

    def test_s3_configuration_is_https_region_bound_and_redacted(self) -> None:
        uploader = OVHS3Uploader(
            "https://s3.eu-south-mil.io.cloud.ovh.net/",
            "EU-SOUTH-MIL",
            "pumpquant-archive",
            "access-value",
            "secret-value",
            client=_FakeS3(),
        )
        self.assertNotIn("access-value", repr(uploader.config))
        self.assertNotIn("secret-value", repr(uploader.config))
        with self.assertRaisesRegex(ValueError, "HTTPS OVH"):
            OVHS3Uploader(
                "http://s3.eu-south-mil.io.cloud.ovh.net/",
                "EU-SOUTH-MIL",
                "pumpquant-archive",
                "access",
                "secret",
                client=_FakeS3(),
            )

    def test_s3_preflight_verifies_bucket_region_and_prefix_listing(self) -> None:
        fake = _FakeS3()
        uploader = OVHS3Uploader(
            "https://s3.gra.io.cloud.ovh.net/",
            "GRA",
            "pumpquant-archive",
            "access",
            "secret",
            client=fake,
        )
        uploader.preflight("raw/archive")

        class WrongRegionS3(_FakeS3):
            def get_bucket_location(self, *, Bucket: str) -> dict[str, object]:
                del Bucket
                return {"LocationConstraint": "rbx"}

        wrong_region = OVHS3Uploader(
            "https://s3.gra.io.cloud.ovh.net/",
            "GRA",
            "pumpquant-archive",
            "access",
            "secret",
            client=WrongRegionS3(),
        )
        with self.assertRaisesRegex(RuntimeError, "region"):
            wrong_region.preflight("raw/archive")

    @unittest.skipUnless(pa is not None and pq is not None, "archive extra is not installed")
    def test_manifest_upload_rejects_unmanifested_prefix_objects(self) -> None:
        fake = _FakeS3()
        prefix = "raw/archive"
        fake.objects[f"{prefix}/unexpected.bin"] = (b"x", {"sha256": "0" * 64})
        uploader = OVHS3Uploader(
            "https://s3.gra.io.cloud.ovh.net/",
            "GRA",
            "pumpquant-archive",
            "access",
            "secret",
            client=fake,
        )
        with TemporaryDirectory() as directory:
            root = Path(directory) / "archive"
            manifest = archive_bigquery_pages([_page()], root)
            with self.assertRaisesRegex(ArchiveConflictError, "unmanifested"):
                uploader.upload_manifest(manifest, root, prefix=prefix)

    @unittest.skipUnless(pa is not None and pq is not None, "archive extra is not installed")
    def test_parallel_manifest_upload_keeps_manifest_last(self) -> None:
        fake = _FakeS3()
        prefix = "raw/archive"
        uploader = OVHS3Uploader(
            "https://s3.gra.io.cloud.ovh.net/",
            "GRA",
            "pumpquant-archive",
            "access",
            "secret",
            client=fake,
        )
        with TemporaryDirectory() as directory:
            root = Path(directory) / "archive"
            manifest = archive_bigquery_pages(
                [_bound_page(0, 2), _bound_page(1, 2)],
                root,
                max_rows_per_shard=1,
                provenance=_provenance(2),
            )
            results = uploader.upload_manifest(
                manifest,
                root,
                prefix=prefix,
                max_workers=2,
            )
            self.assertEqual(len(results), len(manifest.shards) + 1)
            puts = [key for operation, key in fake.calls if operation == "put"]
            self.assertTrue(puts[-1].endswith("/manifest.json"))
            self.assertEqual(set(fake.objects), {result.key for result in results})

    @unittest.skipUnless(pa is not None and pq is not None, "archive extra is not installed")
    def test_remote_manifest_verification_needs_no_local_shards(self) -> None:
        fake = _FakeS3()
        prefix = "raw/archive"
        uploader = OVHS3Uploader(
            "https://s3.gra.io.cloud.ovh.net/",
            "GRA",
            "pumpquant-archive",
            "access",
            "secret",
            client=fake,
        )
        with TemporaryDirectory() as directory:
            root = Path(directory) / "archive"
            manifest = archive_bigquery_pages([_page()], root)
            uploader.upload_manifest(manifest, root, prefix=prefix)
            self.assertEqual(uploader.fetch_manifest(prefix=prefix), manifest)
            shutil.rmtree(root)
            verified = uploader.verify_remote_manifest(manifest, prefix=prefix)
            self.assertEqual(len(verified), len(manifest.shards) + 1)
            self.assertTrue(all(item.skipped for item in verified))
            shard_key = f"{prefix}/{manifest.shards[0].path}"
            fake.objects[shard_key] = (b"changed", fake.objects[shard_key][1])
            with self.assertRaises(ArchiveConflictError):
                uploader.verify_remote_manifest(manifest, prefix=prefix)

    def test_remote_manifest_fetch_distinguishes_empty_and_incomplete_prefix(self) -> None:
        fake = _FakeS3()
        uploader = OVHS3Uploader(
            "https://s3.gra.io.cloud.ovh.net/",
            "GRA",
            "pumpquant-archive",
            "access",
            "secret",
            client=fake,
        )
        self.assertIsNone(uploader.fetch_manifest(prefix="raw/archive"))
        fake.objects["raw/archive/Instructions/part-000000.parquet"] = (
            b"partial",
            {"sha256": hashlib.sha256(b"partial").hexdigest()},
        )
        with self.assertRaisesRegex(ArchiveConflictError, "no manifest"):
            uploader.fetch_manifest(prefix="raw/archive")
