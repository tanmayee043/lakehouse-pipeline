"""Backfill automatically when Project 1 reports a gap."""

import hashlib
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

import psycopg2
from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SensorDefinition,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from ingestion.crypto.settings import CryptoSettings
from ingestion.crypto.stream_extract import connect_read_only, open_gap_days


def build_gap_sensor(job: Any) -> SensorDefinition:
    """job: the crypto asset job (from define_asset_job) to run for each day with open gaps."""

    @sensor(
        job=job,
        minimum_interval_seconds=15 * 60,
        # Off until switched on in the UI, like the schedules.
        default_status=DefaultSensorStatus.STOPPED,
        description="Re-runs a day's crypto partition when Project 1 reports an open gap on it.",
    )
    def project1_gap_sensor(context: SensorEvaluationContext) -> Iterator[RunRequest | SkipReason]:
        try:
            conn = connect_read_only()
        except psycopg2.OperationalError:
            yield SkipReason("Project 1's database isn't reachable")
            return
        try:
            gap_days = open_gap_days(conn)
        finally:
            conn.close()

        start = CryptoSettings().crypto_start_date  # type: ignore[call-arg]
        today = datetime.now(timezone.utc).date()
        requested = 0
        for day, gaps in gap_days.items():
            if not start <= day < today:  # outside the partitions, or the day isn't over yet
                continue
            # The run key changes whenever the day's set of open gaps changes, so each new gap
            # triggers exactly one re-run and an unchanged day is never re-run twice.
            digest = hashlib.sha1(",".join(sorted(gaps)).encode()).hexdigest()[:12]
            requested += 1
            yield RunRequest(partition_key=day.isoformat(), run_key=f"gaps-{day.isoformat()}-{digest}")
        if not requested:
            yield SkipReason("No open gaps on completed days")

    return project1_gap_sensor
