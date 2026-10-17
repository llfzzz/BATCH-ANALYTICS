"""Spark-backed edge cases: correctness rather than snapshots of implementation."""
import json
import math
import os
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.generate import make_row
from src.pipeline import RAW_SCHEMA, ROOT, create_spark, eligible_jobs, ingest, latest_versions, normalize, trend_frame, run_pipeline
import random

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


@pytest.fixture(scope="session")
def spark():
    session = create_spark()
    yield session
    session.stop()


@pytest.fixture
def profile():
    return json.loads((ROOT / "config" / "profile.json").read_text())


def base(**changes):
    row = make_row(100, 10, random.Random(42))
    row.update(employment_type="实习", education="本科", graduation_year_min=2026,
               graduation_year_max=2027, role="大数据开发", longitude=113.893232,
               latitude=22.559690, status="active")
    row.update(changes)
    return row


def frame(spark, values):
    raw = spark.createDataFrame([(json.dumps(row, ensure_ascii=False),) for row in values], "raw_line string")
    return (raw.withColumn("parsed", F.from_json("raw_line", RAW_SCHEMA, {"mode": "PERMISSIVE"}))
            .select("raw_line", "parsed.*").withColumn("record_hash", F.sha2("raw_line", 256)))


@pytest.mark.parametrize("salary,low,high", [
    ("200-300元/天", 4350.0, 6525.0), ("6-9K/月", 6000.0, 9000.0),
    ("15-24万/年", 12500.0, 20000.0), ("30-45元/小时", 5220.0, 7830.0),
    ("250元/天", 5437.5, 5437.5), ("面议", None, None),
])
def test_salary_normalization(spark, salary, low, high):
    row = normalize(frame(spark, [base(salary_text=salary)])).first()
    assert row.salary_monthly_min == low
    assert row.salary_monthly_max == high
    assert row.quality_reason == ""


def test_bad_salary_and_geo_are_quarantined(spark):
    rows = normalize(frame(spark, [base(salary_text="300-200元/天"), base(latitude=220)])).collect()
    assert "invalid_salary_range" in rows[0].quality_reason
    assert "invalid_coordinates" in rows[1].quality_reason


def test_hard_filters_and_distance_boundary(spark, profile):
    delta = lambda km: math.degrees(km / 6371.0088)
    values = [
        base(source_job_id="inside", latitude=22.559690 + delta(4.999)),
        base(source_job_id="boundary", latitude=22.559690 + delta(5.0)),
        base(source_job_id="outside", latitude=22.559690 + delta(5.001)),
        base(source_job_id="unknown", longitude=None),
        base(source_job_id="other-system", coordinate_system="WGS84"),
        base(source_job_id="full-time", employment_type="全职"),
        base(source_job_id="master", education="硕士"),
        base(source_job_id="wrong-year", graduation_year_min=2028, graduation_year_max=2029),
        base(source_job_id="unknown-year", graduation_year_min=None, graduation_year_max=None),
        base(source_job_id="closed", status="closed"),
        base(source_job_id="other-role", role="前端开发"),
    ]
    ids = {row.source_job_id for row in eligible_jobs(normalize(frame(spark, values)), profile).select("source_job_id").collect()}
    assert ids == {"inside", "boundary"}


def test_latest_closed_snapshot_and_backfill(spark):
    rows = [base(snapshot_ts="2026-10-10T09:00:00+08:00"),
            base(snapshot_ts="2026-10-12T09:00:00+08:00", status="closed"),
            base(snapshot_ts="2026-10-09T09:00:00+08:00", status="active")]
    _, latest = latest_versions(normalize(frame(spark, rows)))
    assert latest.count() == 1
    assert latest.first().status == "closed"
    assert latest.first().snapshot_date == "2026-10-12"


def test_aliases_and_cross_source_dedup(spark):
    rows = [base(skills=["PySpark", "Spark SQL", "SQL", "Apache Kafka"], source="source-a"),
            base(skills=["Python"], source="source-b", source_job_id="mirror", snapshot_ts="2026-10-12T09:00:00+08:00")]
    history = normalize(frame(spark, rows))
    assert history.filter("source = 'source-a'").first().canonical_skills == ["kafka", "spark", "sql"]
    source_latest, latest = latest_versions(history)
    assert source_latest.count() == 2
    assert latest.count() == 1
    assert latest.first().source == "source-b"


def test_immutable_incremental_batch_and_backfill(spark, tmp_path):
    inputs = tmp_path / "input"
    inputs.mkdir()
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    first_path = inputs / "batch-a.jsonl"
    first_path.write_text(json.dumps(base()) + "\n")
    manifest, new = ingest(spark, inputs, warehouse)
    assert new == ["batch-a"]
    manifest, new = ingest(spark, inputs, warehouse)
    assert new == []
    assert spark.read.parquet(str(warehouse / "ods")).count() == 1
    (inputs / "batch-b.jsonl").write_text(json.dumps(base(snapshot_ts="2026-10-12T09:00:00+08:00", status="closed")) + "\n")
    (inputs / "backfill-old.jsonl").write_text(json.dumps(base(snapshot_ts="2026-10-09T09:00:00+08:00")) + "\n")
    manifest, new = ingest(spark, inputs, warehouse)
    assert new == ["batch-b", "backfill-old"]
    history = normalize(spark.read.parquet(str(warehouse / "ods")))
    _, latest = latest_versions(history)
    assert history.count() == 3
    assert latest.first().status == "closed"
    first_path.write_text(json.dumps(base(title="changed payload")) + "\n")
    with pytest.raises(ValueError, match="Immutable batch"):
        ingest(spark, inputs, warehouse)


def test_daily_asof_trend_counts_closed_skill_as_zero(spark):
    values = [base(skills=["Kafka"], snapshot_ts="2026-10-10T09:00:00+08:00"),
              base(skills=["Kafka"], snapshot_ts="2026-10-12T09:00:00+08:00", status="closed")]
    trend = trend_frame(spark, normalize(frame(spark, values))).orderBy("as_of_date").collect()
    assert [(row.as_of_date, row["count"], row.change) for row in trend] == [
        ("2026-10-10", 1, None), ("2026-10-11", 1, 0), ("2026-10-12", 0, -1)]


def test_semantic_version_hash_ignores_json_key_order(spark):
    original = base(skills=["PySpark", "SQL"])
    reordered = dict(reversed(list(original.items())))
    normalized = normalize(frame(spark, [original, reordered]))
    assert normalized.select("record_hash").distinct().count() == 2
    assert normalized.select("version_hash").distinct().count() == 1


@pytest.mark.parametrize("mode", ["closed", "active_but_ineligible", "all_invalid"])
def test_empty_results_outputs_actionable_report(spark, profile, tmp_path, mode):
    inputs = tmp_path / "input"
    inputs.mkdir()
    row = base(status="closed") if mode == "closed" else base(employment_type="全职")
    if mode == "all_invalid":
        row["title"] = ""
    (inputs / "empty-result.jsonl").write_text(json.dumps(row) + "\n")
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    report = run_pipeline(spark, inputs, warehouse, artifacts, profile, tmp_path / "model")
    assert report["matches"] == []
    assert report["summary"]["closed_jobs"] == (1 if mode == "closed" else 0)
    assert report["model"]["status"] == ("trained" if mode == "active_but_ineligible" else "not_trained_empty_corpus")
    assert (report["model"]["path"] is None) == (mode != "active_but_ineligible")
    assert report["empty_state"]["action"]


def test_explicit_required_and_preferred_skills(spark, profile):
    values = [base(source_job_id="required-missing", required_skills=["Kafka"]),
              base(source_job_id="preferred-missing", required_skills=["SQL"], preferred_skills=["Kafka"])]
    eligible = eligible_jobs(normalize(frame(spark, values)), profile).collect()
    assert [row.source_job_id for row in eligible] == ["preferred-missing"]
    assert eligible[0].missing_preferred_skills == ["kafka"]


def test_unknown_conditions_do_not_claim_measured_failure(spark, profile):
    from src.pipeline import classify_jobs
    row = classify_jobs(normalize(frame(spark, [base(longitude=None, graduation_year_min=None, graduation_year_max=None)])), profile).first()
    assert "location_unknown_or_system" in row.exclusion_reasons
    assert "graduation_unknown" in row.exclusion_reasons
    assert "outside_radius" not in row.exclusion_reasons
    assert "graduation_year" not in row.exclusion_reasons


def test_snapshot_cache_configuration_recovery_and_failed_publish(spark, profile, tmp_path, monkeypatch):
    from src import pipeline
    inputs = tmp_path / "input"; inputs.mkdir()
    (inputs / "batch-a.jsonl").write_text(json.dumps(base()) + "\n")
    warehouse = tmp_path / "warehouse"; warehouse.mkdir()
    artifacts = tmp_path / "artifacts"; artifacts.mkdir()
    run = lambda target=profile: run_pipeline(spark, inputs, warehouse, artifacts, target)
    first = run()
    assert first["execution"]["cache_hit"] is False
    cached = run()
    assert cached["execution"]["cache_hit"] is True
    assert cached["execution"]["derived_rows_rebuilt"] == 0
    assert cached["snapshot_id"] == first["snapshot_id"]
    changed = dict(profile, skills=profile["skills"] + ["Kafka"])
    rebuilt = run(changed)
    assert rebuilt["execution"]["cache_hit"] is False
    assert rebuilt["snapshot_id"] != first["snapshot_id"]
    # A missing model metadata part invalidates the complete-file inventory.
    metadata = next((warehouse / "current/models/tfidf/metadata").glob("part-*"))
    metadata.unlink()
    recovered = run(changed)
    assert recovered["execution"]["cache_hit"] is False
    assert (warehouse / "current/models/tfidf/metadata").exists()
    committed = (artifacts / "report.json").read_bytes()
    target_before = (warehouse / "current").readlink()
    with monkeypatch.context() as patch:
        def fail(*args, **kwargs):
            raise RuntimeError("injected pre-publication failure")
        patch.setattr(pipeline, "build_snapshot", fail)
        with pytest.raises(RuntimeError, match="injected"):
            run(dict(changed, graduation_year=2028))
    assert (warehouse / "current").readlink() == target_before
    assert (artifacts / "report.json").read_bytes() == committed
    assert not list(warehouse.glob(".staging-derived-*"))
    (inputs / "batch-b.jsonl").write_text(json.dumps(base(snapshot_ts="2026-10-12T09:00:00+08:00", status="closed")) + "\n")
    updated = run(changed)
    assert updated["execution"]["cache_hit"] is False
    assert updated["summary"]["closed_jobs"] == 1
    (inputs / "batch-a.jsonl").write_text(json.dumps(base(title="immutable change")) + "\n")
    with pytest.raises(ValueError, match="Immutable batch"):
        run(changed)


def test_code_fingerprint_invalidates_cache(profile, monkeypatch, tmp_path):
    from src import pipeline
    fake_root = tmp_path; (fake_root / "src").mkdir()
    code = fake_root / "src/pipeline.py"; code.write_text("version-one")
    monkeypatch.setattr(pipeline, "ROOT", fake_root)
    manifest = {"batches": {"b": {"sha256": "a"}}}
    first = pipeline.build_fingerprint(manifest, profile)
    code.write_text("version-two")
    assert pipeline.build_fingerprint(manifest, profile) != first
