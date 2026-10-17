"""Actual Spark ETL/SQL/MLlib. Python only orchestrates files and bounded exports."""
import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import time
import xml.etree.ElementTree as ET
import uuid

from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer, IDF, Normalizer
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, Window, functions as F, types as T

ROOT = Path(__file__).resolve().parents[1]
PIPELINE_VERSION = "0.2.0"
ALIASES = {
    "pyspark": "spark", "spark sql": "spark", "apache spark": "spark",
    "apache kafka": "kafka", "apache flink": "flink", "hive sql": "hive",
    "postgresql": "postgres", "mysql数据库": "mysql", "machine learning": "机器学习",
    "rest api": "api", "restful": "api", "提示词": "prompt",
}
RAW_FIELDS = [
    "source", "source_job_id", "snapshot_ts", "title", "role", "company", "address", "city",
    "employment_type", "education", "graduation_year_min", "graduation_year_max", "salary_text",
    "description", "longitude", "latitude", "coordinate_system", "status", "source_url",
]
RAW_SCHEMA = T.StructType([T.StructField(name, T.StringType()) for name in RAW_FIELDS] + [
    T.StructField("skills", T.ArrayType(T.StringType())), T.StructField("required_skills", T.ArrayType(T.StringType())),
    T.StructField("preferred_skills", T.ArrayType(T.StringType())), T.StructField("synthetic", T.BooleanType()),
    T.StructField("_corrupt_record", T.StringType()),
])


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    temporary.replace(path)


def create_spark(master="local[2]"):
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", os.sys.executable)
    spark = (SparkSession.builder.master(master).appName("JobMarketLakehouse")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.default.parallelism", "2")
             .config("spark.driver.memory", "2g")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.session.timeZone", "Asia/Shanghai")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.sources.partitionOverwriteMode", "static")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def canonical_skills(column):
    entries = [item for pair in ALIASES.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    aliases = F.create_map(*entries)
    return F.array_sort(F.array_distinct(F.filter(
        F.transform(F.coalesce(column, F.array().cast("array<string>")),
                    lambda item: F.coalesce(aliases[F.lower(F.trim(item))], F.lower(F.trim(item)))),
        lambda item: item.isNotNull() & (F.length(item) > 0))))


def normalize(frame):
    """All row normalization/validation uses Spark expressions, including salary."""
    salary_pattern = r"^(\d+(?:\.\d+)?)(?:[-~至](\d+(?:\.\d+)?))?(k|千|万|元)?(?:/|每)?(天|日|月|年|小时|时)$"
    frame = (frame.withColumn("snapshot_time", F.expr("try_cast(snapshot_ts as timestamp)"))
             .withColumn("longitude_num", F.expr("try_cast(longitude as double)"))
             .withColumn("latitude_num", F.expr("try_cast(latitude as double)"))
             .withColumn("grad_min", F.expr("try_cast(graduation_year_min as int)"))
             .withColumn("grad_max", F.expr("try_cast(graduation_year_max as int)"))
             .withColumn("canonical_skills", canonical_skills(F.col("skills")))
             .withColumn("required_skills", canonical_skills(F.col("required_skills")))
             .withColumn("preferred_skills", canonical_skills(F.col("preferred_skills")))
             .withColumn("_salary", F.regexp_replace(F.lower(F.coalesce("salary_text", F.lit(""))), r"\s+", ""))
             .withColumn("_low", F.regexp_extract("_salary", salary_pattern, 1))
             .withColumn("_high", F.regexp_extract("_salary", salary_pattern, 2))
             .withColumn("salary_unit", F.regexp_extract("_salary", salary_pattern, 4))
             .withColumn("_mult", F.regexp_extract("_salary", salary_pattern, 3))
             .withColumn("_low_num", F.expr("try_cast(nullif(_low, '') as double)"))
             .withColumn("_high_num", F.coalesce(F.expr("try_cast(nullif(_high, '') as double)"), F.col("_low_num"))))
    multiplier = (F.when(F.col("_mult").isin("k", "千"), 1000.0)
                  .when(F.col("_mult") == "万", 10000.0).otherwise(1.0))
    period = (F.when(F.col("salary_unit").isin("天", "日"), 21.75)
              .when(F.col("salary_unit").isin("小时", "时"), 21.75 * 8)
              .when(F.col("salary_unit") == "年", 1 / 12).otherwise(1.0))
    frame = (frame.withColumn("salary_monthly_min", F.round(F.col("_low_num") * multiplier * period, 2))
             .withColumn("salary_monthly_max", F.round(F.col("_high_num") * multiplier * period, 2))
             .withColumn("salary_known", F.col("_low_num").isNotNull())
             .withColumn("job_id", F.substring(F.sha2(F.concat_ws("|", F.lower(F.trim("company")),
                                                                 F.lower(F.trim("title")), F.trim("address"),
                                                                 F.trim("city")), 256), 1, 24))
             .withColumn("snapshot_date", F.coalesce(F.to_date("snapshot_time").cast("string"), F.lit("unknown"))))
    frame = frame.withColumn("version_hash", F.sha2(F.to_json(F.struct(*[F.col(name) for name in RAW_FIELDS],
                                                                       F.col("canonical_skills"), F.col("required_skills"), F.col("preferred_skills"), F.col("synthetic"))), 256))
    required = ["source", "source_job_id", "title", "company", "address", "role"]
    missing = F.lit(False)
    for field in required:
        missing = missing | F.col(field).isNull() | (F.length(F.trim(field)) == 0)
    missing = missing | (F.col("source") == "unknown")
    bad_geo = ((F.col("longitude").isNotNull() & F.col("longitude_num").isNull()) |
               (F.col("latitude").isNotNull() & F.col("latitude_num").isNull()) |
               (~F.col("longitude_num").between(-180.0, 180.0)) |
               (~F.col("latitude_num").between(-90.0, 90.0)))
    bad_salary = ((F.col("_low_num").isNotNull()) &
                  ((F.col("_low_num") > F.col("_high_num")) | (F.col("_low_num") <= 0)))
    frame = frame.withColumn("quality_reason", F.concat_ws(";",
        F.when(F.col("_corrupt_record").isNotNull(), "corrupt_json"),
        F.when(missing, "missing_required_field"),
        F.when(F.col("snapshot_time").isNull(), "invalid_snapshot_time"),
        F.when(bad_geo, "invalid_coordinates"),
        F.when(bad_salary, "invalid_salary_range"),
        F.when(~F.coalesce(F.col("status").isin("active", "closed"), F.lit(False)), "invalid_status"),
        F.when(F.col("grad_min").isNotNull() & F.col("grad_max").isNotNull() &
               (F.col("grad_min") > F.col("grad_max")), "invalid_graduation_range")))
    return frame.drop("_salary", "_low", "_high", "_mult", "_low_num", "_high_num")


def with_distance(frame, location):
    # Same-system straight-line great-circle distance, not route/commute distance.
    dlat = F.radians(F.col("latitude_num") - F.lit(location["latitude"]))
    dlon = F.radians(F.col("longitude_num") - F.lit(location["longitude"]))
    hav = (F.sin(dlat / 2) ** 2 + F.cos(F.radians("latitude_num")) *
           F.cos(F.radians(F.lit(location["latitude"]))) * F.sin(dlon / 2) ** 2)
    valid = (F.col("coordinate_system") == location["coordinate_system"]) & F.col("longitude_num").isNotNull() & F.col("latitude_num").isNotNull()
    return frame.withColumn("distance_km", F.when(valid, 6371.0088 * 2 * F.asin(F.sqrt(F.least(F.lit(1.0), F.greatest(F.lit(0.0), hav))))))


def eligible_jobs(frame, profile):
    return classify_jobs(frame, profile).filter(F.size("exclusion_reasons") == 0)


def classify_jobs(frame, profile):
    frame = with_distance(frame, profile["location"])
    skill_profile = canonical_skills(F.array(*[F.lit(skill) for skill in profile["skills"]]))
    frame = (frame.withColumn("missing_required_skills", F.array_except("required_skills", skill_profile))
             .withColumn("missing_preferred_skills", F.array_except("preferred_skills", skill_profile)))
    reasons = [
        (~F.coalesce(F.col("status") == "active", F.lit(False)), "closed"),
        (~F.coalesce(F.col("employment_type") == profile["target_employment"], F.lit(False)), "employment_type"),
        (~F.coalesce(F.col("education").isin("不限", "大专", profile["education"]), F.lit(False)), "education"),
        (F.col("grad_min").isNull() | F.col("grad_max").isNull(), "graduation_unknown"),
        ((F.col("grad_min") > profile["graduation_year"]) | (F.col("grad_max") < profile["graduation_year"]), "graduation_year"),
        (~F.coalesce(F.col("role").isin(profile["target_roles"]), F.lit(False)), "target_role"),
        (F.col("distance_km").isNull(), "location_unknown_or_system"),
        (F.col("distance_km") > profile["location"]["radius_km"], "outside_radius"),
        (F.size("missing_required_skills") > 0, "required_skills_missing"),
    ]
    # Null fields fail their hard requirement; no implicit inference from free text.
    return frame.withColumn("exclusion_reasons", F.filter(F.array(*[
        F.when(F.coalesce(condition, F.lit(False)), F.lit(reason)) for condition, reason in reasons]), lambda x: x.isNotNull()))


def latest_versions(history):
    by_source = Window.partitionBy("source", "source_job_id").orderBy(
        F.col("snapshot_time").desc(), F.when(F.col("status") == "closed", 1).otherwise(0).desc(),
        F.col("record_hash").desc())
    source_latest = history.withColumn("_version", F.row_number().over(by_source)).filter("_version = 1").drop("_version")
    by_job = Window.partitionBy("job_id").orderBy(
        F.col("snapshot_time").desc(), F.when(F.col("status") == "closed", 1).otherwise(0).desc(),
        F.col("source").asc(), F.col("record_hash").desc())
    latest = source_latest.withColumn("_cross_source_rank", F.row_number().over(by_job)).filter("_cross_source_rank = 1").drop("_cross_source_rank")
    return source_latest, latest


def ingest(spark, input_dir, warehouse):
    manifest_path = warehouse / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {"batches": {}}
    new_batches = []
    for path in sorted(input_dir.glob("*.jsonl"), key=lambda item: (item.stem.startswith("backfill"), item.name)):
        batch_id = path.stem
        if not batch_id or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for ch in batch_id):
            raise ValueError(f"Unsupported batch filename: {path.name}")
        with path.open("rb") as batch_file:
            checksum = hashlib.file_digest(batch_file, "sha256").hexdigest()
        prior = manifest["batches"].get(batch_id)
        if prior:
            if prior["sha256"] != checksum:
                raise ValueError(f"Immutable batch {batch_id} changed. Use a new batch ID for corrections/backfills.")
            continue
        destination = warehouse / "ods" / f"batch_id={batch_id}"
        destination.parent.mkdir(parents=True, exist_ok=True)
        staging = warehouse / f".staging-{batch_id}"
        if staging.exists():
            shutil.rmtree(staging)
        raw = spark.read.text(str(path)).withColumnRenamed("value", "raw_line")
        raw = (raw.withColumn("parsed", F.from_json("raw_line", RAW_SCHEMA, {"mode": "PERMISSIVE"}))
               .select("raw_line", "parsed.*")
               .withColumn("record_hash", F.sha2("raw_line", 256))
               .withColumn("ingested_at", F.lit(utc_now()))
               .withColumn("snapshot_date", F.coalesce(F.to_date(F.expr("try_cast(snapshot_ts as timestamp)")).cast("string"), F.lit("unknown")))
               .withColumn("source", F.coalesce("source", F.lit("unknown"))))
        row_count = raw.count()
        raw.write.mode("overwrite").partitionBy("snapshot_date", "source").parquet(str(staging))
        # Complete partition replaces any orphan from a previously interrupted pre-manifest commit.
        if destination.exists():
            shutil.rmtree(destination)
        staging.replace(destination)
        manifest["batches"][batch_id] = {"sha256": checksum, "rows": row_count, "committed_at": utc_now()}
        write_json(manifest_path, manifest)
        new_batches.append(batch_id)
    if not manifest["batches"]:
        raise ValueError(f"No JSONL batches in {input_dir}")
    return manifest, new_batches


def save_parquet(frame, path, partitions):
    frame.write.mode("overwrite").partitionBy(*partitions).parquet(str(path))


def tokens(column, skills):
    # Explicit canonical skills handle Chinese segmentation; English description terms add context.
    words = F.filter(F.split(F.regexp_replace(F.lower(F.coalesce(column, F.lit(""))), r"[^a-z0-9+#]+", " "), r"\s+"), lambda word: F.length(word) > 1)
    return F.concat(skills, skills, skills, words)


def match_jobs(spark, active, eligible, profile, model_dir):
    if not active.take(1):
        ranked = (eligible.withColumn("score", F.lit(0.0))
                  .withColumn("matched_skills", F.array().cast("array<string>"))
                  .withColumn("missing_skills", F.array().cast("array<string>"))
                  .withColumn("rank", F.lit(None).cast("int")))
        return ranked, 0, 0
    corpus = active.withColumn("tokens", tokens(F.col("description"), F.col("canonical_skills")))
    pipeline = Pipeline(stages=[CountVectorizer(inputCol="tokens", outputCol="tf", vocabSize=2048, minDF=1.0),
                                IDF(inputCol="tf", outputCol="tfidf", minDocFreq=1),
                                Normalizer(inputCol="tfidf", outputCol="features", p=2.0)])
    model = pipeline.fit(corpus)
    model.write().overwrite().save(str(model_dir))
    profile_frame = spark.createDataFrame([(profile["description"], profile["skills"])], "description string, skills array<string>")
    profile_frame = profile_frame.withColumn("canonical_skills", canonical_skills(F.col("skills"))).withColumn(
        "tokens", tokens(F.col("description"), F.col("canonical_skills")))
    profile_features = model.transform(profile_frame).select(vector_to_array("features").alias("profile_vector"),
                                                               F.col("canonical_skills").alias("profile_skills"))
    ranked = model.transform(eligible.withColumn("tokens", tokens(F.col("description"), F.col("canonical_skills"))))
    ranked = ranked.crossJoin(F.broadcast(profile_features)).withColumn("job_vector", vector_to_array("features"))
    cosine = F.aggregate(F.zip_with("job_vector", "profile_vector", lambda a, b: a * b), F.lit(0.0), lambda acc, item: acc + item)
    ranked = (ranked.withColumn("score", F.round(F.greatest(F.lit(0.0), F.least(F.lit(1.0), cosine)), 4))
              .withColumn("matched_skills", F.array_intersect("canonical_skills", "profile_skills"))
              .withColumn("missing_skills", F.array_except("canonical_skills", "profile_skills"))
              .withColumn("rank", F.row_number().over(Window.orderBy(F.col("score").desc(), F.col("distance_km").asc(), F.col("job_id")))))
    return ranked, len(model.stages[0].vocabulary), corpus.count()


def trend_frame(spark, history):
    # Build as-of daily states: late-arriving snapshots update past dates by event time.
    days = (history.agg(F.min("snapshot_date").cast("date").alias("first_date"),
                        F.max("snapshot_date").cast("date").alias("last_date"))
            .select(F.explode(F.sequence("first_date", "last_date")).alias("as_of_date"))
            .withColumn("as_of_date", F.col("as_of_date").cast("string")))
    asof = history.crossJoin(days).filter(F.col("snapshot_date") <= F.col("as_of_date"))
    window = Window.partitionBy("as_of_date", "source", "source_job_id").orderBy(
        F.col("snapshot_time").desc(), F.when(F.col("status") == "closed", 1).otherwise(0).desc(), F.col("record_hash").desc())
    asof = asof.withColumn("_n", F.row_number().over(window)).filter("_n = 1").drop("_n")
    cross = Window.partitionBy("as_of_date", "job_id").orderBy(
        F.col("snapshot_time").desc(), F.when(F.col("status") == "closed", 1).otherwise(0).desc(), F.col("source").asc(), F.col("record_hash").desc())
    asof = asof.withColumn("_n", F.row_number().over(cross)).filter("_n = 1 AND status = 'active'")
    asof.select("as_of_date", F.explode("canonical_skills").alias("skill")).createOrReplaceTempView("daily_skills")
    days.createOrReplaceTempView("observed_days")
    history.select(F.explode("canonical_skills").alias("skill")).distinct().createOrReplaceTempView("observed_skills")
    return spark.sql("""
        WITH demand_raw AS (SELECT as_of_date, skill, COUNT(*) AS count FROM daily_skills GROUP BY as_of_date, skill),
        demand AS (
          SELECT d.as_of_date, s.skill, COALESCE(r.count, 0) AS count
          FROM observed_days d CROSS JOIN observed_skills s
          LEFT JOIN demand_raw r ON d.as_of_date = r.as_of_date AND s.skill = r.skill
        )
        SELECT *, count - LAG(count) OVER (PARTITION BY skill ORDER BY as_of_date) AS change
        FROM demand
    """)


MATCH_FIELDS = ["job_id", "title", "role", "company", "address", "city", "salary_text", "salary_monthly_min",
                "salary_monthly_max", "distance_km", "score", "matched_skills", "missing_skills", "source",
                "source_job_id", "description", "snapshot_date", "employment_type", "education", "grad_min",
                "grad_max", "canonical_skills", "synthetic", "rank", "coordinate_system", "source_url",
                "required_skills", "preferred_skills", "missing_required_skills", "missing_preferred_skills", "exclusion_reasons"]


def build_snapshot(spark, ods_root, warehouse, artifacts, profile, model_dir, manifest, new_batches):
    started = time.monotonic()
    ods = spark.read.parquet(str(ods_root / "ods")).cache()
    normalized = normalize(ods).cache()
    quarantine = normalized.filter(F.col("quality_reason") != "").cache()
    history = normalized.filter(F.col("quality_reason") == "").dropDuplicates(["version_hash"]).cache()
    save_parquet(quarantine, warehouse / "quarantine", ["batch_id"])
    save_parquet(history, warehouse / "dwd" / "job_history", ["snapshot_date", "source"])
    source_latest, latest = latest_versions(history)
    latest = latest.cache()
    active = latest.filter("status = 'active'").cache()
    save_parquet(latest, warehouse / "dwd" / "job_latest", ["status", "role"])
    active.createOrReplaceTempView("active_jobs")
    skill_demand = spark.sql("SELECT explode(canonical_skills) AS skill FROM active_jobs").groupBy("skill").count()
    salary = spark.sql("""
        SELECT role, COUNT(*) AS job_count, COUNT(salary_monthly_min) AS known_salary_count,
               ROUND(AVG((salary_monthly_min + salary_monthly_max)/2), 2) AS avg_monthly_salary,
               ROUND(MIN(salary_monthly_min), 2) AS min_monthly_salary,
               ROUND(MAX(salary_monthly_max), 2) AS max_monthly_salary
        FROM active_jobs GROUP BY role
    """)
    trend = trend_frame(spark, history).cache()
    as_of_date = history.agg(F.max("snapshot_date")).first()[0]
    save_parquet(skill_demand.withColumn("as_of_date", F.lit(as_of_date).cast("string")), warehouse / "dws" / "skill_demand", ["as_of_date"])
    save_parquet(salary.withColumn("as_of_date", F.lit(as_of_date).cast("string")), warehouse / "dws" / "salary_by_role", ["as_of_date"])
    save_parquet(trend, warehouse / "dws" / "skill_trend", ["as_of_date"])
    classified = classify_jobs(latest, profile).cache()
    exclusions = classified.select(F.explode("exclusion_reasons").alias("reason")).groupBy("reason").count()
    eligible = classified.filter(F.size("exclusion_reasons") == 0).cache()
    ranked, vocabulary_size, training_rows = match_jobs(spark, active, eligible, profile, model_dir)
    ranked = ranked.select(*MATCH_FIELDS).cache()
    save_parquet(ranked.withColumn("profile_id", F.lit(profile["profile_id"])), warehouse / "ads" / "job_matches", ["profile_id", "snapshot_date"])
    metrics = {
        "ods_rows": ods.count(), "history_rows": history.count(), "quarantine_rows": quarantine.count(),
        "source_latest_rows": source_latest.count(), "unique_jobs": latest.count(), "active_jobs": active.count(),
        "closed_jobs": latest.filter("status = 'closed'").count(), "eligible_jobs": eligible.count(),
        "batches": len(manifest["batches"]), "new_batches": len(new_batches),
        "duplicate_rows_removed": ods.count() - quarantine.count() - history.count(),
        "cross_source_duplicates_removed": source_latest.count() - latest.count(),
        "missing_location_jobs": active.filter("longitude_num IS NULL OR latitude_num IS NULL OR coordinate_system != 'GCJ02'").count(),
        "salary_unknown_jobs": active.filter("NOT salary_known").count(),
        "runtime_seconds": round(time.monotonic() - started, 2),
    }
    metrics.update(total_jobs=metrics["ods_rows"], input_jobs=metrics["ods_rows"],
                   clean_jobs=metrics["history_rows"], valid_jobs=metrics["history_rows"],
                   matched_jobs=metrics["eligible_jobs"], unique_companies=active.select("company").distinct().count(),
                   within_5km=with_distance(active, profile["location"]).filter(F.col("distance_km") <= profile["location"]["radius_km"]).count(),
                   median_salary=active.selectExpr("percentile_approx((salary_monthly_min + salary_monthly_max)/2, 0.5)").first()[0])
    rows = lambda frame, maximum: [row.asDict(recursive=True) for row in frame.limit(maximum).collect()]
    report = {
        "project": "招聘离线数仓与 AI 岗位匹配", "engine": f"Apache Spark {spark.version} / PySpark / MLlib",
        "data_source": f"本次读取 {len(manifest['batches'])} 批岗位快照。默认 data/input 样例为固定种子虚构数据；synthetic 字段区分样例与自有导入。",
        "generated_at": utc_now(), "summary": metrics,
        "version": PIPELINE_VERSION,
        "exclusion_summary": rows(exclusions.orderBy(F.col("count").desc(), "reason"), 20),
        "matches": rows(ranked.orderBy("rank").withColumn("distance_km", F.round("distance_km", 3)), 50),
        "skill_demand": rows(skill_demand.orderBy(F.col("count").desc(), "skill"), 30),
        "salary_by_role": rows(salary.orderBy("role"), 30),
        "skill_trend": rows(trend.orderBy("as_of_date", F.col("count").desc(), "skill"), 200),
        "quality": {
            "quarantined": metrics["quarantine_rows"],
            "reasons": rows(quarantine.groupBy("quality_reason").count().orderBy("quality_reason"), 30),
            "samples": rows(quarantine.select("batch_id", "source_job_id", "quality_reason", "raw_line"), 6),
            "unknown_locations_excluded": metrics["missing_location_jobs"],
            "salary_unknown": metrics["salary_unknown_jobs"],
        },
        "match_policy": {
            "hard_filters": ["最新快照仍有效", "实习", "本科可投", "2027届在岗位范围内", "目标数据开发类岗位", "GCJ02 坐标完整", "距翻身地铁站直线距离 ≤ 5 公里"],
            "location": profile["location"], "graduation_year": profile["graduation_year"], "profile_skills": profile["skills"],
            "scoring": "Spark MLlib CountVectorizer → IDF → L2 Normalizer → cosine，分值0至1；先硬过滤，后排序。分值不是录用概率。",
            "skill_requirement_boundary": "仅显式 required_skills 为技能硬要求；preferred_skills 用于解释，不阻止入选。skills 为未分级技能标签，不自动当作必须条件。没有从自由文本猜测要求。硬筛选通过不代表招聘资格得到认证。",
            "limits": "文本相似度不是录用概率；缺少经历深度和技能熟练度。月薪折算按21.75天/月、8小时/天、12个月/年，不代表实际月收入。直线距离不是通勤距离。",
        },
        "model": {"algorithm": "MLlib CountVectorizer + IDF + Normalizer", "path": "warehouse/current/models/tfidf" if training_rows else None,
                  "status": "trained" if training_rows else "not_trained_empty_corpus",
                  "vocabulary_size": vocabulary_size, "training_rows": training_rows,
                  "training_boundary": "当前最新有效岗位文本；本地示例采用无监督相似度，未做招聘结果监督验证"},
        "lineage": [
            {"layer": "ODS", "path": "warehouse/ods", "purpose": "不可变输入快照、原始行、批次哈希与来源", "partitions": ["batch_id", "snapshot_date", "source"]},
            {"layer": "DWD", "path": "warehouse/current/dwd", "purpose": "规范化历史与最新快照、技能别名、事件时间去重", "partitions": ["snapshot_date/source", "status/role"]},
            {"layer": "DWS", "path": "warehouse/current/dws", "purpose": "技能需求、薪资汇总、as-of每日趋势与LAG窗口", "partitions": ["as_of_date"]},
            {"layer": "ADS", "path": "warehouse/current/ads", "purpose": "硬过滤后的可解释岗位排名", "partitions": ["profile_id", "snapshot_date"]},
        ],
        "execution": {"master": spark.sparkContext.master, "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
                      "new_batch_ids": new_batches, "bounded_export_rows": 50,
                      "incremental_boundary": "ODS 按批次增量幂等；DWD/DWS/ADS 当前为可重建全量派生，非增量 merge 表格式"},
        "empty_state": None if metrics["eligible_jobs"] else {
            "reason": "没有最新有效岗位，未训练匹配模型" if not training_rows else "有效岗位均未满足实习、学历、届别、目标角色和5公里硬条件",
            "action": "导入新的授权岗位快照；核对岗位坐标体系、毕业年份和当前状态后重跑。硬条件不会自动放宽。",
        },
    }
    write_json(artifacts / "report.json", report)
    (artifacts / "spark-plan.txt").write_text(ranked._jdf.queryExecution().toString(), encoding="utf-8")
    # Partition-pruning evidence from a genuine Parquet scan, retained for review.
    persisted = spark.read.schema(history.schema).parquet(str(warehouse / "dwd" / "job_history"))
    sample_date = history.agg(F.max("snapshot_date")).first()[0]
    (artifacts / "partition-pruning-plan.txt").write_text(
        persisted.filter(F.col("snapshot_date") == sample_date)._jdf.queryExecution().toString(), encoding="utf-8")
    for frame in (ranked, eligible, classified, trend, active, latest, history, quarantine, normalized, ods):
        frame.unpersist()
    return report


def file_sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def build_fingerprint(manifest, profile):
    payload = {"batches": {name: row["sha256"] for name, row in manifest["batches"].items()},
               "profile": profile, "version": PIPELINE_VERSION,
               "code": {path.name: file_sha(path) for path in sorted((ROOT / "src").glob("*.py"))}}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def input_unchanged(input_dir, manifest):
    files = list(input_dir.glob("*.jsonl"))
    for path in files:
        prior = manifest["batches"].get(path.stem)
        if prior and file_sha(path) != prior["sha256"]:
            raise ValueError(f"Immutable batch {path.stem} changed. Use a new batch ID.")
        if not prior:
            return False
    # ODS is an accumulating registry: removing source files does not delete committed history.
    return True


def cached_snapshot(warehouse, fingerprint):
    current = warehouse / "current"
    try:
        metadata = json.loads((current / "cache.json").read_text())
        if metadata["fingerprint"] != fingerprint:
            return None
        for name, checksum in metadata["files"].items():
            path = current / name
            if not path.is_file() or file_sha(path) != checksum:
                return None
        return json.loads((current / "report.json").read_text())
    except (OSError, ValueError, KeyError):
        return None


def publish_links(warehouse, artifacts):
    artifacts.mkdir(parents=True, exist_ok=True)
    for name in ("report.json", "spark-plan.txt", "partition-pruning-plan.txt"):
        target = artifacts / name
        temporary = artifacts / ("." + name + ".link")
        temporary.unlink(missing_ok=True)
        temporary.symlink_to(os.path.relpath(warehouse / "current" / name, artifacts))
        temporary.replace(target)


def execution_result(report, artifacts, cache_hit, new_batches, elapsed):
    result = json.loads(json.dumps(report))
    result["execution"].update(cache_hit=cache_hit, derived_rows_rebuilt=0 if cache_hit else report["summary"]["history_rows"],
                               new_batch_ids=new_batches, runtime_seconds=round(elapsed, 3))
    result["summary"].update(new_batches=len(new_batches), runtime_seconds=round(elapsed, 3))
    write_json(artifacts / "last-run.json", {"snapshot_id": report["snapshot_id"], "generated_at": utc_now(),
                                            "execution": result["execution"], "summary": result["summary"]})
    return result


def run_pipeline(spark, input_dir, warehouse, artifacts, profile, model_dir=None):
    """Caller owns single-writer lock. ODS commit precedes atomic derived snapshot publication."""
    started = time.monotonic()
    manifest, new_batches = ingest(spark, input_dir, warehouse)
    fingerprint = build_fingerprint(manifest, profile)
    cached = cached_snapshot(warehouse, fingerprint)
    if cached is not None:
        publish_links(warehouse, artifacts)
        return execution_result(cached, artifacts, True, new_batches, time.monotonic() - started)
    snapshot_id = fingerprint[:16] + "-" + uuid.uuid4().hex[:8]
    staging = warehouse / (".staging-derived-" + snapshot_id)
    staging.mkdir(parents=True)
    try:
        report = build_snapshot(spark, warehouse, staging, staging, profile, staging / "models" / "tfidf", manifest, new_batches)
        report["snapshot_id"] = snapshot_id
        report["execution"].update(cache_hit=False, derived_rows_rebuilt=report["summary"]["history_rows"], fingerprint=fingerprint)
        write_json(staging / "report.json", report)
        inventory = {str(path.relative_to(staging)): file_sha(path) for path in sorted(staging.rglob("*")) if path.is_file()}
        write_json(staging / "cache.json", {"fingerprint": fingerprint, "files": inventory})
        snapshots = warehouse / "snapshots"
        snapshots.mkdir(exist_ok=True)
        staging.replace(snapshots / snapshot_id)
        temporary = warehouse / ".current-next"
        temporary.unlink(missing_ok=True)
        temporary.symlink_to(Path("snapshots") / snapshot_id, target_is_directory=True)
        temporary.replace(warehouse / "current")
        publish_links(warehouse, artifacts)
        return execution_result(report, artifacts, False, new_batches, time.monotonic() - started)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def verify(spark, run_once, first, warehouse, artifacts):
    before = first["summary"]
    repeated = run_once()
    after = repeated["summary"]
    stable_keys = ["ods_rows", "history_rows", "quarantine_rows", "unique_jobs", "active_jobs", "closed_jobs", "eligible_jobs"]
    checks = {"real_spark_engine": spark.version == "4.0.1",
              "rerun_has_zero_new_batches": after["new_batches"] == 0,
              "idempotent_counts": all(before[key] == after[key] for key in stable_keys),
              "idempotent_ranking": [(x["job_id"], x["score"]) for x in first["matches"]] == [(x["job_id"], x["score"]) for x in repeated["matches"]],
              "four_layers_written": bool(list((warehouse / "ods").rglob("_SUCCESS"))) and all(list((warehouse / "current" / layer).rglob("_SUCCESS")) for layer in ("dwd", "dws", "ads")),
              "model_saved_or_explicitly_empty": (warehouse / "current/models/tfidf/metadata").exists() if repeated["model"]["path"] else repeated["model"]["status"] == "not_trained_empty_corpus",
              "unchanged_run_is_cache_hit": repeated["execution"]["cache_hit"],
              "cache_hit_rebuilds_zero_rows": repeated["execution"]["derived_rows_rebuilt"] == 0,
              "quality_counts_reconcile": after["ods_rows"] == after["history_rows"] + after["quarantine_rows"] + after["duplicate_rows_removed"],
              "all_matches_inside_radius": all(x["distance_km"] <= 5.0 for x in repeated["matches"]),
              "all_matches_are_internships": all(x["employment_type"] == "实习" for x in repeated["matches"])}
    manifest = json.loads((warehouse / "manifest.json").read_text())
    demo_fixture = {"batch-20261010", "batch-20261012", "backfill-20261009"}.issubset(manifest["batches"])
    if demo_fixture:
        latest = spark.read.parquet(str(warehouse / "current/dwd/job_latest"))
        fixture = latest.filter("source_job_id = 'S-99000'").select("status").first()
        checks["sample_quarantine_present"] = after["quarantine_rows"] >= 6
        checks["latest_closed_snapshot_not_resurrected"] = fixture is not None and fixture[0] == "closed"
        checks["backfill_retained_without_overriding_latest"] = latest.filter("snapshot_date = '2026-10-09'").count() == 0
    result = {"verified_at": utc_now(), "engine": spark.version, "master": spark.sparkContext.master,
              "checks": checks, "passed": all(checks.values()),
              "first_run": before, "repeated_run": after,
              "scope": "真实本机 Spark local[2]，输入快照功能验收；默认数据为确定性合成样例，不等于分布式集群性能或真实招聘推荐准确率"}
    junit = artifacts / "test-results.xml"
    if junit.exists():
        suites = ET.parse(junit).getroot().iter("testsuite")
        totals = {key: 0 for key in ("tests", "failures", "errors", "skipped")}
        for suite in suites:
            for key in totals:
                totals[key] += int(suite.attrib.get(key, 0))
        result["tests"] = {**totals, "passed": totals["tests"] - totals["failures"] - totals["errors"] - totals["skipped"],
                           "evidence": "artifacts/test-results.xml"}
    write_json(artifacts / "verification.json", result)
    if not result["passed"]:
        raise AssertionError(json.dumps(checks, ensure_ascii=False))
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=ROOT / "data" / "input")
    parser.add_argument("--profile", type=Path, default=ROOT / "config" / "profile.json")
    parser.add_argument("--warehouse", type=Path, default=ROOT / "warehouse")
    parser.add_argument("--artifacts", type=Path, default=ROOT / "artifacts")
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    args.warehouse.mkdir(parents=True, exist_ok=True)
    args.artifacts.mkdir(parents=True, exist_ok=True)
    profile = json.loads(args.profile.read_text(encoding="utf-8"))
    with (ROOT / ".run.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # A repeat CLI run avoids starting the JVM altogether. Verification still starts Spark.
        manifest_file = args.warehouse / "manifest.json"
        if not args.verify and manifest_file.exists():
            manifest = json.loads(manifest_file.read_text())
            if input_unchanged(args.input, manifest):
                cached = cached_snapshot(args.warehouse, build_fingerprint(manifest, profile))
                if cached is not None:
                    publish_links(args.warehouse, args.artifacts)
                    report = execution_result(cached, args.artifacts, True, [], 0.0)
                    print(json.dumps(report["execution"], ensure_ascii=False))
                    return
        spark = create_spark()
        try:
            run_once = lambda: run_pipeline(spark, args.input, args.warehouse, args.artifacts, profile, ROOT / "models" / "tfidf")
            report = run_once()
            if args.verify:
                verification = verify(spark, run_once, report, args.warehouse, args.artifacts)
                print(json.dumps(verification, ensure_ascii=False))
            else:
                print(json.dumps(report["summary"], ensure_ascii=False))
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
