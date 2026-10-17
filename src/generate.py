"""Deterministic, fictional snapshots. No site scraping or network requests."""
import argparse
import json
import random
from pathlib import Path

SKILL_POOLS = {
    "大数据开发": ["Python", "SQL", "Java", "Hadoop", "Hive", "Spark", "Kafka", "Flink", "Linux"],
    "数据开发": ["Python", "SQL", "MySQL", "PySpark", "Hive", "Airflow", "Linux", "Git"],
    "数据工程": ["Python", "SQL", "Spark SQL", "Kafka", "Docker", "ETL", "RAG"],
    "数据分析": ["Python", "SQL", "Pandas", "Excel", "机器学习"],
    "前端开发": ["JavaScript", "React", "TypeScript", "CSS"],
}


def make_row(index, day, rng):
    role = list(SKILL_POOLS)[index % 5]
    skills = rng.sample(SKILL_POOLS[role], min(5, len(SKILL_POOLS[role])))
    source = ["synthetic-campus-a", "synthetic-open-b", "synthetic-referral-c"][index % 3]
    lon = 113.893232 + rng.uniform(-0.065, 0.065)
    lat = 22.559690 + rng.uniform(-0.05, 0.05)
    salary = ["200-300元/天", "6-9K/月", "15-24万/年", "30-45元/小时", "面议"][index % 5]
    return {
        "source": source, "source_job_id": f"S-{index:05d}",
        "snapshot_ts": f"2026-10-{day:02d}T09:00:00+08:00",
        "title": f"{role}{'实习生' if index % 7 else '工程师'}",
        "role": role, "company": f"虚构·宝安数据实验室{index:04d}",
        "address": f"虚构办公点{index:04d}（仅演示）", "city": "深圳",
        "employment_type": "实习" if index % 7 else "全职",
        "education": "本科" if index % 11 else "硕士",
        "graduation_year_min": 2026 if index % 13 else 2028,
        "graduation_year_max": 2027 if index % 17 else 2026,
        "salary_text": salary, "skills": skills,
        "required_skills": [skill for skill in skills if skill in ("Python", "SQL")],
        "preferred_skills": [skill for skill in skills if skill not in ("Python", "SQL")],
        "description": f"【完全虚构数据】参与{role}，负责 ETL 数据质量与数据建模。要求 {' '.join(skills)}，完成 SQL 开发与 pipeline 维护。",
        "longitude": lon if index % 19 else None, "latitude": lat if index % 19 else None,
        "coordinate_system": "GCJ02", "status": "active", "synthetic": True,
        "source_url": None,
    }


def generate(destination: Path, count=600, seed=20261015):
    destination.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    first = [make_row(i, 10, rng) for i in range(count)]
    # Stable fixtures prove source updates, closures, cross-source dedup and safety filters.
    fixture = make_row(99000, 10, rng)
    fixture.update(title="大数据开发实习生", role="大数据开发", education="本科",
                   employment_type="实习", graduation_year_min=2026, graduation_year_max=2027,
                   longitude=113.894, latitude=22.560, salary_text="250-350元/天",
                   skills=["Python", "SQL", "Java", "Spark", "Kafka", "Linux"])
    first.append(fixture)
    first += [dict(first[5]), dict(first[9])]  # Identical duplicate rows.
    second = []
    for row in first[:count // 3]:
        update = dict(row, snapshot_ts="2026-10-12T09:00:00+08:00")
        if int(row["source_job_id"].split("-")[1]) % 9 == 0:
            update["status"] = "closed"
        update["skills"] = list(dict.fromkeys(row["skills"] + ["Spark"]))
        second.append(update)
    second.append(dict(fixture, snapshot_ts="2026-10-12T12:00:00+08:00", status="closed"))
    for row in first[20:32]:
        second.append(dict(row, source="synthetic-open-b", source_job_id=f"MIRROR-{row['source_job_id']}",
                           snapshot_ts="2026-10-12T10:00:00+08:00"))
    # Old snapshots arrive last: ingest time must never override event-time latest state.
    backfill = [dict(row, snapshot_ts="2026-10-09T09:00:00+08:00", salary_text="180-250元/天")
                for row in first[1:31]]
    dirty = [dict(first[1], source_job_id="BAD-TITLE", title=""),
             dict(first[2], source_job_id="BAD-TS", snapshot_ts="bad-time"),
             dict(first[3], source_job_id="BAD-GEO", latitude=220),
             dict(first[4], source_job_id="BAD-SALARY", salary_text="300-200元/天"),
             dict(first[5], source_job_id="BAD-STATUS", status="unknown")]
    batches = {"batch-20261010": first, "batch-20261012": second,
               "backfill-20261009": backfill + dirty}
    for name, rows in batches.items():
        path = destination / f"{name}.jsonl"
        path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")
    # A corrupt JSON record travels through the same ODS/quarantine path.
    with (destination / "backfill-20261009.jsonl").open("a", encoding="utf-8") as output:
        output.write('{"broken":\n')
    return {name: len(rows) for name, rows in batches.items()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("data/input"))
    parser.add_argument("--count", type=int, default=600)
    args = parser.parse_args()
    print(json.dumps(generate(args.output, args.count), ensure_ascii=False))
