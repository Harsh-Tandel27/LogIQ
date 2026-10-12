"""LogIQ batch layer: MapReduce over the raw logs archived in HDFS (syllabus module 2).

Reads the JSON logs the stream job archived, runs three map/reduceByKey jobs and
stores the results in MongoDB for the dashboard's history charts:

    errors per (hour, service)   -> error_stats_hourly
    errors per service           -> error_stats_service
    logs per (day, level)        -> level_stats_daily
    most frequent error templates -> top_templates

    python -m spark.batch_mapreduce            # last 7 days
    python -m spark.batch_mapreduce --days 30
"""

import argparse
import time
from datetime import datetime, timedelta, timezone
from operator import add

from pymongo import MongoClient

from logiq import config
from logiq.textnorm import template
from spark.common import spark_session
from spark.stream import ERROR_LEVELS, LOG_SCHEMA


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()

    started = time.time()
    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    spark = spark_session("logiq-batch")
    logs = (
        spark.read.schema(LOG_SCHEMA).json(config.RAW_LOG_PATH)
        .filter(f"date >= '{since.date().isoformat()}'")
        .select("timestamp", "service", "level", "message")
        .rdd
        .map(lambda r: (_parse_ts(r.timestamp), r.service, r.level, r.message))
        .filter(lambda r: r[0] >= since)
        .cache()
    )
    errors = logs.filter(lambda r: r[2] in ERROR_LEVELS).cache()

    # Map each error to ((hour, service), 1), then reduce by key.
    hourly = (
        errors.map(lambda r: ((r[0].replace(minute=0, second=0, microsecond=0), r[1]), 1))
        .reduceByKey(add)
        .collect()
    )
    by_service = errors.map(lambda r: (r[1], 1)).reduceByKey(add).collect()
    levels = (
        logs.map(lambda r: ((r[0].replace(hour=0, minute=0, second=0, microsecond=0), r[2]), 1))
        .reduceByKey(add)
        .collect()
    )
    top = (
        errors.map(lambda r: ((r[1], template(r[3])), 1))
        .reduceByKey(add)
        .takeOrdered(10, key=lambda kv: -kv[1])
    )
    scanned = logs.count()

    db = MongoClient(config.MONGO_URI, tz_aware=True)[config.MONGO_DB]
    replacements = {
        "error_stats_hourly": [{"hour": h, "service": s, "errors": n} for (h, s), n in hourly],
        "error_stats_service": [{"service": s, "errors": n} for s, n in by_service],
        "level_stats_daily": [{"day": d, "level": lvl, "count": n} for (d, lvl), n in levels],
        "top_templates": [{"service": s, "template": t, "count": n} for (s, t), n in top],
    }
    for name, docs in replacements.items():
        db[name].delete_many({})
        if docs:
            db[name].insert_many(docs)
    duration = round(time.time() - started, 1)
    db.batch_runs.insert_one({"ran_at": datetime.now(timezone.utc), "days": args.days,
                              "logs_scanned": scanned, "duration_sec": duration})
    print(f"Scanned {scanned:,} logs from {config.RAW_LOG_PATH} in {duration}s")
    print(f"  {len(hourly)} (hour, service) buckets, {len(by_service)} services, {len(top)} top templates")
    spark.stop()


if __name__ == "__main__":
    main()
