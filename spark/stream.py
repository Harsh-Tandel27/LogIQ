"""LogIQ speed and archive layers (Spark Structured Streaming over Kafka).

Two streaming queries run side by side:

1. archive  - every raw log from both Kafka topics goes to HDFS as JSON,
              partitioned by date (cold storage for the batch layer).
2. alerts   - live logs only, every few seconds:
              a. Bloom filter drops healthy logs (module 4, stream mining)
              b. MapReduce collapses identical error templates (module 2)
              c. TF-IDF + cosine similarity merges near-duplicate templates,
                 and matches them against alerts that are already open (module 5)
              d. a classifier trained on real Loghub logs grades each alert
                 High / Medium / Low, never below the floor set by its log level (module 5)
              e. alerts are upserted into MongoDB (module 3)

    python -m spark.stream
"""

import time
import uuid
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne
from pyspark.ml import PipelineModel
from pyspark.ml.feature import Normalizer
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from logiq import bloom, config
from logiq.textnorm import template
from spark.common import local_uri, spark_session

LOG_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("timestamp", StringType()),
    StructField("service", StringType()),
    StructField("host", StringType()),
    StructField("level", StringType()),
    StructField("message", StringType()),
    StructField("trace_id", StringType()),
])

ERROR_LEVELS = ["ERROR", "FATAL", "CRITICAL"]
LEVEL_RANK = {"ERROR": 0, "CRITICAL": 1, "FATAL": 1}
SEVERITY_RANK = {"Low": 0, "Medium": 1, "High": 2}
# The training labels map FATAL/CRITICAL to High and ERROR to Medium (spark/prepare_dataset.py),
# so a log's own level sets the lowest severity it can get. The model can only raise it.
LEVEL_FLOOR = {"ERROR": "Medium", "CRITICAL": "High", "FATAL": "High"}
MAX_VARIANTS = 8


def _utc(ts: datetime) -> datetime:
    # PySpark returns naive datetimes in the machine's local time zone.
    return ts.astimezone(timezone.utc)


def read_kafka(spark, topics: str) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", topics)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 100_000)
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.select(F.col("topic"), F.from_json(F.col("value").cast("string"), LOG_SCHEMA).alias("log"))
        .select("topic", "log.*")
        .withColumn("ts", F.to_timestamp("timestamp"))
    )


def start_archive(spark):
    logs = read_kafka(spark, f"{config.KAFKA_TOPIC},{config.KAFKA_BACKFILL_TOPIC}")
    return (
        logs.withColumn("date", F.to_date("ts"))
        .drop("ts")
        .writeStream.format("json")
        .option("path", config.RAW_LOG_PATH)
        .option("checkpointLocation", local_uri(f"{config.CHECKPOINT_DIR}/archive"))
        .partitionBy("date")
        .trigger(processingTime="30 seconds")
        .queryName("archive")
        .start()
    )


class AlertBatcher:
    """foreachBatch handler for the alerts query. Runs on the driver."""

    def __init__(self, spark):
        self.spark = spark
        self.model = PipelineModel.load(local_uri(config.MODEL_PATH))
        self.normalizer = Normalizer(inputCol="tfidf", outputCol="unit", p=2.0)
        healthy = spark.sparkContext.broadcast(bloom.build_healthy_filter())
        self.is_healthy = F.udf(lambda level, msg: bloom.is_healthy(healthy.value, level or "", msg or ""), BooleanType())
        self.mongo = MongoClient(config.MONGO_URI, tz_aware=True)[config.MONGO_DB]

    # ---- helpers -------------------------------------------------------

    def _open_alerts(self) -> list[dict]:
        since = datetime.now(timezone.utc).timestamp() - config.ALERT_WINDOW_SEC
        return list(self.mongo.alerts.find(
            {"status": "open", "last_seen": {"$gte": datetime.fromtimestamp(since, timezone.utc)}},
            {"service": 1, "template": 1, "severity": 1},
        ))

    def _vectorize(self, rows: list[tuple[str, str]]) -> dict:
        """key -> (unit TF-IDF vector, predicted severity, confidence) for (key, text) rows."""
        df = self.spark.createDataFrame(rows, ["key", "message"]).withColumn("severity", F.lit("Low"))
        scored = self.normalizer.transform(self.model.transform(df))
        out = {}
        for r in scored.select("key", "unit", "predicted_severity", "probability").collect():
            out[r["key"]] = (r["unit"], r["predicted_severity"], float(max(r["probability"])))
        return out

    # ---- the batch -----------------------------------------------------

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        started = time.time()
        batch = batch.withColumn("healthy", self.is_healthy("level", "message")).cache()
        received = batch.count()
        if received == 0:
            batch.unpersist()
            return
        dropped = batch.filter("healthy").count()
        errors = batch.filter(~F.col("healthy") & F.col("level").isin(ERROR_LEVELS))

        # MapReduce: ((service, template), stats) -> reduceByKey.
        def merge(a, b):
            return (a[0] + b[0], a[1] if LEVEL_RANK.get(a[1], 0) >= LEVEL_RANK.get(b[1], 0) else b[1],
                    a[2], min(a[3], b[3]), max(a[4], b[4]))

        groups = (
            errors.select("service", "level", "message", "ts").rdd
            .map(lambda r: ((r.service, template(r.message)), (1, r.level, r.message, r.ts, r.ts)))
            .reduceByKey(merge)
            .collect()
        )
        error_count = sum(g[1][0] for g in groups)
        created, updated = self._cluster_and_store(groups) if groups else (0, 0)
        batch.unpersist()

        stats = {"received": received, "bloom_dropped": dropped, "errors": error_count,
                 "distinct_templates": len(groups), "alerts_created": created, "alerts_updated": updated}
        self.mongo.pipeline_stats.insert_one({
            "batch_id": batch_id, "at": datetime.now(timezone.utc),
            "duration_ms": int((time.time() - started) * 1000), **stats,
        })
        self.mongo.pipeline_totals.update_one({"_id": "all"}, {"$inc": stats}, upsert=True)
        print(f"[batch {batch_id}] received={received} bloom_dropped={dropped} errors={error_count} "
              f"templates={len(groups)} alerts +{created} ~{updated} ({int((time.time() - started) * 1000)}ms)",
              flush=True)

    def _cluster_and_store(self, groups) -> tuple[int, int]:
        open_alerts = self._open_alerts()
        # Vectorize templates, not raw samples, so ids and paths do not split clusters.
        rows = [(f"alert:{a['_id']}", a["template"]) for a in open_alerts]
        rows += [(f"group:{i}", g[0][1]) for i, g in enumerate(groups)]
        vectors = self._vectorize(rows)

        # Leaders are cluster representatives. Open alerts lead first, so new
        # errors join an existing alert instead of opening a duplicate.
        leaders = [{"alert": a, "service": a["service"], "unit": vectors[f"alert:{a['_id']}"][0], "members": []}
                   for a in open_alerts]
        order = sorted(range(len(groups)), key=lambda i: -groups[i][1][0])
        for i in order:
            (service, tpl), (count, level, sample, first, last) = groups[i]
            unit, severity, confidence = vectors[f"group:{i}"]
            floor = LEVEL_FLOOR.get(level, "Low")
            if SEVERITY_RANK[floor] > SEVERITY_RANK[severity]:
                severity = floor
            best, best_sim = None, 0.0
            for leader in leaders:
                if leader["service"] != service:
                    continue
                sim = float(unit.dot(leader["unit"]))
                if sim > best_sim:
                    best, best_sim = leader, sim
            member = {"template": tpl, "count": count, "level": level, "sample": sample, "first": _utc(first),
                      "last": _utc(last), "severity": severity, "confidence": confidence, "similarity": best_sim}
            if best is not None and best_sim >= config.SIMILARITY_THRESHOLD:
                best["members"].append(member)
            else:
                leaders.append({"alert": None, "service": service, "unit": unit, "members": [member]})

        ops, created, updated = [], 0, 0
        for leader in leaders:
            members = leader["members"]
            if not members:
                continue
            count = sum(m["count"] for m in members)
            first = min(m["first"] for m in members)
            last = max(m["last"] for m in members)
            variants = [m["template"] for m in members][:MAX_VARIANTS]
            severity = max((m["severity"] for m in members), key=SEVERITY_RANK.get)
            if leader["alert"] is None:
                head = max(members, key=lambda m: m["count"])
                ops.append(UpdateOne({"_id": f"ALT-{uuid.uuid4().hex[:8]}"}, {"$setOnInsert": {
                    "service": leader["service"], "level": head["level"], "message": head["sample"],
                    "template": head["template"], "variants": variants, "count": count,
                    "severity": severity, "severity_confidence": round(head["confidence"], 3),
                    "first_seen": first, "last_seen": last, "status": "open", "incident_id": None,
                    "created_at": datetime.now(timezone.utc),
                }}, upsert=True))
                created += 1
            else:
                alert = leader["alert"]
                if SEVERITY_RANK.get(alert.get("severity"), 0) > SEVERITY_RANK[severity]:
                    severity = alert["severity"]
                ops.append(UpdateOne({"_id": alert["_id"]}, {
                    "$inc": {"count": count},
                    "$max": {"last_seen": last},
                    "$min": {"first_seen": first},
                    "$set": {"severity": severity},
                    "$addToSet": {"variants": {"$each": variants}},
                }))
                updated += 1
        if ops:
            self.mongo.alerts.bulk_write(ops, ordered=False)
        return created, updated


def start_alerts(spark):
    logs = read_kafka(spark, config.KAFKA_TOPIC)
    return (
        logs.writeStream.foreachBatch(AlertBatcher(spark))
        .option("checkpointLocation", local_uri(f"{config.CHECKPOINT_DIR}/alerts"))
        .trigger(processingTime="3 seconds")
        .queryName("alerts")
        .start()
    )


def main() -> None:
    spark = spark_session("logiq-stream", kafka=True)
    start_archive(spark)
    start_alerts(spark)
    print("[stream] archive -> HDFS and alerts -> MongoDB running. Ctrl+C to stop.", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
