"""Build the severity training set from Loghub, a published collection of real system logs.

Source: Zhu et al., "Loghub: A Large Collection of System Log Datasets for AI-driven
Log Analytics", ISSRE 2023. Files from Zenodo, DOI 10.5281/zenodo.8196385.

    BGL        4.7M lines from the BlueGene/L supercomputer at LLNL. System administrators
               tagged 348k lines as alerts (Oliner and Stearley, DSN 2007).
    Hadoop     MapReduce jobs with injected machine-down, network and disk-full failures.
    OpenStack  Nova logs from an OpenStack cloud.
    Zookeeper  Logs from a ZooKeeper ensemble.
    Apache     Apache HTTP server error log.

Severity labels come only from fields that are already in the published logs:

    High    BGL lines that administrators tagged as alerts; FATAL or CRITICAL lines elsewhere
    Medium  ERROR, SEVERE or FAILURE lines (BGL FATAL lines without an alert tag count here:
            Oliner and Stearley show the BGL level field overstates severity)
    Low     WARN and WARNING lines

INFO, DEBUG and notice lines are left out, because the stream only grades errors.

The datasets repeat the same message thousands of times, so the job keeps at most
MAX_PER_TEMPLATE copies of each (source, template, severity). The fold column hashes the
template's first words, so evaluation can hold out whole message families the model has
never seen.

    python -m spark.prepare_dataset        # downloads ~70 MB once, writes data/severity_dataset
"""

import json
import shutil
import tarfile
import urllib.request
import zipfile
from pathlib import Path

from pyspark.sql import DataFrame, Window, functions as F

from logiq import config
from spark.common import local_uri, spark_session

ZENODO = "https://zenodo.org/api/records/8196385/files/{name}/content"
ARCHIVES = ["BGL.zip", "Hadoop.zip", "OpenStack.tar.gz", "Zookeeper.tar.gz", "Apache.tar.gz"]
MAX_PER_TEMPLATE = 300
FOLDS = 5
FAMILY_WORDS = 6

# Same masking as logiq.textnorm.template, in Spark SQL so it runs on the executors.
VARIABLE_TOKEN = r"[^\s=:,()\[\]'\"]*\d[^\s=:,()\[\]'\"]*"


def download(raw_dir: Path) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    for name in ARCHIVES:
        target = raw_dir / name.split(".")[0]
        if target.exists():
            continue
        archive = raw_dir / name
        print(f"Downloading {name} from Zenodo ...", flush=True)
        with urllib.request.urlopen(ZENODO.format(name=name)) as resp, open(archive, "wb") as f:
            shutil.copyfileobj(resp, f)
        if name.endswith(".zip"):
            with zipfile.ZipFile(archive) as z:
                z.extractall(target)
        else:
            with tarfile.open(archive) as t:
                t.extractall(target, filter="data")
        archive.unlink()


def _parse(spark, path: str, pattern: str, fields: dict[str, int]) -> DataFrame:
    lines = spark.read.text(local_uri(path))
    cols = [F.regexp_extract("value", pattern, idx).alias(name) for name, idx in fields.items()]
    return lines.select(*cols).filter(F.col("message") != "")


def parse_all(spark, raw: Path) -> DataFrame:
    bgl = _parse(spark, f"{raw}/BGL/BGL.log",
                 r"^(\S+) \S+ \S+ \S+ \S+ \S+ \S+ \S+ (\S+) (.*)$",
                 {"alert": 1, "level": 2, "message": 3}).withColumn("source", F.lit("BGL"))
    others = [
        ("Hadoop", f"{raw}/Hadoop/*/*.log",
         r"^\d{4}-\d\d-\d\d [\d:,]+ ([A-Z]+) \[[^\]]*\] \S+: (.*)$"),
        ("OpenStack", f"{raw}/OpenStack/*.log",
         r"^\S+ \S+ \S+ \d+ ([A-Z]+) \S+ (?:\[[^\]]*\] )?(.*)$"),
        ("Zookeeper", f"{raw}/Zookeeper/Zookeeper.log",
         r"^\S+ \S+ - ([A-Z]+)\s+\[[^\]]*\] - (.*)$"),
        ("Apache", f"{raw}/Apache/Apache.log",
         r"^\[[^\]]+\] \[(\w+)\] (.*)$"),
    ]
    df = bgl
    for source, path, pattern in others:
        part = (_parse(spark, path, pattern, {"level": 1, "message": 2})
                .withColumn("alert", F.lit("-")).withColumn("source", F.lit(source)))
        df = df.unionByName(part)
    return df


def label(df: DataFrame) -> DataFrame:
    level = F.upper("level")
    severity = (
        F.when((F.col("source") == "BGL") & (F.col("alert") != "-"), "High")
        .when((F.col("source") != "BGL") & level.isin("FATAL", "CRITICAL", "CRIT", "EMERG", "ALERT"), "High")
        .when(level.isin("FATAL", "FAILURE", "SEVERE", "ERROR"), "Medium")
        .when(level.isin("WARN", "WARNING"), "Low")
    )
    return df.withColumn("severity", severity).filter(F.col("severity").isNotNull())


def _family(col: str):
    """First words of a template. Variants that differ only in hex ids share a family,
    so they always land in the same fold."""
    words = F.split(F.trim(F.lower(F.regexp_replace(col, "[^a-zA-Z]+", " "))), " ")
    return F.concat_ws(" ", F.slice(words, 1, FAMILY_WORDS))


def main() -> None:
    raw = config.ROOT / "data" / "loghub"
    download(raw)
    spark = spark_session("logiq-prepare")

    labelled = label(parse_all(spark, raw)).withColumn(
        "template", F.trim(F.regexp_replace(F.regexp_replace("message", VARIABLE_TOKEN, "<*>"), r"\s+", " ")))
    labelled.cache()

    # Cap repeats of one template, so a handful of very chatty messages cannot dominate.
    per_template = Window.partitionBy("source", "template", "severity").orderBy(F.xxhash64("message"))
    dataset = (
        labelled.withColumn("n", F.row_number().over(per_template))
        .filter(F.col("n") <= MAX_PER_TEMPLATE)
        .withColumn("fold", F.pmod(F.xxhash64(_family("template")), F.lit(FOLDS)).cast("int"))
        .select("message", "severity", "source", "template", "fold")
    )
    dataset.write.mode("overwrite").parquet(local_uri(config.TRAINING_DATA))

    dataset = spark.read.parquet(local_uri(config.TRAINING_DATA))
    raw_counts = labelled.groupBy("source", "severity").count().collect()
    kept_counts = dataset.groupBy("source", "severity").count().collect()
    stats = {
        "citation": "Zhu et al., Loghub, ISSRE 2023, doi:10.5281/zenodo.8196385",
        "labelled_lines": labelled.count(),
        "rows_after_cap": dataset.count(),
        "templates": dataset.select("source", "template").distinct().count(),
        "max_per_template": MAX_PER_TEMPLATE,
        "labelled_by_source": {f"{r['source']}/{r['severity']}": r["count"] for r in sorted(raw_counts)},
        "kept_by_source": {f"{r['source']}/{r['severity']}": r["count"] for r in sorted(kept_counts)},
    }
    (config.ROOT / "data" / "severity_dataset_stats.json").write_text(json.dumps(stats, indent=2))
    print(json.dumps(stats, indent=2))
    labelled.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
