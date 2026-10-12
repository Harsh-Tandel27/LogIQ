"""SparkSession factory shared by the LogIQ Spark jobs."""

import os
import shutil
import sys

from pyspark.sql import SparkSession

from logiq import config

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"


def _ship_logiq_package() -> str:
    """Zip the shared `logiq` package so Python workers can import it."""
    build_dir = config.ROOT / "build"
    build_dir.mkdir(exist_ok=True)
    return shutil.make_archive(str(build_dir / "logiq"), "zip", config.ROOT, "logiq")


def local_uri(path: str) -> str:
    """Force a local-filesystem URI, since the default filesystem is HDFS."""
    return path if "://" in path else f"file://{path}"


def spark_session(app_name: str, kafka: bool = False) -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1536m"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        # HDFS runs in Docker; the datanode advertises "localhost" (see docker-compose.yml).
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        # One datanode, so one copy. The client default of 3 leaves every block under-replicated.
        .config("spark.hadoop.dfs.replication", "1")
        .config("spark.hadoop.fs.defaultFS", config.HDFS_URL)
    )
    if kafka:
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGE)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.addPyFile(_ship_logiq_package())
    return spark
