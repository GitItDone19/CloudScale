"""Session defaults shared by local jobs and the Spark container."""

from pyspark.sql import SparkSession


def build_spark(app_name="CloudScale Silver", master="local[2]"):
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.csv.parser.columnPruning.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
