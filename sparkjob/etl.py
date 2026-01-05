import os
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructField, StringType, StructType

ICEBERG_CATALOG = "warehouse"
ICEBERG_TABLE = "github.events"
ICEBERG_TARGET = f"{ICEBERG_CATALOG}.{ICEBERG_TABLE}"


def create_spark_session() -> SparkSession:
    """Create a Spark session configured for Iceberg + S3A/MinIO."""

    # These env vars can be mounted through Spark Operator env/secret injection
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_access_key = os.getenv("S3_ACCESS_KEY", "minioadmin")
    s3_secret_key = os.getenv("S3_SECRET_KEY", "minioadmin")
    checkpoint_location = os.getenv("CHECKPOINT_LOCATION", "s3a://checkpoints/gh-etl/")

    builder = (
        SparkSession.builder.appName("gharchive-iceberg-writer")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.{catalog}".format(catalog=ICEBERG_CATALOG), "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.{catalog}.type".format(catalog=ICEBERG_CATALOG), "hadoop")
        .config(
            "spark.sql.catalog.{catalog}.warehouse".format(catalog=ICEBERG_CATALOG),
            os.getenv("ICEBERG_WAREHOUSE", "s3a://iceberg-data/gh-archive"),
        )
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    )

    spark = builder.getOrCreate()

    # Persist checkpoint location on the session to reuse in start_stream
    spark.conf.set("spark.app.checkpointLocation", checkpoint_location)
    return spark


def process_batch_factory(spark: SparkSession, iceberg_table: str):
    """Return a foreachBatch handler that downloads JSON from presigned URLs."""

    def _process_batch(batch_df, batch_id):
        # Avoid extra work if no messages arrived in this micro-batch
        if batch_df.rdd.isEmpty():
            return

        urls: List[str] = [row.presigned_url for row in batch_df.select("presigned_url").collect() if row.presigned_url]
        if not urls:
            return

        # GitHub Archive files are NDJSON (optionally gzip); Spark handles compression transparently
        payload_df = spark.read.json(urls)

        # Append directly to the Iceberg table; schema evolves automatically
        payload_df.writeTo(iceberg_table).append()

    return _process_batch


def start_stream():
    spark = create_spark_session()

    kafka_schema = StructType(
        [
            StructField("key", StringType()),
            StructField("presigned_url", StringType()),
            StructField("endpoint", StringType()),
        ]
    )

    raw_kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP", "kafka-service:9092"))
        .option("subscribe", os.getenv("KAFKA_TOPIC", "gh-archive-urls"))
        .option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "latest"))
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        raw_kafka_df.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), kafka_schema).alias("data"))
        .select("data.*")
    )

    query = (
        parsed_df.writeStream.foreachBatch(process_batch_factory(spark, ICEBERG_TARGET))
        .option("checkpointLocation", spark.conf.get("spark.app.checkpointLocation"))
        .outputMode("update")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    start_stream()