# Simple image to run the GH Archive Iceberg ETL with Spark Operator
# Assumes Spark Operator passes --packages for Iceberg and Kafka (see sparkapp.yaml)
FROM apache/spark:4.1.0-scala2.13-java17-python3-ubuntu

WORKDIR /opt/spark/work-dir

# Copy application code
COPY etl.py main.py ./
COPY README.md ./

# Default command is inherited from the base image; Spark Operator supplies
# spark-submit arguments based on the SparkApplication spec.
