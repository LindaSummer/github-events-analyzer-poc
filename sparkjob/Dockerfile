# Simple image to run the GH Archive Iceberg ETL with Spark Operator
# Assumes Spark Operator passes --packages for Iceberg and Kafka (see sparkapp.yaml)
FROM apache/spark:3.4.4-scala2.12-java11-python3-ubuntu

WORKDIR /opt/spark/work-dir

# Copy application code
COPY . ./

# Default command is inherited from the base image; Spark Operator supplies
# spark-submit arguments based on the SparkApplication spec.
