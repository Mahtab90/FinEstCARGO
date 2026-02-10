#!/usr/bin/env bash
# Example spark-submit for FP-Growth ARM pipeline
SPARK_HOME=/opt/spark
APP=./scripts/arm_pyspark.py
INPUT_S3=s3://your-bucket/processed/binned_features.parquet
OUTPUT_S3=s3://your-bucket/results/arm_rules/
NUM_EXECUTORS=50
EXECUTOR_CORES=8
EXECUTOR_MEM=64G
DRIVER_MEM=32G

$SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "CARGO_ARM" \
  --conf spark.executor.instances=$NUM_EXECUTORS \
  --conf spark.executor.cores=$EXECUTOR_CORES \
  --conf spark.executor.memory=$EXECUTOR_MEM \
  --conf spark.driver.memory=$DRIVER_MEM \
  --conf spark.sql.shuffle.partitions=2000 \
  --conf spark.default.parallelism=2000 \
  $APP --input $INPUT_S3 --output $OUTPUT_S3 --minSupport 0.05 --minConfidence 0.6 --numPartitions 2000
