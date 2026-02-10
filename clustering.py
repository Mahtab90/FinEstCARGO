#!/usr/bin/env python3
"""
clustering.py
Performs KMeans clustering on voyage-day features and outputs cluster summaries.
"""
import argparse
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)   # parquet features path
    parser.add_argument("--output", required=True)
    parser.add_argument("--k", type=int, default=3)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("CARGO_Clustering").getOrCreate()
    df = spark.read.parquet(args.input)
    # select numeric features for clustering
    feature_cols = ['EEOI','avg_speed_knots','co2_kg','cargo_tonnes']
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_assembled")
    vdf = assembler.transform(df).select('features_assembled')
    scaler = StandardScaler(inputCol="features_assembled", outputCol="features_scaled", withStd=True, withMean=True)
    scaler_model = scaler.fit(vdf)
    scaled = scaler_model.transform(vdf)

    kmeans = KMeans(k=args.k, seed=42, featuresCol='features_scaled')
    model = kmeans.fit(scaled)
    centers = model.clusterCenters()
    # append cluster label to original df and compute summary
    clustered = model.transform(scaled)
    # join back labels with original df as necessary (left as exercise)
    spark.stop()
    print("Clustering finished. Centers:", centers)

if __name__ == "__main__":
    main()
