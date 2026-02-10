#!/usr/bin/env python3
"""
arm_pyspark.py
Runs FP-Growth on a large binned dataset using Spark MLlib.
Outputs CSV with antecedent, consequent, support, confidence, lift.
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, concat_ws, split, explode
from pyspark.ml.fpm import FPGrowth
import math

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)   # parquet path
    parser.add_argument("--output", required=True)  # s3 path or local
    parser.add_argument("--minSupport", default=0.05, type=float)
    parser.add_argument("--minConfidence", default=0.6, type=float)
    parser.add_argument("--numPartitions", default=2000, type=int)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("CARGO_FP_Growth").getOrCreate()
    df = spark.read.parquet(args.input)
    # build item columns: for each categorical column create string "col=val"
    cat_cols = [c for c in df.columns if c.endswith("_bin") or c in ['fuel_type','ship_type']]
    # build array column 'items'
    from pyspark.sql.functions import array
    items_cols = [concat_ws("=", col(c).cast("string")) for c in cat_cols]
    # incorrect above: need to create items as ["col=val", ...] per row
    from pyspark.sql.functions import struct, lit
    def make_item(colname):
        return (col(colname).cast("string"))

    # create items as array of strings with column names explicitly
    from pyspark.sql.functions import array, expr
    items_expr = array(*[expr(f"concat('{c}=', cast({c} as string))") for c in cat_cols])
    df_items = df.withColumn("items", items_expr).select("items")

    # repartition for fp-growth performance
    df_items = df_items.repartition(args.numPartitions)

    fp = FPGrowth(itemsCol="items", minSupport=args.minSupport, minConfidence=args.minConfidence)
    model = fp.fit(df_items)

    # associationRules is a DataFrame [antecedent, consequent, confidence]
    rules = model.associationRules
    # compute support for antecedent and consequent
    # convert supports to floats and compute lift = support(ante+conse)/ (support(ante)*support(conse))
    # model.freqItemsets contains freq for itemsets
    freq = model.freqItemsets
    freq_local = freq.withColumnRenamed("items", "itemset").withColumnRenamed("freq", "support_count")

    # join supports (implementation detail: collect freq to driver for small cardinality; or broadcast)
    freq_pd = freq_local.toPandas()
    total_transactions = df_items.count()
    support_map = {tuple(row['itemset']): row['support_count']/total_transactions for _, row in freq_pd.iterrows()}

    # convert rules to pandas for lift computation (safe if number of rules small)
    rules_pd = rules.toPandas()
    def compute_lift(row):
        ant = tuple(row['antecedent'])
        cons = tuple(row['consequent'])
        combined = tuple(list(row['antecedent']) + list(row['consequent']))
        s_ante = support_map.get(ant, 0.0)
        s_cons = support_map.get(cons, 0.0)
        s_comb = support_map.get(combined, 0.0)
        lift = (s_comb / (s_ante * s_cons + 1e-12)) if (s_ante>0 and s_cons>0) else None
        return lift

    rules_pd['lift'] = rules_pd.apply(compute_lift, axis=1)
    # save
    rules_out = args.output.rstrip('/') + "/arm_rules.csv"
    rules_pd.to_csv(rules_out, index=False)
    print("Saved ARM rules to", rules_out)
    spark.stop()

if __name__ == "__main__":
    main()
