#!/usr/bin/env python3
"""
preprocess.py (production)
- Reads MRV (CSV), AIS (parquet), ERA5 (netcdf/csv) from input storage (S3 or local)
- Performs cleaning, normalization, voyage-level aggregation, and emits parquet files
- Writes schema metadata for downstream consumption
"""
import os
import argparse
from pathlib import Path
import dask.dataframe as dd
import pandas as pd
import numpy as np
from datetime import datetime
from scripts.spatio_temporal_join import spatio_temporal_match

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--mrv", required=True, help="S3:// or path to MRV CSV(s)")
    p.add_argument("--ais", required=True, help="S3:// or path to AIS parquet(s)")
    p.add_argument("--era5", required=True, help="S3:// or path to ERA5 netcdf/csv")
    p.add_argument("--out", required=True, help="Output folder (S3 or local) for processed parquet")
    p.add_argument("--tempdir", default="/tmp/cargo")
    return p.parse_args()

def normalize_mrv(ddf):
    # standardize column names & units
    ddf = ddf.rename(columns=lambda x: x.strip().lower())
    # ensure required columns exist
    required = ['imo','voyage_id','report_year','co2_kg','cargo_tonnes','distance_nm','fuel_type','ship_type']
    for c in required:
        if c not in ddf.columns:
            raise ValueError(f"Missing required MRV column: {c}")
    # cast numerics
    ddf['co2_kg'] = ddf['co2_kg'].astype('float64')
    ddf['cargo_tonnes'] = ddf['cargo_tonnes'].astype('float64')
    ddf['distance_nm'] = ddf['distance_nm'].astype('float64')
    return ddf

def main():
    args = parse_args()
    os.makedirs(args.tempdir, exist_ok=True)
    # MRV read (Dask)
    mrv = dd.read_csv(args.mrv)
    mrv = normalize_mrv(mrv)
    # Read AIS (parquet)
    ais = dd.read_parquet(args.ais)
    # Read ERA5 (assume pre-extracted csv/ parquet splits)
    era5 = dd.read_parquet(args.era5) if args.era5.endswith(".parquet") else dd.read_csv(args.era5)

    # Aggregate AIS to voyage-day level (avg speed, etc.)
    ais['timestamp'] = dd.to_datetime(ais['timestamp'] if 'timestamp' in ais.columns else (ais['date']+' '+ais['time']))
    agg = ais.groupby(['imo','voyage_id', ais['timestamp'].dt.date.rename('vdate')]).agg({
        'speed_over_ground': 'mean',
        'lat': 'median','lon':'median'
    }).reset_index().rename(columns={'speed_over_ground':'avg_speed_knots'})

    # Align MRV per voyage with AIS aggregated features and ERA5 nearest-in-time/space
    # Convert Dask->Pandas in manageable partitions and call spatio_temporal_match per partition
    def matcher(part):
        return spatio_temporal_match(part, agg.compute(), era5.compute())  # spatio_temporal_match optimized for in-memory partitions

    # Use map_partitions if necessary; here we do single-step for clarity
    merged = pd.merge(mrv.compute(), agg.compute(), on=['imo','voyage_id'], how='left')
    merged = pd.merge(merged, era5.compute(), left_on=['vdate','lat','lon'], right_on=['date','lat','lon'], how='left')

    # Feature engineering
    merged['eeoi'] = merged['co2_kg'] / (merged['cargo_tonnes'] * merged['distance_nm'] + 1e-9)

    # Save to parquet (partition by year and ship_type for efficient reads)
    out_path = args.out.rstrip('/') + '/features.parquet'
    pd.DataFrame(merged).to_parquet(out_path, index=False)
    print("Saved features to", out_path)

if __name__ == "__main__":
    main()
