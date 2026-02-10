#!/usr/bin/env python3
"""
Entropy-based discretization (MDLP) using the mdlp library or custom implementation.
Generates discrete bins per continuous column and saves encoder objects.
"""
import argparse
import pandas as pd
import joblib
from mdlp.discretization import MDLP

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)    # parquet/csv features
    parser.add_argument("--columns", required=False, default="")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    df = pd.read_parquet(args.input) if args.input.endswith('.parquet') else pd.read_csv(args.input)
    cont_cols = args.columns.split(',') if args.columns else ['avg_speed_knots','precip_mm_per_h','wind_speed_m_s','EEOI']
    encoders = {}
    for c in cont_cols:
        series = df[c].fillna(df[c].median()).values.reshape(-1,1)
        # MDLP expects (X, y). We discretize relative to a target (CO2 surge). For ARM we may discretize independent of y
        # Example: use a proxy target (co2_kg > threshold) for supervised MDLP; or use unsupervised binning fallback.
        try:
            y = (df['co2_kg'] > df['co2_kg'].quantile(0.9)).astype(int).values
            mdlp = MDLP()
            bins = mdlp.fit_transform(series, y)
            encoders[c] = mdlp
            df[c + "_bin"] = bins
        except Exception as e:
            # fallback to KBinsDiscretizer
            from sklearn.preprocessing import KBinsDiscretizer
            est = KBinsDiscretizer(n_bins=6, encode='ordinal', strategy='quantile')
            df[c + "_bin"] = est.fit_transform(series).astype(int)
            encoders[c] = est

    joblib.dump(encoders, args.output + ".encoders.pkl")
    df.to_parquet(args.output, index=False)
    print("Saved binned features to", args.output)

if __name__ == "__main__":
    main()
