import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree
import pyproj

def haversine_balltree(coords):
    # coords in radians [[lat, lon],...]
    return BallTree(coords, metric='haversine')

def latlon_to_radians(df, lat_col='lat', lon_col='lon'):
    coords = np.radians(df[[lat_col, lon_col]].to_numpy())
    return coords

def spatio_temporal_match(mrv_df, ais_df, era5_df, max_distance_km=20, max_time_minutes=60):
    """
    mrv_df: pandas DataFrame (voyage rows)
    ais_df: pandas DataFrame aggregated to voyage-day or representative lat/lon
    era5_df: pandas DataFrame with date/time lat lon fields
    Returns merged DataFrame with nearest ERA5 meteorology and AIS statistics.
    """
    # Ensure date/time columns
    # Convert lat/lon to radians
    ais_coords = latlon_to_radians(ais_df, 'lat', 'lon')
    tree = haversine_balltree(ais_coords)
    # Query each MRV row for nearest AIS representative
    mrv_coords = latlon_to_radians(mrv_df, 'lat', 'lon')
    dist, idx = tree.query(mrv_coords, k=1)
    # dist returned in radians; convert to km
    earth_radius_km = 6371.0088
    d_km = dist.flatten() * earth_radius_km
    matched = ais_df.iloc[idx.flatten()].reset_index(drop=True)
    merged = pd.concat([mrv_df.reset_index(drop=True), matched.add_prefix('ais_')], axis=1)
    # Nearest ERA5 by datetime and location (simplified)
    # In practice implement multi-index nearest join by time and location with KDTree + time tolerance
    return merged
