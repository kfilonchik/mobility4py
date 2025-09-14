# mobility_pipeline/trackintel_bridge.py
import numpy as np
import pandas as pd
import geopandas as gpd
import trackintel as ti

class TrackintelBridge:
    def __init__(self, tz="Europe/Lisbon"):
        self.tz = tz

    def to_positionfixes(self, df: pd.DataFrame) -> gpd.GeoDataFrame:
        """
        Build a GeoDataFrame and then TI positionfixes from your processed CDR.
        Expects: ['user_id','timestamp','est_lon','est_lat','stop_id', ...]
        """
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df["est_lon"], df["est_lat"]),
            crs="EPSG:4326"
        )
        pfs = ti.io.read_positionfixes_gpd(
            gdf, user_id='unique_id', tracked_at='timestamp', geom_col='geometry', tz=self.tz
        )
        return pfs


    def build_staypoints_from_pfs(self, pfs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Build staypoints from (user_id, stop_id, tracked_at) with day resets.
        - Consecutive rows with same (user_id, day, stop_id) belong to one staypoint.
        - Rows with stop_id == -1 are ignored (staypoint_id = NA).
        - Returns a GeoDataFrame with user_id, staypoint_id, started_at, finished_at, stop_id, geometry.
        - Index is set to staypoint_id (kept also as a column).
        """
        df = pfs.copy()
        # Day boundary (if you need Lisbon-local days, replace with:
        # df["day"] = df["tracked_at"].dt.tz_convert("Europe/Lisbon").dt.normalize()
        df["day"] = df["tracked_at"].dt.normalize()

        # Work on a sorted copy; keep original index for alignment
        g = df.sort_values(["user_id", "day", "tracked_at"]).copy()

        valid = g["stop_id"].ne(-1)
        starts = (
            g["user_id"].ne(g["user_id"].shift()) |   # new user
            g["day"].ne(g["day"].shift()) |           # new day
            g["stop_id"].ne(g["stop_id"].shift())     # stop changed
        )

        # Global ID that increments only on valid starts; -1 rows -> NA
        g["staypoint_id"] = (starts & valid).cumsum()
        g.loc[~valid, "staypoint_id"] = np.nan
        g["staypoint_id"] = g["staypoint_id"].astype("Int64")

        # Map IDs back to original row order without shifting anything
        df["staypoint_id"] = g.sort_index()["staypoint_id"]

        # Aggregate stays (ignore NA)
        staypoints = (
            df.dropna(subset=["staypoint_id"])
            .groupby(["user_id", "staypoint_id"], as_index=False)
            .agg(
                started_at=("tracked_at", "min"),
                finished_at=("tracked_at", "max"),
                stop_id=("stop_id", "first"),
                geometry=("geometry", "first")  # or compute centroid if preferred
            )
            .sort_values(["user_id", "started_at"])
        )

        # Optional: duration
        #staypoints["duration"] = staypoints["finished_at"] - staypoints["started_at"]

        # GeoDataFrame with same CRS as input
        #staypoints = gpd.GeoDataFrame(staypoints, geometry="geometry", crs=pfs.crs)
        staypoints = ti.io.read_staypoints_gpd(staypoints, started_at = 'started_at', finished_at ='finished_at', geom_col = 'geometry', crs="EPSG:4326", tz='Europe/Lisbon')

        # Set index to staypoint_id but keep the column
        staypoints = staypoints.set_index("staypoint_id", drop=False)

        return g, staypoints


    def assign_staypoint_ids_to_pfs(self, pfs: gpd.GeoDataFrame, sps: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Forward-fill candidate staypoint IDs, then keep only within [started_at, finished_at].
        Requires that you already added a 'sp_cand' to pfs (candidate mapping).
        If you don't have it, derive a candidate by joining on proximity in time & same stop_id.
        """
        # Minimal robust mapping: use (user_id, stop_id, event_date)
        tmp = sps[['user_id','stop_id','started_at','staypoint_id','finished_at']].copy()
        #tmp['event_date'] = pd.to_datetime(tmp['event_date'])

        p = pfs.copy()
        #p['event_date'] = p['tracked_at'].dt.normalize()
        p = p.sort_values(["user_id","tracked_at"], kind="mergesort").reset_index(drop=True)
        p = p.merge(tmp[["user_id","started_at","staypoint_id", "geometry"]],
                    left_on = ["user_id","tracked_at"],
                    right_on= ["user_id","started_at"],
                    how="left"
                    ).rename(columns={"staypoint_id":"sp_start"}).drop(columns=["started_at"])
        
        # 2) Forward-fill the last seen start within each user → candidate stay id
        p["sp_cand"] = p.groupby("user_id")["sp_start"].ffill()

        # 3) LEFT JOIN bring finished_at for that candidate id
        p = p.merge(
            tmp[["staypoint_id","finished_at"]].rename(columns={"staypoint_id":"sp_cand"}),
            on="sp_cand",
            how="left"
        )

        # 4) Keep ID only inside the interval [started_at, finished_at]
        # (started_at already implied by ffill from step 1)
        p["staypoint_id"] = p["sp_cand"].where(p["tracked_at"] <= p["finished_at"]).astype("Int64")

        # 5) cleanup
        p = p.drop(columns=["sp_start","sp_cand","finished_at"])

        return p

    def pfs_triplegs(self, pfs_with_sp: gpd.GeoDataFrame, sps: gpd.GeoDataFrame):
        """
        Use trackintel to derive triplegs and trips from positionfixes+staypoints.
        Exact function names depend on TI version; the idea is:
         - segment into triplegs between staypoints
         - aggregate triplegs into trips
        """
        pfs, tpls = ti.preprocessing.generate_triplegs(pfs_with_sp, sps, 'overlap_staypoints',gap_threshold=30)

        return pfs, tpls 
    
    def pfs_trips(self, tpls: gpd.GeoDataFrame, sps: gpd.GeoDataFrame):
        """
        Use trackintel to derive triplegs and trips from positionfixes+staypoints.
        Exact function names depend on TI version; the idea is:
         - segment into triplegs between staypoints
         - aggregate triplegs into trips
        """
        staypoints, triplegs, trips = ti.preprocessing.generate_trips(tpls, sps)

        return staypoints, triplegs, trips
