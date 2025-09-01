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
            gdf, user_id='user_id', tracked_at='timestamp', geom_col='geometry', tz=self.tz
        )
        return pfs

    def build_staypoints_from_stopids(self, pfs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Your aggregation logic turning InfoStop 'stop_id' into TI-style staypoints.
        """
        assert 'stop_id' in pfs.columns, "Positionfixes must carry 'stop_id'"
        # Only rows assigned to a stop
        s = pfs.loc[pfs['stop_id'] != -1].copy()
        # Aggregate per (user, stop_id, date) into a single stay (min..max interval)
        #s['event_date'] = s['tracked_at'].dt.date
        agg = (
            s.groupby(['user_id','stop_id','event_date'], as_index=False)
             .agg(started_at=('tracked_at','min'),
                  finished_at=('tracked_at','max'),
                  geometry=('geometry','first'))        # centroid or first is okay for TI schema
        )
        agg['staypoint_id'] = np.arange(len(agg))
        sps = gpd.GeoDataFrame(agg, geometry='geometry', crs=pfs.crs)
        return sps

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
        p = p.merge(tmp[["user_id","started_at","staypoint_id"]],
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
