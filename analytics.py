# mobility_pipeline/analytics.py
import numpy as np
import pandas as pd
import geopandas as gpd
import trackintel as ti

class MobilityAnalytics:
    def __init__(self, tz="Europe/Lisbon"):
        self.tz = tz

    # ---------- HOME / WORK on staypoints ----------
    def annotate_home_work(self, sps: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Adds home/work labels to staypoints using Trackintel's OSNA method.
        Expects staypoints columns at least:
          ['user_id', 'started_at', 'finished_at', 'geometry']
        Returns sps with columns:
          ['location_id', 'home','work'] (booleans or ints depending on TI version)
        """
        sps = sps.copy()
        # Your mapping: location_id = stop_id
        if "stop_id" in sps.columns:
            sps["location_id"] = sps["stop_id"]

        # Run OSNA home/work detection
        # (TI may add 'home'/'work' columns or a 'purpose' column; keep both if present)
        sps = ti.analysis.osna_method(sps)
        # If TI returns categorical purpose, make explicit booleans too
        #if "purpose" in sps.columns:
            #sps["home"] = (sps["purpose"].astype(str).str.lower() == "home").astype(int)
            #sps["work"] = (sps["purpose"].astype(str).str.lower() == "work").astype(int)

        return sps

    # ---------- TRANSPORT MODE on triplegs ----------
    def predict_transport_modes(self, tpls: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Uses your Trackintel transport mode predictor (as you wrote: tpls = tpls.predict_transport_mode()).
        If your TI version exposes a different entrypoint, swap the call here.
        """
        tpls = tpls.copy()
        # Your exact call
        if hasattr(tpls, "predict_transport_mode"):
            tpls = tpls.predict_transport_mode()
        else:
            # Fallback: warn user; you can plug your own predictor here
            raise AttributeError("triplegs GeoDataFrame has no .predict_transport_mode(). "
                                 "Replace this call with your mode classifier.")
        return tpls

    # ---------- METRICS for TRIPLEGS ----------
    def add_tripleg_metrics(self, tpls: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Adds 'length' (meters), 'duration_minutes', 'speed_kmh' to triplegs.
        Uses Trackintel geogr utils where available.
        Requires columns: ['started_at','finished_at','geometry']
        """
        tpls = tpls.copy()

        # Length (meters) via Haversine
        tpls["length"] = ti.geogr.calculate_haversine_length(tpls)

        # Duration (minutes)
        tpls["started_at"] = pd.to_datetime(tpls["started_at"])
        tpls["finished_at"] = pd.to_datetime(tpls["finished_at"])
        tpls["duration_minutes"] = (tpls["finished_at"] - tpls["started_at"]).dt.total_seconds() / 60.0

        # Speed (km/h) — guard against division by zero
        tpls["speed_kmh"] = (tpls["length"] / 1000.0) / (tpls["duration_minutes"] / 60.0)
        tpls.loc[tpls["duration_minutes"] <= 0, "speed_kmh"] = np.nan

        # If available: TI’s helper for tripleg speed (m/s); keep as reference
        if hasattr(ti.geogr, "get_speed_triplegs"):
            spd = ti.geogr.get_speed_triplegs(tpls)  # m/s
            # Align index if needed
            spd = pd.Series(spd, index=tpls.index)
            tpls["speed_mps_geogr"] = spd
        return tpls

    # ---------- SPEED for POSITIONFIXES ----------
    def add_pfs_speed(self, pfs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Adds instantaneous speed on positionfixes via TI helper if available.
        """
        pfs = pfs.copy()
        if hasattr(ti.geogr, "get_speed_positionfixes"):
            sp = ti.geogr.get_speed_positionfixes(pfs)  # m/s
            sp = pd.Series(sp, index=pfs.index)
            pfs["speed_mps"] = sp
            pfs["speed_kmh"] = pfs["speed_mps"] * 3.6
        return pfs
