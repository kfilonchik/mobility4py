# mobility_pipeline/infostop_detector.py
import logging
import numpy as np
import pandas as pd
from infostop import Infostop
from utils_geometry import GeometryUtils


class InfoStopDetector:
    """
    Runs InfoStop on processed CDR points and persists result with stop_id.
    Expects columns:
      - id_col (default 'unique_id')
      - time_col (default 'time_id')  -> will be converted to 'unix_timestamp'
      - est_lon, est_lat
    """
    def __init__(self,
                 id_col: str = "unique_id",
                 time_col: str = "time_id",
                 lon_col: str = "est_lon",
                 lat_col: str = "est_lat"
                 #,pickle_out: str = "datasets/processed_with_stops.pkl"
                 ):
        self.id_col = id_col
        self.time_col = time_col
        self.lon_col = lon_col
        self.lat_col = lat_col
        #self.pickle_out = pickle_out
        self.logger = logging.getLogger("MobilityPipeline.InfoStopDetector")

        # Your exact parameters:
        self.model = Infostop(
            r1=20, r2=20,
            label_singleton=True,
            min_staying_time=600,
            max_time_between=86400,
            min_size=2
        )

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Preparing data for InfoStop: %d rows", len(df))
        # 1) Ensure time is clean and add unix_timestamp
        if not np.issubdtype(df[self.time_col].dtype, np.datetime64):
            # optional clean as in your snippet
            if df[self.time_col].dtype == object:
                df = df.copy()
                df[self.time_col] = df[self.time_col].astype(str).str.replace(".0", "", regex=False)
            df = GeometryUtils.convert_to_unix_timestamp(df, self.time_col, output_col="unix_timestamp")
        else:
            df = GeometryUtils.convert_to_unix_timestamp(df, self.time_col, output_col="unix_timestamp")

        # 2) Sort by user and time (critical)
        df = df.sort_values(by=[self.id_col, "unix_timestamp"]).reset_index(drop=True)

        # 3) Build traces per user in the order you requested: [est_lat, est_lon, unix_timestamp]
        traces = [
            grp[[self.lat_col, self.lon_col, "unix_timestamp"]].to_numpy()
            for _, grp in df.groupby(self.id_col, sort=False)
        ]

        self.logger.info("Running InfoStop on %d user traces", len(traces))
        labels_nested = self.model.fit_predict(traces)

        # 4) Flatten and assign back
        all_labels = np.concatenate(labels_nested)  # your exact line
        out = df.copy()
        out["stop_id"] = all_labels

        # 5) Save to pickle
        #out.to_pickle(self.pickle_out)
        #self.logger.info("Saved InfoStop output with stop_id → %s", self.pickle_out)

        return out

