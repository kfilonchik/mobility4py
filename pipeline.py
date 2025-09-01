import logging
from  cdr_processor import CDRProcessor
from trackintel_render import TrackintelBridge
from infostop_detector import InfoStopDetector
from analytics import MobilityAnalytics
#from .staypoint_detector import StaypointDetector
#from .trip_segmenter import TripSegmenter

class MobilityPipeline:
    def __init__(self, radius_km=1.0, crs_proj="EPSG:3763", tz="Europe/Lisbon"):
        self.cdr_processor = CDRProcessor(radius_km, crs_proj)
        self.stops = InfoStopDetector()
        self.ti = TrackintelBridge(tz=tz)
        self.analytics = MobilityAnalytics(tz=tz)
        #elf.staypoint_detector = StaypointDetector()
        #self.trip_segmenter = TripSegmenter()
        self.logger = logging.getLogger("MobilityPipeline")

    def run(self, df, rivers_gdf):
        try:
            self.logger.info("Pipeline started.")
            #df_processed = self.cdr_processor.process(df, rivers_gdf)
            df_processed = df
            staypoints = self.stops.run(df_processed)
            self.logger.info("Saving Staypoints.")
            staypoints.to_pickle("output/staypoints.pkl")

            # 3) to TI positionfixes
            pfs = self.ti.to_positionfixes(staypoints)

            # 4) staypoints (from stop_id) and assign to pfs
            sps = self.ti.build_staypoints_from_stopids(pfs)
            pfs_sp = self.ti.assign_staypoint_ids_to_pfs(pfs, sps)

            sps_hw = self.analytics.annotate_home_work(sps)
            self.logger.info("Saving Staypoints with HM.")
            sps_hw.to_pickle("output/staypoints_with_hm.pkl")


            # 5) triplegs/trips (fill in your working TI calls)
            pfs, tpls = self.ti.pfs_triplegs(pfs_sp)
            pfs_spd = self.analytics.add_pfs_speed(pfs)

            self.logger.info("Saving PFX with speed.")
            pfs_spd.to_pickle("output/pfs.pkl")

            tpls = self.analytics.predict_transport_modes(tpls)
            tpls = self.analytics.add_tripleg_metrics(tpls)

            self.logger.info("Saving Triplegs.")
            tpls.to_pickle("output/triplegs.pkl")

            #trips = self.trip_segmenter.segment(staypoints)
            self.logger.info("Pipeline completed successfully.")
            return {
                "processed_cdr": df_processed,
                "staypoints": staypoints,
                "staypoints_hw": sps_hw,
                "pfs": pfs_spd,
                "triplegs": tpls
                #"trips": trips
            }
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise
