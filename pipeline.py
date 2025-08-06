import logging
from  cdr_processor import CDRProcessor
#from .staypoint_detector import StaypointDetector
#from .trip_segmenter import TripSegmenter

class MobilityPipeline:
    def __init__(self, radius_km=1.0, crs_proj="EPSG:3763"):
        self.cdr_processor = CDRProcessor(radius_km, crs_proj)
        #elf.staypoint_detector = StaypointDetector()
        #self.trip_segmenter = TripSegmenter()
        self.logger = logging.getLogger("MobilityPipeline")

    def run(self, df, rivers_gdf):
        try:
            self.logger.info("Pipeline started.")
            df_processed = self.cdr_processor.process(df, rivers_gdf)
            #staypoints = self.staypoint_detector.detect(df_processed)
            #trips = self.trip_segmenter.segment(staypoints)
            self.logger.info("Pipeline completed successfully.")
            return {
                "processed_cdr": df_processed#,
                #"staypoints": staypoints,
                #"trips": trips
            }
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise
