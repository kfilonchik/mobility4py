import pandas as pd
import geopandas as gpd
from logger_config import setup_logger
from pipeline import MobilityPipeline
from azure.ai.ml import MLClient
from azure.identity import DefaultAzureCredential
from azure.ai.ml.entities import Data
from azure.ai.ml.constants import AssetTypes
from azure.ai.ml.entities import Environment

if __name__ == "__main__":
    logger = setup_logger()

    try:
        logger.info("Loading input data...")
        # CDR data loads
        ml_client = MLClient.from_config(credential=DefaultAzureCredential(), path="config.json")
        cdr_asset = ml_client.data.get(name="lisboa_feb_march", version="1")
        df = pd.read_parquet(cdr_asset.path)
        df['a_bts_cgi'] = df['a_bts_cgi'].astype("int")
        df = df[['unique_id', 'time_id','event_date', 'a_bts_cgi']]

        # Read network file
        network_asset = ml_client.data.get("network_file_v9", version="1")
        network = pd.read_csv(network_asset.path)
        
        network = network.dropna(how="all")
        network['cell_id'] = network['cgi_key'].astype("int")
        network['cell_key'] = network['cgi_key'].astype("int")

        cdr_df = pd.merge(df, network[['longitude_cell', 'latitude_cell','cgi_key', 'cell_id','r', 'azi_min1', 'azi_max1', 'concelho', 'new_radius']], left_on='a_bts_cgi', right_on='cgi_key', how='inner')

        #cdr_df = cdr_df.sample(10000)
        #cdr_df = pd.read_pickle("/home/azureuser/cloudfiles/code/Users/20230692/mobility4py/datasets/processed_cdr.pkl")

        rivers_gdf = gpd.read_file("datasets/hotosm_prt_waterways_polygons_geojson.geojson")
        rivers_gdf = rivers_gdf.loc[(rivers_gdf['name:en'] == 'Tagus River') & (rivers_gdf['osm_type'] == 'ways_poly')]

        logger.info("Running mobility pipeline...")
        pipeline = MobilityPipeline(radius_km=1.0)
        results = pipeline.run(cdr_df, rivers_gdf)

        #results["processed_cdr"] = results["processed_cdr"].drop(columns=['geometry', 'point_proj'])

        #results["processed_cdr"].to_pickle("output/processed_cdr.pkl")
        #results["staypoints"].to_csv("output/staypoints.csv", index=False)
        #results["trips"].to_csv("output/trips.csv", index=False)

        #if results["staypoints"] is not None:
            #results["staypoints"].to_pickle("output/staypoints.pkl")
        #if results["triplegs"] is not None:
            #results["triplegs"].to_pickle("output/triplegs.pkl")
        #if results["pfs"] is not None:
            #results["pfs"].to_pickle("output/pfs.pkl")

        logger.info("Done.")

        logger.info("All results saved successfully.")

    except Exception as e:
        logger.error(f"Main execution failed: {e}", exc_info=True)
