import geopandas as gpd
from  utils_geometry import GeometryUtils
import logging

class CDRProcessor:
    def __init__(self, radius_km=1.0, crs_proj="EPSG:3763"):
        self.radius_km = radius_km
        self.crs_proj = crs_proj
        self.logger = logging.getLogger("MobilityPipeline.CDRProcessor")

    def process(self, df, rivers_gdf):
        try:
            self.logger.info("Starting CDR processing...")

            rivers_proj = rivers_gdf.to_crs(self.crs_proj)
            water_union = rivers_proj.union_all()

            towers_df = df[["cell_id", "longitude_cell", "latitude_cell"]].drop_duplicates()
            towers_gdf = gpd.GeoDataFrame(
                towers_df,
                geometry=gpd.points_from_xy(towers_df["longitude_cell"], towers_df["latitude_cell"]),
                crs="EPSG:4326"
            )

            # Project tower coordinates to metric CRS (EPSG:3763)
            towers_gdf = towers_gdf.to_crs(self.crs_proj)

            towers_gdf["az_min"] = towers_gdf["cell_id"].map(df.groupby("cell_id")["azi_min1"].first())
            towers_gdf["az_max"] = towers_gdf["cell_id"].map(df.groupby("cell_id")["azi_max1"].first())
            towers_gdf["new_radius"] = towers_gdf["cell_id"].map(df.groupby("cell_id")["new_radius"].first())

            towers_gdf["sector_poly"] = [
                        GeometryUtils.make_sector_projected(
                            pt.x, pt.y,
                            *GeometryUtils.adjust_azimuth_for_omni(az_min, az_max),
                            radius  # << this is unique per tower
                        )
                        for pt, az_min, az_max, radius in zip(
                            towers_gdf.geometry,
                            towers_gdf["az_min"],
                            towers_gdf["az_max"],
                            towers_gdf["new_radius"]
                        )
                    ]

            sectors_gdf = gpd.GeoDataFrame(towers_gdf[["cell_id"]], geometry=towers_gdf["sector_poly"], crs=self.crs_proj)
            sectors_gdf["geometry"] = sectors_gdf.geometry.apply(lambda g: g.difference(water_union))

            sectors_gdf["geometry"] = sectors_gdf.geometry.apply(
                lambda g: max(g.geoms, key=lambda gg: gg.area) if g.geom_type == "MultiPolygon" else g
            )

            sectors_gdf.to_file("sectors.geojson", driver="GeoJSON")

            df = df.merge(
                sectors_gdf[["cell_id", "geometry"]],
                on="cell_id",
                how="left",
                validate="many_to_one"
            )

            df["point_proj"] = [
                GeometryUtils.deterministic_point_in_polygon(uid, cid, poly) if poly and not poly.is_empty else None
                for uid, cid, poly in zip(df["unique_id"], df["cell_id"], df["geometry"])
            ]

            points_gdf = gpd.GeoDataFrame(df, geometry=df["point_proj"], crs=self.crs_proj).to_crs("EPSG:4326")
            df["est_lon"] = points_gdf.geometry.x
            df["est_lat"] = points_gdf.geometry.y

            self.logger.info("CDR processing completed successfully.")
            return df

        except Exception as e:
            self.logger.error(f"Error during CDR processing: {e}", exc_info=True)
            raise
