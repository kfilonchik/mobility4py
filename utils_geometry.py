import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point

class GeometryUtils:
    @staticmethod
    def telco_to_math_angle(telco_angle):
        return (90 - telco_angle) % 360

    @staticmethod
    def adjust_azimuth_for_omni(azimuth_min_telco, azimuth_max_telco):
        try:
            azimuth_min_telco = float(azimuth_min_telco) if not pd.isna(azimuth_min_telco) else 0
        except ValueError:
            azimuth_min_telco = 0

        try:
            azimuth_max_telco = float(azimuth_max_telco) if not pd.isna(azimuth_max_telco) else 0
        except ValueError:
            azimuth_max_telco = 0

        if azimuth_min_telco == 0 and azimuth_max_telco == 0:
            return 0, 360  

        az_min = GeometryUtils.telco_to_math_angle(azimuth_min_telco)
        az_max = GeometryUtils.telco_to_math_angle(azimuth_max_telco)
        if az_max < az_min:
            az_max += 360
        return az_min, az_max

    @staticmethod
    def make_sector_projected(x, y, az_min, az_max, radius_m, num_points=60):
        angles = np.linspace(np.deg2rad(az_min), np.deg2rad(az_max), num_points)
        arc_points = [(x + radius_m * np.cos(a), y + radius_m * np.sin(a)) for a in angles]
        points = gpd.GeoSeries([Point(x, y)] + [Point(px, py) for px, py in arc_points])
        return points.union_all().convex_hull

    @staticmethod
    def deterministic_point_in_polygon(user_id, cell_id, polygon):
        if polygon.is_empty:
            return None
        minx, miny, maxx, maxy = polygon.bounds
        rng = np.random.default_rng(abs(hash((user_id, cell_id))) % (2**32))
        for _ in range(50):
            px = rng.uniform(minx, maxx)
            py = rng.uniform(miny, maxy)
            point = Point(px, py)
            if polygon.contains(point):
                return point
        return polygon.centroid

