import pandas as pd
import geopandas as gpd
import folium

def plot_user_locations(sp_filtered_freq, location_col='center', purpose_col='purpose', user_col='user_id'):
    """
    Plots user location points on a Folium map based on purpose and unique user count.

    Parameters:
    - sp_filtered_freq: DataFrame with at least [location_col, purpose_col, user_col] columns
    - location_col: Name of the column containing Shapely Point geometries
    - purpose_col: Name of the column specifying the user's purpose (e.g. 'home', 'work')
    - user_col: Name of the column with user IDs

    Returns:
    - Folium Map object
    """
    # Step 1: Group and count unique users
    location_counts = sp_filtered_freq.groupby([location_col, purpose_col])[user_col].nunique().reset_index(name='user_count')

    # Step 2: Compute min and max user counts
    max_users = location_counts['user_count'].max()
    min_users = location_counts['user_count'].min()

    # Step 3: Convert to GeoDataFrame
    gdf = gpd.GeoDataFrame(location_counts, geometry=location_col, crs='EPSG:4326')
    gdf = gdf.rename(columns={location_col: 'geometry'})

    # Step 4: Create folium map centered on Lisbon (customize as needed)
    m = folium.Map(location=[38.72, -9.14], zoom_start=12)

    # Step 5: Add circle markers
    for _, row in gdf.iterrows():
        color = 'red' if row[purpose_col] == 'home' else 'blue'
        radius = 1 + (row['user_count'] - min_users) * (20 - 1) / (max_users - min_users) if max_users > min_users else 10

        folium.CircleMarker(
            location=[row.geometry.y, row.geometry.x],
            radius=radius,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.6,
            popup=f"{row[purpose_col].capitalize()}<br>{row['user_count']} user(s)"
        ).add_to(m)

    return m
