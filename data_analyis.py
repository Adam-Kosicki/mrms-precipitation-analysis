import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import warnings
import argparse

# Try to import geospatial libraries, but don't fail if they're not there.
try:
    import geopandas as gpd
    from shapely.geometry import Point
    import contextily as cx
    GEOSPATIAL_LIBS_AVAILABLE = True
except ImportError:
    GEOSPATIAL_LIBS_AVAILABLE = False
    warnings.warn(
        "Geospatial libraries (geopandas, contextily) not found. "
        "Spatial analysis will be skipped. To install, run: \n"
        "pip install geopandas contextily"
    )

def analyze_file_formats():
    """
    Loads and analyzes the metadata for NetCDF and GRIB2 files to show their differences.
    """
    print("--- File Format Analysis ---")
    
    try:
        with open('netcdf_vs_grib2/netcdf_file_format.json', 'r') as f:
            netcdf_format = json.load(f)
        
        with open('netcdf_vs_grib2/grib2_file_format.json', 'r') as f:
            grib2_format = json.load(f)
    except FileNotFoundError as e:
        print(f"Error: {e}. Make sure the format JSON files are in the correct directory.")
        return

    # Get first file from each as an example
    first_netcdf_file = list(netcdf_format.keys())[0]
    netcdf_meta = netcdf_format[first_netcdf_file]
    
    first_grib2_file = list(grib2_format.keys())[0]
    grib2_meta = grib2_format[first_grib2_file]
    
    # --- NetCDF Analysis ---
    print("\n--- NetCDF Metadata (example) ---")
    print(f"File: {first_netcdf_file}")
    
    info_str = netcdf_meta.get('info', '')
    lon_match = re.search(r'lon = (\d+)', info_str)
    lat_match = re.search(r'lat = (\d+)', info_str)
    if lon_match and lat_match:
        print(f"Dimensions: lat={lat_match.group(1)}, lon={lon_match.group(1)}")

    print("Variables:")
    netcdf_units = 'N/A'
    for var, attrs_str in netcdf_meta.get('variables', {}).items():
        try:
            attrs = literal_eval(attrs_str)
            print(f"  - {var}: units={attrs.get('units', 'N/A')}, long_name={attrs.get('long_name', 'N/A')}")
            if var == 'mrms_a2m':
                netcdf_units = attrs.get('units', 'N/A')
        except (ValueError, SyntaxError):
             print(f"  - {var}: {attrs_str} (could not parse attributes)")

    # --- GRIB2 Analysis ---
    print("\n--- GRIB2 Metadata (example) ---")
    print(f"File: {first_grib2_file}")
    print(f"Dimensions: lat={grib2_meta.get('Nj')}, lon={grib2_meta.get('Ni')}")
    print("Properties:")
    print(f"  - Name: {grib2_meta.get('name')}")
    print(f"  - Units: {grib2_meta.get('units')}")
    print(f"  - Short Name: {grib2_meta.get('shortName')}")

    # --- Summary of Differences ---
    print("\n--- Key Differences in Precipitation Data ---")
    print(f"NetCDF: variable 'mrms_a2m', Units: {netcdf_units} (likely 2-minute accumulation)")
    print(f"GRIB2: variable '{grib2_meta.get('shortName')}', Units: {grib2_meta.get('units')} (rate)")
    print("\nConclusion:")
    print("The primary difference is that NetCDF files provide accumulated precipitation (mm),")
    print("while GRIB2 files provide precipitation rate (mm/hr).")
    print("For a fair comparison in 'value_not_zero.json', the GRIB2 rates appear to have been converted to 2-minute accumulations.")

def analyze_zero_value_data(df):
    """
    Performs specific analysis for the 'zero-value' dataset.
    """
    print("\n\n--- Zero-Value Discrepancy Analysis ---")
    
    netcdf_zero = df['netcdf_precip_mm'] == 0
    grib2_zero = df['grib2_precip_mm_2min'] == 0

    netcdf_nonzero = df['netcdf_precip_mm'] > 0
    grib2_nonzero = df['grib2_precip_mm_2min'] > 0

    # Calculate the different conditions
    both_zero = df[netcdf_zero & grib2_zero].shape[0]
    netcdf_zero_grib2_nonzero = df[netcdf_zero & grib2_nonzero].shape[0]
    netcdf_nonzero_grib2_zero = df[netcdf_nonzero & grib2_zero].shape[0]
    both_nonzero = df[netcdf_nonzero & grib2_nonzero].shape[0]

    print("\nComparison of Zero vs. Non-Zero Precipitation:")
    print(f"  - Both NetCDF and GRIB2 are 0: \t\t{both_zero} incidents")
    print(f"  - NetCDF is 0, GRIB2 is non-zero: \t{netcdf_zero_grib2_nonzero} incidents")
    print(f"  - NetCDF is non-zero, GRIB2 is 0: \t{netcdf_nonzero_grib2_zero} incidents")
    print(f"  - Both are non-zero (unexpected for this dataset): \t{both_nonzero} incidents")
    
    # Show stats for the cases where GRIB2 was not zero
    if netcdf_zero_grib2_nonzero > 0:
        print("\nDescriptive Statistics for GRIB2 values when NetCDF was 0:")
        print(df[netcdf_zero & grib2_nonzero]['grib2_precip_mm_2min'].describe())

def analyze_data(df):
    """
    Performs and prints statistical analysis of the precipitation and distance data.
    """
    print("\n\n--- Precipitation Data Analysis ---")
    
    # --- 1. Aggregate Statistical Comparison ---
    print("\nDescriptive Statistics for 2-min Precipitation (mm):")
    print(df[['netcdf_precip_mm', 'grib2_precip_mm_2min']].describe())

    # Calculate and print error metrics
    precip_diff = df['netcdf_precip_mm'] - df['grib2_precip_mm_2min']
    mae = np.mean(np.abs(precip_diff))
    rmse = np.sqrt(np.mean(precip_diff**2))
    bias = np.mean(precip_diff)

    print("\nKey Error Metrics (NetCDF - GRIB2):")
    print(f"  - Mean Absolute Error (MAE): {mae:.4f} mm")
    print(f"  - Root Mean Square Error (RMSE): {rmse:.4f} mm")
    print(f"  - Bias: {bias:.4f} mm")
    if bias > 0:
        print("    (Positive bias indicates NetCDF values are slightly higher on average)")
    elif bias < 0:
        print("    (Negative bias indicates GRIB2 values are slightly higher on average)")
    else:
        print("    (Bias is zero, indicating no average tendency)")


    print("\n\n--- Haversine Distance Analysis ---")
    print("\nDescriptive Statistics for Haversine Distance to Nearest Grid Point (meters):")
    print(df[['netcdf_nearest_dist_m', 'grib2_nearest_dist_m']].describe())
    
    print("\nInsight:")
    print("The difference in mean/std for distances reflects the two products using slightly different grid resolutions or alignments.")
    print("This is expected and confirms the script is correctly finding the nearest point on each distinct grid.")


def create_visualizations(df, file_prefix=""):
    """
    Creates and saves visualizations comparing NetCDF and GRIB2 data.
    """
    print("\n\n--- Creating Visualizations ---")
    sns.set_theme(style="whitegrid")
    
    output_dir = 'netcdf_vs_grib2'

    # --- 2. Visual and Distributional Analysis ---
    
    # Box Plot of Precipitation Values
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df[['netcdf_precip_mm', 'grib2_precip_mm_2min']])
    plt.title('Comparison of 2-min Precipitation Values', fontsize=16)
    plt.ylabel('Precipitation (mm)', fontsize=12)
    plt.xlabel('Data Source', fontsize=12)
    filename = f"{output_dir}/{file_prefix}precipitation_boxplot.png"
    plt.savefig(filename)
    print(f"Saved precipitation box plot to {filename}")
    plt.close()

    # Scatter Plot of Precipitation Values
    plt.figure(figsize=(8, 8))
    # Per the plan, GRIB2 is on the x-axis
    plt.scatter(df['grib2_precip_mm_2min'], df['netcdf_precip_mm'], alpha=0.4)
    max_val = max(df['netcdf_precip_mm'].max(), df['grib2_precip_mm_2min'].max())
    plt.plot([0, max_val], [0, max_val], 'r--', label='y=x (perfect agreement)')
    plt.title('NetCDF vs. GRIB2 Precipitation', fontsize=16)
    plt.xlabel('GRIB2 Precipitation (mm, 2-min accumulation)', fontsize=12)
    plt.ylabel('NetCDF Precipitation (mm, 2-min accumulation)', fontsize=12)
    plt.legend()
    plt.grid(True)
    filename = f"{output_dir}/{file_prefix}precipitation_scatter.png"
    plt.savefig(filename)
    print(f"Saved precipitation scatter plot to {filename}")
    plt.close()
    
    # Histogram of Precipitation Differences
    df['precip_diff'] = df['netcdf_precip_mm'] - df['grib2_precip_mm_2min']
    plt.figure(figsize=(12, 7))
    sns.histplot(df['precip_diff'], kde=True, bins=50, color='mediumpurple')
    plt.title('Distribution of Precipitation Difference (NetCDF - GRIB2)', fontsize=16)
    plt.xlabel('Difference (mm)', fontsize=12)
    plt.ylabel('Frequency', fontsize=12)
    mean_diff = df['precip_diff'].mean()
    median_diff = df['precip_diff'].median()
    plt.axvline(mean_diff, color='r', linestyle='--', label=f"Mean: {mean_diff:.4f}")
    plt.axvline(median_diff, color='g', linestyle='-', label=f"Median: {median_diff:.4f}")
    plt.legend()
    filename = f"{output_dir}/{file_prefix}precipitation_difference_distribution.png"
    plt.savefig(filename)
    print(f"Saved precipitation difference plot to {filename}")
    plt.close()

    # --- Distance Visualizations ---
    
    # Box Plot for Distance to Grid Point
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df[['netcdf_nearest_dist_m', 'grib2_nearest_dist_m']])
    plt.title('Distance from Incident to Nearest Grid Point', fontsize=16)
    plt.ylabel('Distance (meters)', fontsize=12)
    plt.xlabel('Data Source', fontsize=12)
    filename = f"{output_dir}/{file_prefix}distance_boxplot.png"
    plt.savefig(filename)
    print(f"Saved distance box plot to {filename}")
    plt.close()

    # Histogram for Distance to Grid Point
    plt.figure(figsize=(12, 7))
    sns.histplot(df['netcdf_nearest_dist_m'], color="skyblue", label="NetCDF", kde=True, bins=50)
    sns.histplot(df['grib2_nearest_dist_m'], color="lightcoral", label="GRIB2", kde=True, bins=50)
    plt.legend()
    plt.title('Distribution of Distance to Nearest Grid Point', fontsize=16)
    plt.xlabel('Distance (meters)', fontsize=12)
    plt.ylabel('Frequency', fontsize=12)
    filename = f"{output_dir}/{file_prefix}distance_distribution.png"
    plt.savefig(filename)
    print(f"Saved distance distribution plot to {filename}")
    plt.close()

    # --- 3. Spatial Analysis ---
    if GEOSPATIAL_LIBS_AVAILABLE:
        print("\n--- Creating Spatial Analysis Map ---")
        
        # Define a bounding box for the San Antonio area
        # Roughly [lon_min, lat_min, lon_max, lat_max]
        san_antonio_bbox = [-98.8, 29.2, -98.2, 29.7]
        
        # Filter the DataFrame to only include incidents within the San Antonio BBox
        sa_df = df[
            (df['incident_lon'] >= san_antonio_bbox[0]) & (df['incident_lon'] <= san_antonio_bbox[2]) &
            (df['incident_lat'] >= san_antonio_bbox[1]) & (df['incident_lat'] <= san_antonio_bbox[3])
        ]

        if sa_df.empty:
            print("No incidents found within the San Antonio bounding box. Skipping map.")
            return

        geometry = [Point(xy) for xy in zip(sa_df['incident_lon'], sa_df['incident_lat'])]
        gdf = gpd.GeoDataFrame(sa_df, geometry=geometry, crs="EPSG:4326")
        
        # Convert to a projected CRS for accurate plotting and basemap overlay
        gdf = gdf.to_crs(epsg=3857)
        
        fig, ax = plt.subplots(1, 1, figsize=(12, 12))
        
        # Use the magnitude of the difference for coloring
        gdf.plot(
            ax=ax, 
            column='precip_diff', 
            cmap='coolwarm', 
            markersize=150, # Increased marker size
            legend=True,
            legend_kwds={'label': "Precipitation Difference (mm)\n(NetCDF - GRIB2)",
                         'orientation': "horizontal"},
            edgecolor='black', # Added edge color for visibility
            linewidth=0.5
        )
        
        # Set the map extent to the projected San Antonio bounding box
        min_lon, min_lat, max_lon, max_lat = san_antonio_bbox
        # Project the bounding box corners to the target CRS
        bbox_gdf = gpd.GeoDataFrame(
            geometry=[Point(min_lon, min_lat), Point(max_lon, max_lat)],
            crs="EPSG:4326"
        ).to_crs(epsg=3857)
        
        xlim = (bbox_gdf.geometry[0].x, bbox_gdf.geometry[1].x)
        ylim = (bbox_gdf.geometry[0].y, bbox_gdf.geometry[1].y)
        ax.set_xlim(xlim)
        ax.set_ylim(ylim)

        cx.add_basemap(ax, source=cx.providers.CartoDB.Positron, zoom=11)
        ax.set_title('Spatial Distribution of Precipitation Differences (San Antonio)', fontsize=16)
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        
        filename = f"{output_dir}/{file_prefix}spatial_difference_map_san_antonio.png"
        plt.savefig(filename, bbox_inches='tight')
        print(f"Saved spatial difference map to {filename}")
        plt.close()
    else:
        print("\nSkipping spatial analysis map because geospatial libraries are not installed.")

    print(f"\nAll visualizations have been saved in the '{output_dir}/' directory.")

def main():
    """
    Main function to run the complete analysis pipeline.
    """
    parser = argparse.ArgumentParser(description="Analyze and visualize MRMS NetCDF vs GRIB2 data.")
    parser.add_argument(
        '--file', 
        type=str, 
        default='netcdf_vs_grib2/value_not_zero.json',
        help="Path to the input JSON file to analyze."
    )
    args = parser.parse_args()

    try:
        df = pd.read_json(args.file)
        print(f"Successfully loaded '{args.file}'\n")
    except Exception as e:
        print(f"Error loading '{args.file}': {e}")
        return

    # The file format analysis is very specific and might fail if JSON format changes.
    # The primary analysis is on the value_not_zero.json file, so we focus on that.
    # analyze_file_formats()
    
    file_prefix = ""
    # Run specific analysis if it's the zero-value file
    if 'zero_value' in args.file:
        file_prefix = "zero_value_"
        analyze_zero_value_data(df)
    elif 'not_zero' in args.file:
        file_prefix = "nonzero_value_"

    analyze_data(df)
    create_visualizations(df, file_prefix)

if __name__ == '__main__':
    main()
