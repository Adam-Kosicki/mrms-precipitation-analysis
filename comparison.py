import argparse
import asyncio
import gzip
import io
import json
import logging
import math
import os
import shutil
import sys
import xarray as xr
from datetime import datetime
import pytz
import aiohttp
import numpy as np
import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.pool import SimpleConnectionPool
from scipy.spatial import KDTree
import pygrib

# --- Constants ---
DATA_DIR = "data"
NETCDF_DATA_DIR = os.path.join(DATA_DIR, "netcdf")
GRIB_DATA_DIR = os.path.join(DATA_DIR, "grib2")
GRIB2_FORMAT_FILE = "netcdf_vs_grib2/grib2_file_format.json"
NETCDF_FORMAT_FILE = "netcdf_vs_grib2/netcdf_file_format.json"
INCIDENTS_JSON_FILE = "netcdf_vs_grib2/value_not_zero.json"

# --- New Constants for Zero-Value Comparison ---
GRIB2_FORMAT_ZERO_FILE = "netcdf_vs_grib2/grib2_file_format_zero.json"
NETCDF_FORMAT_ZERO_FILE = "netcdf_vs_grib2/netcdf_file_format_zero.json"
INCIDENTS_ZERO_JSON_FILE = "netcdf_vs_grib2/incidents_zero_value.json"

# --- New Constants for NetCDF from IEM ---
NETCDF_PRODUCT_CODE = "mrms_a2m"
API_THROTTLE_ERROR_COUNT = 0
INVALID_NETCDF_ERROR_COUNT = 0

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s'
)

# --- Database Operations ---
class DatabasePool:
    """A simple connection pool for psycopg2."""
    def __init__(self, db_params, minconn=1, maxconn=2):
        try:
            self.pool = SimpleConnectionPool(minconn, maxconn, **db_params)
            logging.info("Database connection pool created successfully.")
        except Exception as e:
            logging.error(f"Error creating connection pool: {e}", exc_info=True)
            raise
    def get_connection(self): return self.pool.getconn()
    def put_connection(self, conn): self.pool.putconn(conn)
    def close_all(self): self.pool.closeall()

database_pool = None

def init_db_pool(db_params, minconn=1, maxconn=2):
    global database_pool
    if database_pool is None:
        database_pool = DatabasePool(db_params, minconn, maxconn)

def get_db_connection_params():
    load_dotenv()
    db_params = {
        'dbname': os.getenv('DB_NAME'), 'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'), 'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT')
    }
    if not all(db_params.values()):
        logging.error("DB connection parameters missing in .env file.")
        return None
    return db_params

def inspect_table_schema(table_name):
    if not database_pool: return logging.error("DB pool not initialized.")
    conn = database_pool.get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_name = %s ORDER BY ordinal_position;
            """, (table_name,))
            columns = cursor.fetchall()
            if not columns: return logging.warning(f"Table '{table_name}' not found.")
            print(f"--- Schema for table: {table_name} ---")
            for col in columns: print(f'  - "{col[0]}"\t{col[1]}')
            print("---------------------------------------\n")
    finally:
        database_pool.put_connection(conn)

def run_db_inspection(args):
    db_params = get_db_connection_params()
    if not db_params: return
    try:
        init_db_pool(db_params)
        inspect_table_schema(args.table_name)
    finally:
        if database_pool: database_pool.close_all()

# --- GRIB2 and Analysis Operations ---
def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance in meters between two points
    on the earth (specified in decimal degrees).
    """
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    R = 6371000  # Radius of earth in meters
    return R * c

def vectorized_nearest_indices(grid, values):
    """
    For a sorted 1D grid array and a numpy array of values, returns the index
    in grid corresponding to the nearest neighbor for each value.
    """
    indices = np.searchsorted(grid, values)
    indices = np.clip(indices, 1, len(grid) - 1)
    left = indices - 1
    right = indices
    choose_left = np.abs(values - grid[left]) < np.abs(values - grid[right])
    return np.where(choose_left, left, right)

async def download_weather_data_async(session, product_code, date_time, semaphore, max_retries=5, backoff_factor=60):
    """Downloads NetCDF data from IEM."""
    global API_THROTTLE_ERROR_COUNT, INVALID_NETCDF_ERROR_COUNT
    # IEM URL uses format YYYYMMDDHHMM
    formatted_date = date_time.strftime('%Y%m%d%H%M')
    attempt = 0
    async with semaphore:
        while attempt <= max_retries:
            netcdf_url = f"https://mesonet.agron.iastate.edu/cgi-bin/request/raster2netcdf.py?dstr={formatted_date}&prod={product_code}"
            try:
                async with session.get(netcdf_url, timeout=60) as response:
                    if response.status == 200:
                        content = await response.read()
                        try:
                            # Validate by trying to open
                            with xr.open_dataset(io.BytesIO(content), engine='h5netcdf'):
                                pass
                            logging.debug(f"Successfully downloaded and validated NetCDF for {formatted_date}")
                            return (io.BytesIO(content), formatted_date, True, attempt + 1, "ok")
                        except Exception as e:
                            logging.error(f"Invalid NetCDF file content for {formatted_date}: {e}")
                            INVALID_NETCDF_ERROR_COUNT += 1
                            return (io.BytesIO(content), formatted_date, False, attempt + 1, "invalid_netcdf_content")
                    elif response.status == 429:
                        API_THROTTLE_ERROR_COUNT += 1
                        retry_after = int(response.headers.get('Retry-After', backoff_factor))
                        logging.warning(f"API rate limit reached for {formatted_date}. Retrying after {retry_after} seconds.")
                        await asyncio.sleep(retry_after)
                        attempt += 1
                    else:
                        logging.error(f"Failed to download data for {formatted_date}. HTTP Status: {response.status}")
                        attempt += 1
                        if attempt <= max_retries:
                           await asyncio.sleep(backoff_factor * attempt)
            except asyncio.TimeoutError:
                logging.error(f"Timeout error downloading {formatted_date} on attempt {attempt+1}.")
                attempt += 1
            except Exception as e:
                logging.error(f"Error downloading {formatted_date} on attempt {attempt+1}: {e}")
                attempt += 1

            if attempt <= max_retries:
                await asyncio.sleep(backoff_factor * attempt)

        logging.error(f"Exceeded maximum retries for {formatted_date}. Skipping download.")
        return (None, formatted_date, False, attempt, "max_retries_exceeded")

async def fetch_netcdf_grid_coordinates_once(session, semaphore):
    """Attempts to download one file from IEM to get the grid coordinates."""
    logging.info("Attempting to fetch NetCDF grid coordinates from IEM...")
    # Use a recent, fixed timestamp to ensure we get a valid file
    sample_timestamp = datetime(2024, 6, 1, 12, 0, tzinfo=pytz.utc)

    result_tuple = await download_weather_data_async(
        session, NETCDF_PRODUCT_CODE, sample_timestamp, semaphore
    )

    if result_tuple and result_tuple[0] is not None and result_tuple[2]:
        file_data_bytesio, _, _, _, _ = result_tuple
        try:
            with xr.open_dataset(file_data_bytesio, engine='h5netcdf') as ds:
                if 'lat' in ds.variables and 'lon' in ds.variables:
                    grid_lon = ds['lon'].values.copy()
                    grid_lat = ds['lat'].values.copy()
                    logging.info(f"Successfully fetched NetCDF grid (Lat: {grid_lat.shape}, Lon: {grid_lon.shape}).")
                    return grid_lat, grid_lon
                else:
                    logging.error("'lat' or 'lon' variables not found in sample NetCDF file.")
                    return None, None
        except Exception as e:
            logging.error(f"Failed to extract grid from sample NetCDF file: {e}")
            return None, None
    else:
        status_msg = result_tuple[4] if result_tuple else "Unknown download failure"
        logging.error(f"Could not download sample NetCDF file to fetch grid. Status: {status_msg}")
        return None, None

def get_grid_kdtree(lats, lons):
    """Builds a KDTree from latitude and longitude grids."""
    if lats.ndim == 1 and lons.ndim == 1:
        lons_grid, lats_grid = np.meshgrid(lons, lats)
    else:
        lats_grid, lons_grid = lats, lons
    
    lon_360 = np.where(lons_grid < 0, lons_grid + 360, lons_grid)
    return KDTree(np.vstack([lats_grid.ravel(), lon_360.ravel()]).T)

def find_nearest_point_and_value(incident, kdtree, lats, lons, values):
    """Finds the nearest grid point, its value, and the distance."""
    if lats.ndim == 1 and lons.ndim == 1:
        lons_grid, lats_grid = np.meshgrid(lons, lats)
    else:
        lats_grid, lons_grid = lats, lons
    
    lon_360 = np.where(lons_grid < 0, lons_grid + 360, lons_grid)

    incident_lon_360 = incident['incident_lon'] + 360 if incident['incident_lon'] < 0 else incident['incident_lon']
    _, nearest_idx = kdtree.query([incident['incident_lat'], incident_lon_360])
    
    lat_idx, lon_idx = np.unravel_index(nearest_idx, lats_grid.shape)
    
    point_lat = lats_grid[lat_idx, lon_idx]
    point_lon_360 = lon_360[lat_idx, lon_idx]
    point_lon_180 = point_lon_360 if point_lon_360 <= 180 else point_lon_360 - 360

    distance_m = haversine_distance(
        incident['incident_lat'], incident['incident_lon'], point_lat, point_lon_180
    )
    
    value = values[lat_idx, lon_idx]
    value = value if not np.isnan(value) and value >= 0 else None
    
    return {
        'lat': point_lat, 'lon': point_lon_180, 
        'dist_m': distance_m, 'precip_mm': value
    }

def fetch_grib_grid_coordinates_once():
    """Downloads one sample GRIB file to determine the grid coordinate system."""
    if not os.path.exists(GRIB_DATA_DIR): os.makedirs(GRIB_DATA_DIR)
    sample_utc_time = datetime(2024, 6, 1, 12, 0, tzinfo=pytz.utc)
    file_timestamp = sample_utc_time.strftime('%Y%m%d-%H%M%S')
    filename = f"MRMS_PrecipRate_00.00_{file_timestamp}.grib2.gz"
    filepath = os.path.join(GRIB_DATA_DIR, filename)
    
    if not os.path.exists(filepath):
        path_date = sample_utc_time.strftime('%Y%m%d')
        grib_url = f"https://noaa-mrms-pds.s3.amazonaws.com/CONUS/PrecipRate_00.00/{path_date}/{filename}"
        try:
            with requests.get(grib_url, stream=True) as r:
                r.raise_for_status()
                with open(filepath, 'wb') as f: shutil.copyfileobj(r.raw, f)
            logging.info(f"Successfully downloaded sample GRIB file to {filepath}")
        except Exception as e:
            logging.error(f"Failed to download sample GRIB file: {e}")
            return None, None

    temp_uncompressed_path = filepath.replace('.gz', '.tmp')
    try:
        with gzip.open(filepath, 'rb') as f_in, open(temp_uncompressed_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        with pygrib.open(str(temp_uncompressed_path)) as grbs:
            for grb in grbs:
                if grb.discipline == 209 and grb.parameterCategory == 6 and grb.parameterNumber == 1:
                    lats, lons = grb.latlons()
                    logging.info(f"Extracted grid from sample GRIB. Lat shape: {lats.shape}")
                    return lats, lons
        logging.error("Could not find PrecipRate message in sample file.")
        return None, None
    except Exception as e:
        logging.error(f"Failed to process sample GRIB file {filepath}: {e}")
        return None, None
    finally:
        if os.path.exists(temp_uncompressed_path): os.remove(temp_uncompressed_path)

def process_grib_file_values(grib_file_path):
    """Opens a GRIB2 file, finds the PrecipRate message, and extracts its data and metadata, SKIPPING latlons."""
    temp_uncompressed_path = grib_file_path.replace('.gz', f".{os.getpid()}.tmp")
    try:
        with gzip.open(grib_file_path, 'rb') as f_in, open(temp_uncompressed_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        with pygrib.open(temp_uncompressed_path) as grbs:
            for grb in grbs:
                if grb.discipline == 209 and grb.parameterCategory == 6 and grb.parameterNumber == 1:
                    metadata = {key: grb[key] for key in [
                        'discipline', 'disciplineName', 'parameterCategory', 'parameterNumber', 'level', 'typeOfLevel',
                        'stepRange', 'validityDate', 'validityTime', 'Ni', 'Nj',
                        'projString'] if grb.has_key(key)}
                    metadata.update({
                        'name': 'Radar Precipitation Rate',
                        'shortName': 'PrecipRate',
                        'units': 'mm/hr'
                    })
                    values = grb.values
                    if hasattr(values, 'filled'): values = values.filled(np.nan)
                    return {'data': values, 'metadata': metadata}
        logging.warning(f"Could not find PrecipRate message (209,6,1) in {grib_file_path}.")
        return None
    except Exception as e:
        logging.error(f"Failed to process GRIB file {grib_file_path}: {e}", exc_info=True)
        return None
    finally:
        if os.path.exists(temp_uncompressed_path): os.remove(temp_uncompressed_path)

async def download_file_async(session, url, filepath, semaphore):
    """Generic async file downloader."""
    if os.path.exists(filepath): return True
    async with semaphore:
        try:
            async with session.get(url, timeout=120) as response:
                if response.status == 200:
                    with open(filepath, 'wb') as f: f.write(await response.read())
                    return True
                else:
                    logging.warning(f"Download failed for {url} with status: {response.status}")
                    return False
        except Exception as e:
            logging.error(f"Exception downloading {url}: {e}")
            return False

async def download_grib_file_async(session, timestamp_dt, semaphore):
    """Downloads a GRIB2 file for a specific timestamp from the NOAA S3 bucket."""
    if not os.path.exists(GRIB_DATA_DIR): os.makedirs(GRIB_DATA_DIR)
    path_date = timestamp_dt.strftime('%Y%m%d')
    file_timestamp = timestamp_dt.strftime('%Y%m%d-%H%M%S')
    filename = f"MRMS_PrecipRate_00.00_{file_timestamp}.grib2.gz"
    filepath = os.path.join(GRIB_DATA_DIR, filename)
    grib_url = f"https://noaa-mrms-pds.s3.amazonaws.com/CONUS/PrecipRate_00.00/{path_date}/{filename}"
    return await download_file_async(session, grib_url, filepath, semaphore)

async def download_netcdf_file_async(session, timestamp_dt, semaphore):
    """Downloads a NetCDF file for a specific timestamp from the NOAA S3 bucket."""
    if not os.path.exists(NETCDF_DATA_DIR): os.makedirs(NETCDF_DATA_DIR)
    path_date = timestamp_dt.strftime('%Y%m%d')
    file_timestamp = timestamp_dt.strftime('%Y%m%d-%H%M%S')
    filename = f"MRMS_SeamlessHSR_00.00_{file_timestamp}.netcdf.gz"
    filepath = os.path.join(NETCDF_DATA_DIR, filename)
    netcdf_url = f"https://noaa-mrms-pds.s3.amazonaws.com/CONUS/SeamlessHSR_00.00/{path_date}/{filename}"
    return await download_file_async(session, netcdf_url, filepath, semaphore)

def process_netcdf_file(netcdf_file_path):
    """Opens a NetCDF file and extracts its data and coordinate info."""
    temp_uncompressed_path = netcdf_file_path.replace('.gz', f".{os.getpid()}.tmp")
    try:
        with gzip.open(netcdf_file_path, 'rb') as f_in, open(temp_uncompressed_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        with xr.open_dataset(temp_uncompressed_path) as ds:
            # Based on inspection of similar files, variable might be 'SeamlessHSR'
            data_var_name = next(iter(ds.data_vars))
            logging.info(f"Using NetCDF data variable: {data_var_name}")
            
            lats, lons = ds['lat'].values, ds['lon'].values
            values = ds[data_var_name].values
            
            metadata = ds.attrs
            metadata.update({v: ds[v].attrs for v in ds.variables})
            
            return {'data': values, 'lats': lats, 'lons': lons, 'metadata': metadata}
    except Exception as e:
        logging.error(f"Failed to process NetCDF file {netcdf_file_path}: {e}", exc_info=True)
        return None
    finally:
        if os.path.exists(temp_uncompressed_path): os.remove(temp_uncompressed_path)

# --- Main Workflows ---
async def run_comparison_workflow(args, fetch_zero_values=False):
    """
    Generic comparison workflow.
    `fetch_zero_values=True` runs the comparison for incidents with data_value = 0.
    """
    db_params = get_db_connection_params()
    if not db_params: return

    init_db_pool(db_params)
    conn = database_pool.get_connection()
    incidents = []
    try:
        with conn.cursor() as cursor:
            if fetch_zero_values:
                logging.info("Fetching incidents with data_value = 0 from the database...")
                query = """
                    SELECT *
                    FROM public.mrms_data_for_cris_records_60min_statewide WHERE data_value = 0.0 LIMIT 400;
                """
            else:
                logging.info("Fetching incidents with data_value > 0 from the database...")
                query = """
                    SELECT *
                    FROM public.mrms_data_for_cris_records_60min_statewide WHERE data_value > 0.0 LIMIT 400;
                """
            cursor.execute(query)
            rows = cursor.fetchall()
            if not rows:
                logging.warning(f"No incidents found for the specified condition (zero_values={fetch_zero_values}).")
                return
            column_names = [desc[0] for desc in cursor.description]
            incidents = [dict(zip(column_names, row)) for row in rows]
            logging.info(f"Successfully fetched {len(incidents)} incidents.")
    finally:
        database_pool.put_connection(conn)

    def get_aligned_timestamp(ts):
        """Aligns timestamp to the nearest 2-minute interval and ensures UTC."""
        if isinstance(ts, str):
            ts_dt = datetime.strptime(ts.replace(" UTC", ""), '%Y-%m-%d %H:%M:%S')
        else:
            ts_dt = ts

        if ts_dt.tzinfo is None:
            ts_dt = pytz.utc.localize(ts_dt)
        else:
            ts_dt = ts_dt.astimezone(pytz.utc)
        
        return ts_dt.replace(
            second=0, microsecond=0, minute=(ts_dt.minute // 2) * 2
        )

    # Group incidents by timestamp and add verification logging
    incidents_by_timestamp = {}
    logging.info("Verifying timestamp alignment for the first 5 incidents...")
    for i, incident in enumerate(incidents):
        aligned_ts = get_aligned_timestamp(incident['mrms_timestamp'])
        if i < 5:
            logging.info(f"  - Incident ID {incident['incident_id']}: Original MRMS TS: {incident['mrms_timestamp']} -> Aligned File TS: {aligned_ts}")
        if aligned_ts not in incidents_by_timestamp:
            incidents_by_timestamp[aligned_ts] = []
        incidents_by_timestamp[aligned_ts].append(incident)

    # --- File Downloads ---
    unique_timestamps = list(incidents_by_timestamp.keys())
    logging.info(f"Found {len(unique_timestamps)} unique timestamps for {len(incidents)} incidents.")
    
    downloaded_netcdf_files = {} # Dict to store {timestamp_str: BytesIO}

    logging.info("Starting file downloads for GRIB2 (S3) and NetCDF (IEM)...")
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(20) # Shared semaphore
        
        # Create GRIB download tasks
        grib_download_tasks = [download_grib_file_async(session, dt, semaphore) for dt in unique_timestamps]
        
        # Create NetCDF download tasks
        netcdf_download_tasks = [download_weather_data_async(session, NETCDF_PRODUCT_CODE, dt, semaphore) for dt in unique_timestamps]
        
        # Run GRIB downloads
        await asyncio.gather(*grib_download_tasks)
        logging.info("GRIB2 file downloads from S3 are complete.")

        # Run NetCDF downloads and store results
        netcdf_download_results = await asyncio.gather(*netcdf_download_tasks)
        for res in netcdf_download_results:
            if res and res[0] and res[2]: # If content exists and is valid
                content_bytes, ts_str, _, _, _ = res
                downloaded_netcdf_files[ts_str] = content_bytes
        logging.info(f"NetCDF file downloads from IEM are complete. Successfully downloaded {len(downloaded_netcdf_files)} files.")

    # --- Grid Fetching and KDTree/Index Setup ---
    # GRIB Setup (existing logic)
    logging.info("Fetching GRIB grid to build KDTree...")
    grib_lats, grib_lons = fetch_grib_grid_coordinates_once()
    if grib_lats is None or grib_lons is None:
        logging.error("Failed to fetch GRIB grid, aborting.")
        if database_pool: database_pool.close_all()
        return
    grib_kdtree = get_grid_kdtree(grib_lats, grib_lons)
    logging.info("Successfully built KDTree for GRIB grid.")

    # NetCDF Setup (new logic)
    logging.info("Fetching NetCDF grid for vectorized lookup...")
    async with aiohttp.ClientSession() as session:
        netcdf_grid_lat, netcdf_grid_lon = await fetch_netcdf_grid_coordinates_once(session, asyncio.Semaphore(1))
    
    if netcdf_grid_lat is None or netcdf_grid_lon is None:
        logging.error("Failed to fetch NetCDF grid, aborting.")
        if database_pool: database_pool.close_all()
        return

    # --- Main Comparison Loop ---
    updated_incidents = []
    grib2_formats = {}
    netcdf_formats = {} # To store NetCDF metadata
    
    logging.info(f"Starting comparison processing for {len(unique_timestamps)} unique timestamps...")
    for i, aligned_ts_dt in enumerate(unique_timestamps):
        if (i + 1) % 10 == 0:
            logging.info(f"Processing timestamp {i + 1}/{len(unique_timestamps)}...")
        
        incidents_for_ts = incidents_by_timestamp[aligned_ts_dt]
        
        # --- Process NetCDF Data for this timestamp ---
        netcdf_results_for_ts = {}
        netcdf_file_metadata = {}
        netcdf_api_ts = aligned_ts_dt.strftime('%Y%m%d%H%M')
        
        if netcdf_api_ts in downloaded_netcdf_files:
            try:
                with xr.open_dataset(downloaded_netcdf_files[netcdf_api_ts], engine="h5netcdf") as ds:
                    netcdf_file_metadata = ds.attrs
                    netcdf_formats[netcdf_api_ts] = netcdf_file_metadata # Store metadata
                    data_array = ds[NETCDF_PRODUCT_CODE].squeeze().values
                    
                    incident_lats_np = np.array([item['incident_lat'] for item in incidents_for_ts])
                    incident_lons_np = np.array([item['incident_lon'] for item in incidents_for_ts])

                    lat_indices = vectorized_nearest_indices(netcdf_grid_lat, incident_lats_np)
                    lon_indices = vectorized_nearest_indices(netcdf_grid_lon, incident_lons_np)

                    for j, item in enumerate(incidents_for_ts):
                        lat_idx, lon_idx = lat_indices[j], lon_indices[j]
                        grid_lat, grid_lon = float(netcdf_grid_lat[lat_idx]), float(netcdf_grid_lon[lon_idx])
                        precip_val = float(data_array[lat_idx, lon_idx])
                        
                        netcdf_results_for_ts[item['incident_id']] = {
                            'netcdf_nearest_lat': grid_lat,
                            'netcdf_nearest_lon': grid_lon,
                            'netcdf_nearest_dist_m': haversine_distance(item['incident_lat'], item['incident_lon'], grid_lat, grid_lon),
                            'netcdf_precip_mm': precip_val if precip_val >= 0 else None,
                            'netcdf_product_code': NETCDF_PRODUCT_CODE,
                        }
            except Exception as e:
                logging.error(f"Failed to process NetCDF file for {netcdf_api_ts}: {e}")
        else:
            logging.warning(f"NetCDF file for {netcdf_api_ts} was not downloaded.")

        # --- Process GRIB2 Data for this timestamp (existing logic, simplified) ---
        grib_results_for_ts = {}
        grib_file_metadata = {}
        grib_filename = f"MRMS_PrecipRate_00.00_{aligned_ts_dt.strftime('%Y%m%d-%H%M%S')}.grib2.gz"
        grib_filepath = os.path.join(GRIB_DATA_DIR, grib_filename)

        if os.path.exists(grib_filepath):
            grib_info = process_grib_file_values(grib_filepath)
            if grib_info:
                grib_file_metadata = grib_info['metadata']
                grib2_formats[grib_filename] = grib_file_metadata
                for item in incidents_for_ts:
                    search_point = {'incident_lat': item['incident_lat'], 'incident_lon': item['incident_lon']}
                    result = find_nearest_point_and_value(search_point, grib_kdtree, grib_lats, grib_lons, grib_info['data'])
                    
                    raw_precip_mm_hr = result['precip_mm']
                    precip_2min = (raw_precip_mm_hr / 60.0) * 2.0 if raw_precip_mm_hr is not None else None

                    grib_results_for_ts[item['incident_id']] = {
                        'grib2_nearest_lat': result['lat'],
                        'grib2_nearest_lon': result['lon'],
                        'grib2_nearest_dist_m': result['dist_m'],
                        'grib2_precip_raw_value_mm_hr': raw_precip_mm_hr,
                        'grib2_precip_unit': 'mm/hr',
                        'grib2_precip_mm_2min': precip_2min
                    }
            else:
                logging.warning(f"Failed to process GRIB file {grib_filepath}.")
        else:
            logging.warning(f"GRIB file {grib_filename} not found.")

        # --- Combine results for all incidents in this timestamp ---
        for incident in incidents_for_ts:
            updated_incident = incident.copy()
            incident_id = incident['incident_id']
            
            # Add general provenance info
            updated_incident['aligned_utc_timestamp'] = aligned_ts_dt.isoformat()
            path_date = aligned_ts_dt.strftime('%Y%m%d')
            file_ts_grib = aligned_ts_dt.strftime('%Y%m%d-%H%M%S')
            file_ts_netcdf = aligned_ts_dt.strftime('%Y%m%d%H%M')
            grib_fname = f"MRMS_PrecipRate_00.00_{file_ts_grib}.grib2.gz"
            
            updated_incident['grib2_source_url'] = f"https://noaa-mrms-pds.s3.amazonaws.com/CONUS/PrecipRate_00.00/{path_date}/{grib_fname}"
            updated_incident['netcdf_source_url'] = f"https://mesonet.agron.iastate.edu/cgi-bin/request/raster2netcdf.py?dstr={file_ts_netcdf}&prod={NETCDF_PRODUCT_CODE}"

            # Add NetCDF results and metadata if available
            if incident_id in netcdf_results_for_ts:
                updated_incident.update(netcdf_results_for_ts[incident_id])
                updated_incident['netcdf_file_metadata'] = netcdf_file_metadata

            # Add GRIB2 results and metadata if available
            if incident_id in grib_results_for_ts:
                updated_incident.update(grib_results_for_ts[incident_id])
                updated_incident['grib2_file_metadata'] = grib_file_metadata

            # Add original NetCDF data from DB for reference, with clearer names
            updated_incident['db_netcdf_precip_mm'] = updated_incident.pop('data_value', None)
            updated_incident['db_netcdf_lat'] = updated_incident.pop('mrms2_lat', None)
            updated_incident['db_netcdf_lon'] = updated_incident.pop('mrms2_lon', None)
            
            updated_incidents.append(updated_incident)

    # --- Save results ---
    if fetch_zero_values:
        incidents_file = INCIDENTS_ZERO_JSON_FILE
        grib2_format_file = GRIB2_FORMAT_ZERO_FILE
        netcdf_format_file = NETCDF_FORMAT_ZERO_FILE
    else:
        incidents_file = INCIDENTS_JSON_FILE
        grib2_format_file = GRIB2_FORMAT_FILE
        netcdf_format_file = NETCDF_FORMAT_FILE

    with open(incidents_file, 'w') as f: json.dump(updated_incidents, f, indent=4, default=str)
    with open(grib2_format_file, 'w') as f: json.dump(grib2_formats, f, indent=4, default=str)
    with open(netcdf_format_file, 'w') as f: json.dump(netcdf_formats, f, indent=4, default=str)
    
    logging.info(f"Comparison complete. Data saved to {incidents_file}, {grib2_format_file}, and {netcdf_format_file}")
    if database_pool: database_pool.close_all()

def main():
    parser = argparse.ArgumentParser(description="A script for downloading and comparing MRMS GRIB2 and NetCDF data.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    inspect_parser = subparsers.add_parser("inspect-db", help="Inspect a database table schema.")
    inspect_parser.add_argument("table_name", help="Name of the table to inspect.")
    
    subparsers.add_parser("run-comparison", help="Download and compare GRIB2 and NetCDF data for non-zero precipitation incidents.")
    subparsers.add_parser("run-zero-comparison", help="Download and compare GRIB2 and NetCDF data for zero precipitation incidents.")
    
    args = parser.parse_args()
    if args.command == 'inspect-db':
        run_db_inspection(args)
    elif args.command == 'run-comparison':
        asyncio.run(run_comparison_workflow(args, fetch_zero_values=False))
    elif args.command == 'run-zero-comparison':
        asyncio.run(run_comparison_workflow(args, fetch_zero_values=True))
    else:
        logging.error(f"Unknown command: {args.command}")
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()
