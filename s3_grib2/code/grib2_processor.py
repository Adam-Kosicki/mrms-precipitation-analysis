import boto3
from botocore.config import Config
from botocore import UNSIGNED
import gzip
import os
import pygrib
import pandas as pd
from datetime import datetime, timedelta
import pytz
from pathlib import Path
import time # Added for timing
import logging # Import logging
import shutil # Added for file operations

logger = logging.getLogger(__name__) # Module-level logger

class GRIB2Processor:
    def __init__(self, output_dir=None):
        self.s3_client = boto3.client('s3', region_name='us-east-1', config=Config(signature_version=UNSIGNED, max_pool_connections=50))
        self.bucket_name = 'noaa-mrms-pds'
        
        # Use $SCRATCH directory on TACC systems, fallback to local directory if not available
        if output_dir is None:
            scratch_dir = os.getenv('SCRATCH')
            if scratch_dir:
                self.output_dir = Path(scratch_dir) / 'mrms_grib2'
            else:
                self.output_dir = Path('data/grib2')
        else:
            self.output_dir = Path(output_dir)
            
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.processed_files = set()  # Keep track of files we've processed
        
    def get_grib2_filename(self, utc_time):
        """Generate the expected GRIB2 filename for a given UTC time"""
        date_str = utc_time.strftime('%Y%m%d')
        time_str = utc_time.strftime('%H%M%S')
        # Updated for PrecipRate product
        return f"MRMS_PrecipRate_00.00_{date_str}-{time_str}.grib2.gz"
    
    def download_grib2(self, utc_time):
        """Download GRIB2 file from S3 for a given UTC time"""
        date_str = utc_time.strftime('%Y%m%d')
        filename = self.get_grib2_filename(utc_time)
        # Updated for PrecipRate product
        s3_key = f"CONUS/PrecipRate_00.00/{date_str}/{filename}"
        
        local_path = self.output_dir / filename
        if local_path.exists():
            logger.debug(f"File already exists, skipping download: {local_path}")
            return local_path
            
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, str(local_path))
            logger.info(f"Successfully downloaded S3 object {s3_key} from bucket {self.bucket_name} to {local_path}")
            return local_path
        except Exception as e:
            logger.error(f"Error downloading S3 object {s3_key} from bucket {self.bucket_name}: {e}", exc_info=True)
            return None
    
    def process_grib2(self, grib2_path):
        """Process GRIB2 file and return data and timing information"""
        if not grib2_path.exists():
            logger.warning(f"GRIB file does not exist: {grib2_path}")
            return None, {'total_process_grib2_time': 0, 'errors': ['FILE_NOT_FOUND']}

        timings = {
            'pygrib_open_time': 0,
            'grb_values_time': 0,
            'total_process_grib2_time': 0,
            'errors': [] # Initialize errors list
        }
        total_start_time = time.perf_counter()

        # Determine if decompression is needed and set up paths
        is_gzipped = grib2_path.name.endswith(".gz")
        uncompressed_grib_path = None
        grib_path_to_open = grib2_path

        if is_gzipped:
            # Create a unique path for the uncompressed file in the same directory to avoid race conditions
            pid = os.getpid()
            uncompressed_grib_path = grib2_path.with_name(f"{grib2_path.stem}.{pid}")
            logger.debug(f"Decompressing {grib2_path} to {uncompressed_grib_path}")
            try:
                with gzip.open(grib2_path, 'rb') as f_in:
                    with open(uncompressed_grib_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                grib_path_to_open = uncompressed_grib_path
                logger.debug(f"Successfully decompressed to {uncompressed_grib_path}")
            except Exception as e_decompress:
                logger.error(f"Error decompressing GRIB2 file {grib2_path}: {e_decompress}", exc_info=True)
                timings['errors'].append(f'DECOMPRESSION_ERROR: {e_decompress}')
                timings['total_process_grib2_time'] = time.perf_counter() - total_start_time
                # Clean up potentially partially written uncompressed file
                if uncompressed_grib_path and uncompressed_grib_path.exists():
                    try:
                        uncompressed_grib_path.unlink()
                    except Exception as e_cleanup:
                        logger.error(f"Error cleaning up temp file {uncompressed_grib_path} after decompression error: {e_cleanup}")
                return None, timings
            
        grbs = None # Define grbs here to ensure it's available in finally if pygrib.open fails
        try:
            pygrib_open_start_time = time.perf_counter()
            logger.debug(f"Attempting to open GRIB file for pygrib: {grib_path_to_open} (original: {grib2_path}, size: {grib_path_to_open.stat().st_size if grib_path_to_open.exists() else 'N/A'})")
            grbs = pygrib.open(str(grib_path_to_open)) 
            timings['pygrib_open_time'] = time.perf_counter() - pygrib_open_start_time
            
            messages_count = 0
            try:
                # Iterate to count messages safely, pygrib.open might return an empty handle for non-grib files
                for _ in grbs:
                    messages_count += 1
                grbs.seek(0) # Reset iterator
            except Exception as e_count:
                logger.warning(f"Could not count messages in {grib_path_to_open}: {e_count}")
                # Continue, grbs[1] might still work or fail revealing the issue

            logger.debug(f"Successfully opened GRIB file: {grib_path_to_open}. Messages found: {messages_count}")

            if messages_count == 0 :
                logger.error(f"No GRIB messages found in file: {grib_path_to_open}")
                timings['errors'].append('NO_GRIB_MESSAGES_FOUND')
                timings['total_process_grib2_time'] = time.perf_counter() - total_start_time
                return None, timings
            
            # For PrecipRate, we expect one message
            try:
                grb = grbs[1]
            except IndexError:
                logger.error(f"GRIB file {grib_path_to_open} is valid but contains no messages (or is not a GRIB file).")
                timings['errors'].append('NO_GRIB_MESSAGES_FOUND')
                timings['total_process_grib2_time'] = time.perf_counter() - total_start_time
                return None, timings
            
            parameter_name = grb.shortName if grb else 'None'
            if grb and (parameter_name == 'unknown' or parameter_name is None): # Check for None as well
                try:
                    key = (grb.discipline, grb.parameterCategory, grb.parameterNumber)
                    # Updated for PrecipRate product (discipline=209, category=6, number=1)
                    if key == (209, 6, 1):
                        looked_up_name = "PrecipRate"
                    else:
                        looked_up_name = None # Keep behavior for other unknown parameters

                    if looked_up_name:
                        parameter_name = looked_up_name
                    else:
                        # If lookup fails, retain 'unknown' or a more specific message if needed
                        parameter_name = f"unknown (D{grb.discipline}C{grb.parameterCategory}P{grb.parameterNumber})"
                except AttributeError: 
                    # In case discipline, parameterCategory, parameterNumber attributes are missing
                    parameter_name = 'unknown (lookup attributes missing)'
                except Exception as e_lookup:
                    logger.warning(f"Error during GRIB parameter name lookup: {e_lookup}")
                    parameter_name = 'unknown (lookup exception)'

            logger.debug(f"Accessed GRIB message 1: {parameter_name}")

            grb_values_start_time = time.perf_counter()
            logger.debug(f"Attempting to get values from GRIB message in {grib_path_to_open}")
            data = grb.values
            
            # Convert from mm/hr to mm/2-min
            if data is not None:
                # The raw PrecipRate data is in mm/hr. We need to convert it to a 2-minute accumulation.
                data = (data / 60.0) * 2.0
            
            timings['grb_values_time'] = time.perf_counter() - grb_values_start_time
            logger.debug(f"Successfully got values and converted to 2-min accumulation. Data shape: {data.shape if data is not None else 'None'}")

            timings['total_process_grib2_time'] = time.perf_counter() - total_start_time
            return {
                'data': data,
                'valid_time': grb.validDate,
                'grib_message': grb
            }, timings
            
        except Exception as e:
            logger.error(f"Error processing GRIB2 file {grib_path_to_open} (original: {grib2_path}): {e}", exc_info=True) # Log with traceback
            timings['errors'].append(f'EXCEPTION_IN_PROCESS_GRIB2: {e}')
            timings['total_process_grib2_time'] = time.perf_counter() - total_start_time
            return None, timings # Return timings even on error
        finally:
            if grbs: # Ensure grbs was successfully opened before trying to close
                try:
                    grbs.close()
                    logger.debug(f"Closed pygrib object for {grib_path_to_open}")
                except Exception as e_close:
                    logger.error(f"Error closing pygrib object for {grib_path_to_open}: {e_close}")
            
            # Clean up the uncompressed file if it was created
            if uncompressed_grib_path and uncompressed_grib_path.exists():
                try:
                    uncompressed_grib_path.unlink()
                    logger.debug(f"Cleaned up temporary uncompressed file: {uncompressed_grib_path}")
                except Exception as e_cleanup:
                    logger.error(f"Error cleaning up temporary uncompressed file {uncompressed_grib_path}: {e_cleanup}")
    
    def cleanup_processed_files(self):
        """Remove all processed GRIB2 files"""
        for file_path in self.processed_files:
            try:
                if file_path.exists():
                    file_path.unlink()
                    logger.info(f"Cleaned up processed file: {file_path}")
            except Exception as e:
                logger.error(f"Error cleaning up processed file {file_path}: {e}", exc_info=True)
        self.processed_files.clear()  # Reset the set after cleanup

    def extract_grid_definition(self, grib2_path):
        """
        Extracts grid definition parameters from a GRIB2 file.
        Returns a dictionary with parameters like latOfFirstGridPointInDegrees, etc.,
        or None if extraction fails.
        """
        if not grib2_path.exists():
            logger.warning(f"GRIB file does not exist for grid definition extraction: {grib2_path}")
            return None

        is_gzipped = grib2_path.name.endswith(".gz")
        uncompressed_grib_path = None
        grib_path_to_open = grib2_path
        grid_params = {}

        if is_gzipped:
            pid = os.getpid()
            uncompressed_grib_path = grib2_path.with_name(f"{grib2_path.stem}.{pid}")
            logger.debug(f"Decompressing {grib2_path} to {uncompressed_grib_path} for grid definition.")
            try:
                with gzip.open(grib2_path, 'rb') as f_in, open(uncompressed_grib_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                grib_path_to_open = uncompressed_grib_path
            except Exception as e_decompress:
                logger.error(f"Error decompressing GRIB2 file {grib2_path} for grid definition: {e_decompress}", exc_info=True)
                if uncompressed_grib_path and uncompressed_grib_path.exists():
                    try:
                        uncompressed_grib_path.unlink()
                    except Exception as e_cleanup:
                        logger.error(f"Error cleaning up temp file {uncompressed_grib_path} after grid def decompression error: {e_cleanup}")
                return None
        
        grbs = None
        try:
            logger.debug(f"Attempting to open GRIB file for grid definition: {grib_path_to_open}")
            grbs = pygrib.open(str(grib_path_to_open))
            
            messages_count = 0
            for _ in grbs: # Iterate to count messages
                messages_count += 1
            grbs.seek(0)

            if messages_count == 0:
                logger.error(f"No GRIB messages found in file for grid definition: {grib_path_to_open}")
                if grbs: # Close if opened
                    grbs.close()
                if uncompressed_grib_path and uncompressed_grib_path.exists(): # Cleanup
                    uncompressed_grib_path.unlink()
                return None
            
            grb = grbs[1] # Use the first message for grid definition

            # Extract necessary GDS parameters
            # Basic parameters for constructing 1D grid arrays
            grid_params['latOfFirstGridPointInDegrees'] = grb.latitudeOfFirstGridPointInDegrees
            grid_params['lonOfFirstGridPointInDegrees'] = grb.longitudeOfFirstGridPointInDegrees
            grid_params['Ni'] = grb.Ni # Number of points along i-axis (longitude)
            grid_params['Nj'] = grb.Nj # Number of points along j-axis (latitude)
            grid_params['iDirectionIncrementInDegrees'] = grb.iDirectionIncrementInDegrees
            grid_params['jDirectionIncrementInDegrees'] = grb.jDirectionIncrementInDegrees
            
            # Scanning mode flags are crucial
            # jScansPositively = 0 (North to South), 1 (South to North)
            # iScansNegatively = 0 (West to East), 1 (East to West) - typically 0 for MRMS
            grid_params['jScansPositively'] = grb.jScansPositively 
            # To confirm if iScansNegatively is needed; MRMS is usually West to East (0)
            # grid_params['iScansNegatively'] = grb.iScansNegatively # If available and needed

            # Optionally, add more parameters if your grid construction logic needs them
            # E.g., lonOfLastGridPointInDegrees, latOfLastGridPointInDegrees
            # For GDT 0 (Lat/Lon grid), these are usually sufficient with Ni, Nj, and increments

            logger.info(f"Successfully extracted grid definition from {grib_path_to_open}: {grid_params}")
            return grid_params

        except Exception as e:
            logger.error(f"Error extracting grid definition from {grib_path_to_open}: {e}", exc_info=True)
            return None
        finally:
            if grbs:
                try:
                    grbs.close()
                except Exception as e_close:
                    logger.error(f"Error closing pygrib object for grid definition file {grib_path_to_open}: {e_close}")
            if uncompressed_grib_path and uncompressed_grib_path.exists():
                try:
                    uncompressed_grib_path.unlink()
                    logger.debug(f"Cleaned up temporary uncompressed file used for grid definition: {uncompressed_grib_path}")
                except Exception as e_cleanup:
                    logger.error(f"Error cleaning up temporary uncompressed grid def file {uncompressed_grib_path}: {e_cleanup}")

if __name__ == "__main__":
    # Example usage for direct execution and testing of GRIB2Processor
    
    # 1. Setup
    # Basic logging for standalone script execution
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    processor = GRIB2Processor()
    
    # 2. Define a time and convert to UTC
    # Example time for PrecipRate
    time_to_process = datetime(2024, 6, 1, 12, 0, tzinfo=pytz.utc) 
    
    # 3. Download and process the GRIB2 file
    print(f"Processing data for UTC time: {time_to_process}")
    
    grib2_file_path = processor.download_grib2(time_to_process)
    
    result = None
    if grib2_file_path:
        processor.processed_files.add(grib2_file_path) # Manually track for cleanup
        data_dict, _ = processor.process_grib2(grib2_file_path)
        result = data_dict # Keep the name 'result' for consistency with print logic
        
    # 4. Print results and cleanup
    if result:
        print(f"Successfully processed data for {time_to_process}")
        print(f"  Data shape: {result['data'].shape}")
        print(f"  Valid time: {result['valid_time']}")
        # Check a non-zero value if possible
        non_zero_values = result['data'][result['data'] > 0]
        if len(non_zero_values) > 0:
            print(f"  Example non-zero 2-min accumulation value: {non_zero_values[0]}")
    else:
        print(f"Failed to process data for {time_to_process}")

    # It's good practice to clean up downloaded files when testing
    processor.cleanup_processed_files()