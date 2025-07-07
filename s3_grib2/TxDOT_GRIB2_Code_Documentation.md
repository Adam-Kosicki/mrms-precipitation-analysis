---
layout: default
title: MRMS-CRIS Records Mapping System - GRIB2 Pipeline Technical Documentation
---

# MRMS-CRIS Records Mapping System: GRIB2 Pipeline Technical Documentation

## Table of Contents
- [Introduction & Project Overview](#introduction--project-overview)
  - [Scope](#scope)
  - [Executive Summary](#executive-summary)
- [1. Data Profile & Source Capabilities](#1-data-profile--source-capabilities)
  - [1.1 GRIB2 Data Characteristics](#11-grib2-data-characteristics)
  - [1.2 NOAA S3 Bucket Source Capabilities](#12-noaa-s3-bucket-source-capabilities)
- [2. CTR's Data Acquisition & Processing Methodology](#2-ctrs-data-acquisition--processing-methodology)
  - [2.1 System Architecture & Technology Stack](#21-system-architecture--technology-stack)
  - [2.2 Data Acquisition Process](#22-data-acquisition-process)
  - [2.3 Processing Workflow & Component Interaction](#23-processing-workflow--component-interaction)
  - [2.4 Modular System Components & Codebase](#24-modular-system-components--codebase)
  - [2.5 Critical Algorithms & Technical Implementation](#25-critical-algorithms--technical-implementation)
  - [2.6 Performance & Resource Optimization](#26-performance--resource-optimization)
  - [2.7 Quality Assurance & Quality Control](#27-quality-assurance--quality-control)
  - [2.8 Final Data Analysis & Variable Calculation](#28-final-data-analysis--variable-calculation)
- [3. Architectural Findings and Future Enhancements](#3-architectural-findings-and-future-enhancements)
  - [3.1 Architectural Considerations for Production Environments](#31-architectural-considerations-for-production-environments)
  - [3.2 Investigation of `wgrib2` Utility](#32-investigation-of-wgrib2-utility)
  - [3.3 Investigation of Bilinear Interpolation](#33-investigation-of-bilinear-interpolation)
- [4. Project Documentation and Resources](#4-project-documentation-and-resources)
- [5. Contact Information](#5-contact-information)
- [Appendix](#appendix)

## Introduction & Project Overview

### Scope
This document provides a comprehensive technical overview of the current-generation data processing pipeline that links Texas Department of Transportation (TxDOT) CRIS (Crash Records Information System) incident data with NOAA MRMS (Multi-Radar Multi-Sensor) weather data. The documentation details the workflow from data acquisition directly from NOAA's public data repository to the final processing and storage of enriched crash records. The focus is on the system's architecture, data flow, performance benchmarks, and the technical implementation that ensures efficient and reliable operation.

### Executive Summary

**Overview:**
The MRMS-CRIS Records Mapping System has been re-engineered to directly process high-resolution GRIB2 (GRIdded Binary) weather data, creating enriched datasets for detailed traffic safety analysis. The automated, scalable workflow is built on Python and PostgreSQL and extracts key weather metrics at the specific time and location of each incident.

**Key Findings from System Upgrade:**
The research has transitioned the system from a third-party NetCDF service to a direct GRIB2 file processing workflow. This architectural upgrade has yielded significant performance and data-quality improvements. Empirical testing of the new system against the legacy pipeline demonstrates transformational enhancements:
-   **4x Increase in Throughput:** The new system processes crash records four times faster than the previous version.
-   **Elimination of Supercomputing Dependency:** The pipeline operates on standard server hardware or high-performance workstations, removing the dependency on specialized supercomputing resources. This change improves accessibility, scalability, and cost-effectiveness.
-   **Higher-Fidelity Data Source:** By processing data directly from the authoritative NOAA MRMS source, the system utilizes more precise rainfall data. This allows for the detection of trace precipitation amounts that were previously unavailable through the third-party data provider.

**Architectural Path for Production:**
An investigation into production deployment models has been conducted. A functional, event-driven proof-of-concept has been successfully built and verified on an AWS EC2 instance, demonstrating that the most complex technical hurdles—installing specialized weather software (`wgrib2`) and automating data retrieval via event notifications—are solved. This points to a hybrid architectural pattern, a standard industry practice, where specialized data decoding is performed on a flexible compute service (like EC2) and the resulting structured data is handed off to a data platform (like Snowflake) for long-term storage and analytics. This represents a low-risk, high-reward strategy for a production environment.

**Conclusion:**
This advanced, flexible system delivers actionable weather insights correlated with traffic incidents efficiently. The research findings confirm that the new GRIB2-based pipeline is a significant technological step forward, paving the way for more detailed, data-driven transportation safety decisions across Texas.

---

## 1. Data Profile & Source Capabilities

### 1.1 GRIB2 Data Characteristics

- **Temporal Attributes**:
    - **Resolution**: 2-minute intervals for the `PrecipRate` product.
    - **Time zones**: Source data is in UTC. The pipeline standardizes all incident timestamps to UTC for processing.
    - **Coverage**: The system processes one hour of weather data (30 distinct 2-minute files) for the period preceding each traffic incident.
- **Spatial Attributes**:
    - **Geographic coverage**: The MRMS grid covers the entire Continental United States (CONUS).
    - **Spatial Resolution**: The `PrecipRate` grid has a resolution of 0.01° x 0.01°, which is approximately 1km x 1km.
    - **Spatial metadata**: The system uses latitude and longitude for all spatial calculations, mapping crash locations to the high-resolution GRIB2 grid. The grid dimensions are 7000x3500 points.
- **Data Format & Storage Needs**:
    - **File format**: The raw weather data is in GRIB2 format, compressed with GZIP (`.grib2.gz`).
    - **Data Variables**: The `PrecipRate` product contains a 2D array of precipitation rate values in millimeters per hour (mm/hr).
    - **File Size**: Individual gzipped GRIB2 files average approximately 0.56 MB.

### 1.2 NOAA S3 Bucket Source Capabilities

- **Source**: The primary weather data is sourced directly from the public `noaa-mrms-pds` Amazon S3 bucket.
- **Data download process**: The system uses asynchronous HTTP requests to download files concurrently, maximizing network throughput.
- **Data Availability**: A new `PrecipRate` file is generated and made available every 2 minutes. The S3 bucket provides high availability and is designed for massive-scale parallel access.
- **Transformation Requirements**: The system is responsible for converting the downloaded precipitation rate (mm/hr) into a 2-minute accumulation value for analysis. 

```python
# Source: preciprate/code/grib2_processor.py
# In GRIB2Processor.process_grib2 method

# The raw PrecipRate data is in mm/hr. We need to convert it to a 2-minute accumulation.
data = (data / 60.0) * 2.0
```

---

## 2. CTR's Data Acquisition & Processing Methodology

### 2.1 System Architecture & Technology Stack

- **Programming Language**: Python 3
- **Database**: PostgreSQL
- **Key Libraries**:
  - `asyncio`: For managing concurrent operations.
  - `aiobotocore`: An asynchronous AWS SDK client for efficient S3 downloads.
  - `pygrib`: A Python interface to the ECMWF GRIB API for reading GRIB2 files.
  - `numpy`: For high-performance numerical and spatial calculations.
  - `asyncpg`/`psycopg2`: For asynchronous and synchronous database connection pooling and operations.
  - `pytz` & `dateutil`: For robust timezone handling.

### 2.2 Data Acquisition Process
The data acquisition is orchestrated by the main processing script, which begins by fetching batches of unprocessed records from the local CRIS database.
1.  **Grouping by Time:** The script identifies all unique 2-minute timestamps for which weather data is needed.
2.  **Concurrent Downloading:** For each unique timestamp, the system constructs the precise GRIB2 filename (e.g., `MRMS_PrecipRate_00.00_YYYYMMDD-HHMMSS.grib2.gz`).
3.  **S3 Integration:** It then uses `aiobotocore` to download all required files from the `noaa-mrms-pds` S3 bucket concurrently. This non-blocking approach allows hundreds of files to be downloaded in parallel, maximizing network efficiency.
4.  **Error Handling:** The download process includes a retry mechanism with a linear backoff strategy to robustly handle transient network errors or S3 API throttling.

### 2.3 Processing Workflow & Component Interaction
The end-to-end workflow transforms a raw crash record into one enriched with a full hour of high-resolution weather data.

1.  **Entry Point & Setup**: The main script (`main.py`) initializes database connection pools and launches the primary asynchronous processing loop (`main_processing.py`).
2.  **Table Pre-population**: A preparatory script (`generate_table.py`) creates the target database table and pre-populates it with 30 rows for each crash incident—one for each 2-minute interval in the hour prior to the crash. These rows are flagged as unprocessed.
3.  **Acquiring Work**: The main loop queries the database for a batch of unprocessed records, grouping them by the GRIB2 file (`mrms_timestamp`) they require.
4.  **GRIB2 Acquisition**: The system concurrently downloads the necessary GRIB2 files from NOAA's S3 bucket.
5.  **Core Data Extraction**: The downloaded GRIB2 files and their associated incident lists are distributed to a pool of CPU-bound worker processes. Each worker parses a GRIB2 file, extracts the 2D precipitation data array, and matches the data to every relevant crash location using a nearest-neighbor search.
6.  **Database Update**: The extracted weather data is returned to the main process and then inserted into the database using a highly efficient bulk `UPDATE FROM` strategy, which marks the records as processed.
7.  **Final Analysis**: After the raw data is populated, a final script (`data_analysis.py`) is run. It calculates high-level analytical variables (e.g., `rain_status`, `rain_intensity_hourly`) for each crash and updates the database.

### 2.4 Modular System Components & Codebase
The system is composed of several key Python scripts that handle distinct parts of the workflow.
-   **`preciprate/code/main.py`**: The main entry point that orchestrates the entire pipeline.
-   **`preciprate/code/main_processing.py`**: Contains the core asynchronous logic for acquiring, downloading, processing, and storing data.
-   **`preciprate/code/generate_table.py`**: The standalone script for preparing the database with target records.
-   **`preciprate/code/grib2_processor.py`**: A dedicated module for all interactions with GRIB2 files, including grid extraction and data parsing using `pygrib`.
-   **`preciprate/code/db_operations.py`**: Manages all database interactions, including efficient data retrieval and bulk updates.
-   **`preciprate/code/data_analysis.py`**: The final script that calculates and stores the analytical rain variables.

### 2.5 Critical Algorithms & Technical Implementation

-   **Spatial Mapping Algorithm (Nearest Neighbor)**: The system uses a highly efficient, vectorized nearest-neighbor algorithm. It leverages `numpy.searchsorted`, a binary search function, to find the nearest grid coordinates for an entire batch of incident locations simultaneously. This avoids slow, iterative loops and is a key to the system's high performance.

```python
# Source: preciprate/code/main_processing.py
def vectorized_nearest_indices(grid, values):
    """Finds nearest neighbor indices for values in a sorted grid."""
    values_arr = np.asarray(values)
    # ... handles ascending/descending grids ...
    indices = np.searchsorted(grid, values_arr, side='left')

    # Clip indices to be within bounds and check neighbor
    indices = np.clip(indices, 0, len(grid) - 1)
    left_indices = np.clip(indices - 1, 0, len(grid) - 1)
    
    dist_to_indices = np.abs(values_arr - grid[indices])
    dist_to_left = np.abs(values_arr - grid[left_indices])
    
    return np.where(dist_to_left <= dist_to_indices, left_indices, indices)
```

-   **Timestamp Generation and Alignment**: Incident timestamps are rounded to the previous even two-minute mark to ensure perfect alignment with the MRMS data's publication schedule. All timestamps are handled in UTC to prevent timezone-related errors.

```python
# Source: preciprate/code/timestamps.py
def round_to_previous_even_minute(dt):
    minute = dt.minute
    if minute % 2 != 0:
        minute -= 1
    return dt.replace(minute=minute, second=0, microsecond=0)
```

-   **Database Bulk Updates**: For persisting data, the system uses a `copy_records_to_table` and `UPDATE ... FROM` strategy. All processed data for a batch is first bulk-loaded into a temporary table and then updated into the main table in a single, efficient transaction. This is significantly more performant than thousands of individual `UPDATE` statements.

```sql
-- Source: preciprate/code/db_operations.py
-- Simplified SQL from async_insert_rows function
UPDATE preciprate_to_cris_60min_statewide AS target
SET
    mrms2_lat = temp.mrms2_lat,
    mrms2_lon = temp.mrms2_lon,
    data_value = temp.data_value,
    data_source = temp.data_source,
    -- ... other columns ...
FROM temp_insert_batch AS temp -- In code, table name is dynamically generated
WHERE target.incident_id = temp.incident_id 
  AND target.mrms_timestamp = temp.mrms_timestamp;
```

### 2.6 Performance & Resource Optimization

The following metrics were aggregated from a complete pipeline run processing 22,000 CRIS records. The data demonstrates a highly efficient architecture.

- **High-Level Performance Summary**:
    - **Total Records Processed**: 22,000
    - **Total Script Elapsed Time**: 61.37 seconds
    - **Overall Average Throughput**: **~21,510 records/minute**
- **Detailed Batch Performance Breakdown** (Average per 1,000-record batch):
    - **Database Record Retrieval**: 0.27 seconds (9.6%)
    - **S3 GRIB2 File Downloading**: 0.51 seconds (18.2%)
    - **CPU-Bound GRIB2 Processing**: 1.64 seconds (58.6%)
    - **Database Record Insertion**: 0.22 seconds (7.9%)
- **Observations**:
    - The most time-intensive operation is the CPU-bound GRIB2 processing.
    - Database and network I/O operations are highly optimized and do not represent a bottleneck.

### 2.7 Quality Assurance & Quality Control

- **Data Validation**: The system validates that downloaded files are well-formed GRIB2 files. If an error is detected (e.g., an HTML error page from S3), it is logged, and the corresponding records are marked appropriately.
- **Error Handling & Recovery**:
    - **API/S3 Requests**: The system implements a linear backoff retry mechanism for all S3 download requests to handle transient network issues or API throttling.
    - **Database Operations**: All database writing is performed within transactions. The use of `ON CONFLICT (...) DO NOTHING` in the `generate_table.py` script ensures that re-running the script does not create duplicate records.
- **Monitoring & Metrics**: The pipeline logs detailed performance metrics for every batch, including timings for each stage, file download speeds, and error counts into a timestamped metrics file.

### 2.8 Final Data Analysis & Variable Calculation
After the raw 2-minute precipitation data is populated, the `data_analysis.py` script calculates the final analytical variables for each crash.
1.  **Data Loading & Imputation**: The script loads all 30 data points for each incident. To ensure a continuous time series, it uses forward-fill and backward-fill logic to handle any intervals where a GRIB2 file may have been missing.
2.  **Variable Calculation**: For each incident, it calculates:
    -   `cumulative_rain_calc`: The sum of precipitation over the full hour.
    -   `rain_intensity_hourly`: A human-readable classification (e.g., "Light Rain", "Heavy Rain") based on the cumulative total.
    -   `rain_status`: A boolean indicating if rain was occurring at the exact time of the crash.
    -   `minutes_since_rain_started` / `minutes_since_rain_stopped`: The time duration to or from the last precipitation event relative to the crash time.

```python
# Source: preciprate/code/data_analysis.py
def classify_rain_intensity(cumulative_precipitation_mm):
    """Classifies hourly rainfall intensity based on cumulative precipitation."""
    if cumulative_precipitation_mm == 0:
        return "No Rainfall"
    elif 0 < cumulative_precipitation_mm < 0.25:
        return "Trace Amount"
    elif 0.25 <= cumulative_precipitation_mm <= 2.50:
        return "Light Rain"
    elif 2.50 < cumulative_precipitation_mm <= 7.60:
        return "Moderate Rain"
    elif cumulative_precipitation_mm > 7.60:
        return "Heavy Rain"
    else:
        return "Undefined"
```
3.  **Final Update**: These calculated variables are then bulk-updated into the final columns of the database table using `psycopg2.extras.execute_values`.

---

## 3. Architectural Findings and Future Enhancements
This section details architectural patterns investigated for production environments and potential enhancements to the processing methodology that were researched.

### 3.1 Architectural Considerations for Production Environments
A key finding of this research is that the GRIB2 processing workflow is fundamentally a **complex dependency management problem**, not a "Big Data" problem. The core challenge is the need to install specialized, compiled C and Fortran software libraries (e.g., `eccodes` for `pygrib`, or `wgrib2` itself). This requirement dictates the suitability of different cloud platforms.

- **Platform Capabilities Analysis**:
    - **Snowflake**: A "fully native" Snowflake architecture for this workflow is **not feasible** using standard Snowpark functions. The secure, sandboxed nature of the Snowpark environment, while excellent for security and manageability, prohibits the installation of the required custom-compiled libraries. The `pygrib` library, for example, depends on the `eccodes` C-library from the ECMWF, which cannot be reliably installed in the Snowpark sandbox ([see pygrib installation docs](https://jswhit.github.io/pygrib/installing.html)). Furthermore, Snowpark UDFs cannot execute external command-line binaries like `wgrib2` ([see Snowpark limitations](https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-limitations)).
    - **Amazon EC2**: As an Infrastructure-as-a-Service (IaaS) offering, EC2 provides complete control over the operating system. This allows for the installation of the necessary compilers (`gcc`, `gfortran`) and scientific libraries required to compile `wgrib2` ([see wgrib2 compile guide](https.www.cpc.ncep.noaa.gov/products/wesley/wgrib2/compile_questions.html)) and reliably install `pygrib`.

- **Feasibility of Migrating the Current Workflow to Native Snowflake**:
    - The existing pipeline, composed of the scripts `main.py`, `main_processing.py`, `grib2_processor.py`, and others, represents a self-contained Python application. A direct migration of this logic into a standard Snowpark environment faces several obstacles.
    - **Challenge 1: GRIB2 Decoding.** The core logic in `grib2_processor.py` relies on `pygrib`. As noted, standard Snowpark Python UDFs cannot support this library's compiled dependencies.
    - **Challenge 2: Workflow Orchestration.** The logic in `main_processing.py`—which manages fetching records, orchestrating concurrent downloads, and distributing work to multiple processes—would need a significant rewrite. While Snowflake provides tools like [Tasks and Streams](https://docs.snowflake.com/en/developer-guide/snowflake-python-api/snowflake-python-overview) for orchestration, it is not a direct "lift-and-shift" of the existing Python application logic. The logic would have to be re-architected to fit Snowflake's model.
    - **Challenge 3: Data Analysis.** While the final analysis in `data_analysis.py` could be rewritten as a [SQL UDF or stored procedure](https://docs.snowflake.com/en/developer-guide/udf/sql/udf-sql-introduction), this can only happen *after* the initial GRIB2 decoding has successfully produced structured data.

- **Alternative Path: Snowpark Container Services**:
    - A potential path for running this workflow natively in Snowflake is [Snowpark Container Services](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview). This feature allows for running custom OCI-compliant container images within the Snowflake environment.
    - This approach would involve building a Docker container with the Python code, `pygrib`, `wgrib2`, and all the underlying C/Fortran dependencies. While this overcomes the dependency issue, it shifts the complexity from running a Python script on a server to building, maintaining, and securing a custom scientific computing container image specifically for Snowflake. This is a considerable engineering task and differs significantly from the simplicity of a standard serverless function.

- **Recommended Hybrid Architectural Pattern**: The findings strongly recommend a **hybrid cloud architecture**. In this model, a flexible compute service like EC2 (or a container-based service like AWS Fargate) is used for the specialized task of decoding GRIB2 files into a structured format (like Parquet or CSV). The processed, structured data is then loaded into a data platform like Snowflake, which is used for its strengths in long-term storage, governance, and high-performance analytics. This pattern is endorsed by cloud data platforms, which provide mechanisms like External Functions to integrate with specialized compute running outside their environment ([see Snowflake External Functions on AWS](https://quickstarts.snowflake.com/guide/getting_started_external_functions_aws/index.html?index=..%2F..index)).

### 3.2 Investigation of `wgrib2` Utility
`wgrib2` is a highly-optimized, command-line utility developed by NOAA and is considered the industry standard for many operational GRIB2 processing tasks.
-   **Performance**: For querying the value at a single latitude/longitude point, `wgrib2` is generally faster than `pygrib` because it can extract the point without loading the entire GRIB data field into memory.
-   **Usage Model Trade-offs**: The current pipeline's architecture is built around `pygrib` providing an entire data field as a single NumPy array. This is ideal for the vectorized, batch-based approach where values for thousands of incidents are found at once. An architecture using `wgrib2` would likely involve calling its command-line interface for each incident's coordinate, which could introduce significant overhead from starting thousands of external processes. A future implementation could be redesigned to batch coordinates into a single `wgrib2` call to leverage its performance characteristics.

### 3.3 Investigation of Bilinear Interpolation
The pipeline currently uses a **Nearest Neighbor** search. An alternative, more advanced method is **Bilinear Interpolation**.
-   **Method**: This method identifies the four closest grid points surrounding a crash coordinate and calculates a weighted average of their precipitation values. The weighting is based on the crash's proximity to each point.
-   **Benefits**: It can provide a more accurate estimate of precipitation for locations that fall between grid cell centers and produces a smoother, more continuous output.
-   **Costs**: This method is more computationally intensive, as it requires fetching four data points instead of one and involves several additional floating-point calculations for every incident. This would increase the `cpu_processing_time` but could be a valuable future enhancement for applications requiring higher precision.

---

## 4. Project Documentation and Resources

For a complete overview of the project, including source code and related reports, please refer to the following resources.

- **GRIB2 Pipeline Source Code:**
    - **Description:** The complete Python source code for the new, high-performance GRIB2-based pipeline.
    - **Link:** [GRIB2 Pipeline Code on UT Box](https://utexas.box.com/s/wgk244e6p1ex6o00rmhywdbjfwmwlo35)
- **GRIB2 vs. NetCDF Comparative Report:**
    - **Description:** A detailed technical report that analyzes the data differences between the GRIB2 and NetCDF sources.
    - **Link:** [Comparative Analysis of MRMS Precipitation Data on GitHub](https://github.com/Adam-Kosicki/mrms-precipitation-analysis/blob/main/netcdf_grib2_report.md)
- **NOAA GRIB2 Data Access Demo:**
    - **Description:** A Jupyter Notebook providing a hands-on demonstration for accessing and downloading raw GRIB2 files from the public NOAA S3 bucket.
    - **Link:** [NOAA PrecipRate Access Demo on GitHub](https://github.com/Adam-Kosicki/mrms-precipitation-analysis/blob/main/s3_grib2/noaa_preciprate_access_demo.ipynb)
- **Previous NetCDF-Based System (Legacy):**
    - **Description:** The technical documentation for the previous-generation system that used NetCDF data.
    - **Link:** [Legacy NetCDF System Documentation on GitHub](https://github.com/Adam-Kosicki/mrms-precipitation-analysis/blob/main/TXDOT_Deliverable.md)

---

## 5. Contact Information

### For Technical Inquiries
For questions regarding the data acquisition and processing pipeline:

**Adam Kosicki**  
Engineering Scientist Associate  
Center for Transportation Research  
The University of Texas at Austin  
Email: adam.kosicki@austin.utexas.edu

### For Data Analysis Inquiries
For questions regarding data analysis, variable estimation, and analytical methodologies:

**Saddam Hossain**  
Graduate Research Assistant  
Department of Civil, Architectural, and Environmental Engineering  
The University of Texas at Austin  
Email: saddam.hossain@austin.utexas.edu

---

## Appendix

<details>
<summary>Click to expand for Performance and Scalability Analysis</summary>

### Appendix: Performance and Scalability Analysis

The following metrics were aggregated from a complete pipeline run processing 22,000 CRIS records against their corresponding MRMS timestamps. The data demonstrates a highly efficient and scalable architecture, capable of processing large datasets without significant bottlenecks.

#### High-Level Performance Summary

| Metric                        | Value                            |
| :---------------------------- | :------------------------------- |
| **Total Records Processed**   | 22,000                           |
| **Total Script Elapsed Time** | 61.37 seconds (1.02 minutes)     |
| **Overall Average Throughput**| **~21,510 records/minute**       |

This throughput indicates that the system is capable of enriching over 1.2 million records per hour under the tested conditions (Batch size: 1000, Download workers: 10, Processing workers: 5).

#### Detailed Batch Performance Breakdown

The script processes records in batches. On average, a batch of 1,000 records is completed in **~2.8 seconds**. The time spent in each stage of the pipeline is broken down as follows:

| Pipeline Stage                       | Average Time per 1,000-Record Batch | Percentage of Batch Time |
| :----------------------------------- | :---------------------------------- | :----------------------- |
| 1. Database Record Retrieval         | 0.27 seconds                        | 9.6%                     |
| 2. S3 GRIB2 File Downloading         | 0.51 seconds                        | 18.2%                    |
| 3. CPU-Bound GRIB2 Processing        | 1.64 seconds                        | 58.6%                    |
| 4. Database Record Insertion         | 0.22 seconds                        | 7.9%                     |
| **Total**                            | **~2.80 seconds**                   | **100%**                 |

**Observations:**
*   **CPU-Bound Workload:** As expected, the most time-intensive operation is the CPU-bound GRIB2 processing, where raw weather data is extracted and mapped to incident coordinates. This accounts for nearly 60% of the total time.
*   **Efficient I/O:** Both database retrieval and insertion operations are extremely fast, each taking less than 300 milliseconds per batch. This confirms that the database queries and bulk-update strategies are highly optimized and are not a system bottleneck.

#### Database Query Optimization Analysis

The metrics include the `EXPLAIN ANALYZE` output for the main record retrieval query in every batch. The query to retrieve unprocessed records is a key component of the pipeline's performance.

```sql
-- Source: preciprate/code/db_operations.py
-- Simplified SQL from get_incidents function
WITH needy_mrms_timestamps AS (
    SELECT p.mrms_timestamp
    FROM preciprate_to_cris_60min_statewide p
    WHERE p.data_source = 'grib2_prepop_target'
      AND p.data_value IS NULL
    GROUP BY p.mrms_timestamp
    ORDER BY p.mrms_timestamp ASC
    LIMIT %s -- num_distinct_mrms_timestamps
)
SELECT 
    placeholder.incident_id, 
    placeholder.incident_timestamp,
    placeholder.incident_lat, 
    placeholder.incident_lon,
    placeholder.mrms_timestamp
FROM preciprate_to_cris_60min_statewide placeholder
JOIN needy_mrms_timestamps nmt 
  ON placeholder.mrms_timestamp = nmt.mrms_timestamp
WHERE placeholder.data_source = 'grib2_prepop_target'
ORDER BY placeholder.mrms_timestamp ASC, placeholder.incident_id ASC
LIMIT %s; -- batch_size
```

*   **Consistent Index Usage:** Across all batches, the query planner consistently and correctly used the `idx_preciprate_cris_mrms_timestamp` index and other relevant indexes. This is critical for performance, as it allows the database to instantly locate the required unprocessed rows without scanning the entire table.
*   **Low Execution Time:** The actual database execution time for fetching 1,000 records to process was consistently between **26-71 milliseconds**. This proves the database is performing efficiently under load.

#### GRIB2 File Handling Metrics

The pipeline's performance is also dependent on its ability to handle the GRIB2 files efficiently.

*   **File Download Speed:** The average download speed for GRIB2 files from the `noaa-mrms-pds` S3 bucket was **~19.2 Mbps**.
*   **File Processing Speed:** Once downloaded, each GRIB2 file was processed by a worker in an average of **0.159 seconds**. This includes GZIP decompression and the extraction of precipitation values for all relevant incidents within that file's two-minute window.
*   **File Size:** The gzipped GRIB2 files had an average size of **0.56 MB**.
*   **Error Rate:** Across the entire run of 22,000 records, there were **zero** errors reported for S3 downloads, file decompression, or GRIB2 message processing.

</details>

<details>
<summary>Click to expand for GRIB2 Data Dictionary</summary>

### Appendix: GRIB2 Data Dictionary (PrecipRate)

| Discipline | Category | Parameter | Name       | Frequency | Unit  | Missing | Range Folded | No Coverage | Description                | Notes |
| :--------- | :------- | :-------- | :--------- | :-------- | :---- | :------ | :----------- | :---------- | :------------------------- | :---- |
| 209        | 6        | 1         | PrecipRate | 2-min     | mm/hr | -1      | n/a          | -3          | Radar Precipitation Rate |       |

</details>

<details>
<summary>Click to expand for GRIB2 File Internals (Sample)</summary>

### Appendix: Human-Readable Summary of GRIB2 File Metadata

This table provides a simplified view of the most important metadata fields from a sample `PrecipRate` GRIB2 file.

| Category              | Field                                  | Value                                         | Description                                           |
| :-------------------- | :------------------------------------- | :-------------------------------------------- | :---------------------------------------------------- |
| **Identification**    | `discipline`                           | 209 (Local Use)                               | The high-level data category.                         |
|                       | `centre`                               | 161 (US NOAA/OAR)                             | The originating center of the data.                   |
|                       | `shortName`                            | `unknown`                                     | A short name for the parameter (often `prate`).       |
| **Time**              | `dataDate`                             | `20240601`                                    | The UTC date of the data.                             |
|                       | `dataTime`                             | `1122`                                        | The UTC time (HHMM) of the data.                      |
|                       | `stepType`                             | `instant`                                     | The data represents an instantaneous value.           |
| **Grid Definition**   | `gridType`                             | `regular_ll`                                  | A regular latitude/longitude grid.                    |
|                       | `Ni` x `Nj`                            | 7000 x 3500                                   | Number of points along longitude and latitude.        |
|                       | `iDirectionIncrementInDegrees`         | 0.01°                                         | Spacing between points along longitude.               |
|                       | `jDirectionIncrementInDegrees`         | 0.01°                                         | Spacing between points along latitude.                |
|                       | `latitudeOfFirstGridPointInDegrees`    | 54.995°                                       | Latitude of the grid's starting corner.               |
|                       | `longitudeOfFirstGridPointInDegrees`   | 230.005°                                      | Longitude of the grid's starting corner.              |
| **Data Representation** | `dataRepresentationTemplateNumber`     | 41 (PNG Compression)                          | The method used to pack the data values.              |
|                       | `bitsPerValue`                         | 16                                            | The number of bits used for each data value.          |
|                       | `missingValue`                         | 9999                                          | The value used to indicate missing data points.       |
| **Data Summary**      | `minimum`                              | -3.0                                          | Minimum value in the dataset (often indicates "no coverage"). |
|                       | `maximum`                              | 175.0                                         | Maximum value in the dataset (mm/hr).                 |
|                       | `average`                              | -0.948                                        | The average value across all grid points.             |
|                       | `standardDeviation`                    | 1.842                                         | The standard deviation of the data values.            |
|                       | `getNumberOfValues`                    | 24,500,000                                    | The total number of data points in the grid.          |

</details>

<details>
<summary>Click to expand for Data Sources, Inputs, and Outputs</summary>

### Appendix: Data Dictionary and Schema

This section details the primary data sources that serve as inputs to the pipeline and the final data structure that is produced as its output.

#### Input Data Sources

There are two primary sources of data for this pipeline.

##### 1. CRIS Crash Records (PostgreSQL Table)

This is the foundational dataset containing all crash records used as input for the pipeline.

*   **Table Name:** `public.cris_records_statewide`
*   **Description:** Contains raw crash data from the Texas DOT CRIS system. Each row represents a single crash event that is a candidate for weather analysis.
*   **Approximate Size:** ~1.1 million records.

**Key Columns:**

| Column Name            | Data Type          | Description                                                    |
| :--------------------- | :----------------- | :------------------------------------------------------------- |
| `Crash ID`             | integer            | Unique identifier for the crash.                               |
| `crash_date_timestamp` | text               | The full timestamp of the crash, including timezone.           |
| `Latitude`             | double precision   | The latitude of the crash location.                            |
| `Longitude`            | double precision   | The longitude of the crash location.                           |
| `Weather Condition`    | text               | The original weather condition recorded at the scene by officers. |

##### 2. NOAA MRMS PrecipRate (GRIB2 Files)

This is the primary weather data source, providing the high-resolution precipitation data that is the core of this project.

*   **Source:** NOAA MRMS (Multi-Radar Multi-Sensor) system.
*   **Product:** `PrecipRate` (Radar-based precipitation rate).
*   **Format:** GRIB2 files, compressed with GZIP (`.grib2.gz`).
*   **Location:** Publicly available on the `noaa-mrms-pds` Amazon S3 bucket.
*   **Update Frequency:** A new file is generated every 2 minutes, containing a nationwide snapshot.
*   **Description:** Each file contains a grid of precipitation rate values (in mm/hr) covering the continental US (CONUS) for a specific 2-minute interval. This data is downloaded on-demand by the pipeline to be matched with crash events.

#### Output Data Schema (PostgreSQL Table)

This table is the final product of the pipeline, joining CRIS crash data with the corresponding time-series weather data from MRMS.

*   **Table Name:** `public.preciprate_to_cris_60min_statewide`
*   **Description:** This table stores the processed results. For each input crash, it contains 30 rows—one for each 2-minute interval in the hour preceding the crash. It is initially pre-populated with incident and timestamp data, then updated with weather values and final analytics.
*   **Approximate Size:** ~137,000 records (and growing as new crashes are processed).

```sql
-- Source: preciprate/code/generate_table.py
CREATE TABLE IF NOT EXISTS preciprate_to_cris_60min_statewide (
    incident_id BIGINT,
    incident_timestamp TEXT,
    incident_lat DOUBLE PRECISION,
    incident_lon DOUBLE PRECISION,
    mrms2_lat DOUBLE PRECISION,
    mrms2_lon DOUBLE PRECISION,
    data_value DOUBLE PRECISION,
    mrms_timestamp TEXT,
    data_source TEXT,
    data_source_description TEXT,
    data_source_unit TEXT,
    distance_to_grid_point DOUBLE PRECISION,
    minutes_since_rain_started INTEGER,
    minutes_since_rain_stopped INTEGER,
    rain_status BOOLEAN,
    rain_intensity_hourly TEXT,
    PRIMARY KEY (incident_id, mrms_timestamp)
);
```

**Key Columns:**

| Column Name                  | Data Type          | Description                                                                    |
| :--------------------------- | :----------------- | :----------------------------------------------------------------------------- |
| `incident_id`                | bigint             | The `Crash ID` from the source `cris_records_statewide` table.                 |
| `incident_timestamp`         | text               | The original timestamp of the crash.                                           |
| `mrms_timestamp`             | text               | The specific 2-minute UTC timestamp for which the weather data was retrieved.  |
| `data_value`                 | double precision   | The measured precipitation in millimeters for the 2-minute interval.           |
| `rain_status`                | boolean            | `True` if it was raining at the exact time of the crash.                       |
| `rain_intensity_hourly`      | text               | A classification of the total rainfall in the hour (e.g., "Light Rain").       |
| `minutes_since_rain_started` | integer            | The number of minutes from when rain first started in the hour to the crash time. |
| `minutes_since_rain_stopped` | integer            | If not raining at crash time, the minutes since rain last stopped.             |
| `distance_to_grid_point`     | double precision   | The distance (in meters) from the crash to the center of the assigned grid point. |
| `data_source`                | text               | Tracks the processing state (e.g., `grib2_prepop_target`, `mrms_preciprate`). |

</details>

<details>
<summary>Click to expand for GRIB2 vs. NetCDF Pipeline Technical Comparison</summary>

### Appendix: GRIB2 vs. NetCDF Pipeline Technical Comparison

This section provides a more detailed technical breakdown of the architectural and code-level differences between the current GRIB2-based pipeline and the previous NetCDF-based implementation.

#### Architectural Differences

The fundamental change was moving from a web API-dependent architecture to a direct cloud storage access model.

*   **Previous (NetCDF) Architecture:**
    *   `CRIS DB -> Python App -> IEM Web API -> NetCDF File -> Postgres`
    - This workflow required complex, adaptive rate-limiting logic within the Python application to avoid overwhelming the Iowa Environmental Mesonet's web server.

*   **Current (GRIB2) Architecture:**
    *   `CRIS DB -> Python App -> NOAA S3 Bucket -> GRIB2 File -> Postgres`
    - This workflow is simpler and more scalable. It connects directly to a high-throughput cloud storage service (AWS S3), which is designed for massive parallel access and does not require the same delicate rate-limiting.

#### Dependency Changes

The change in data format and source necessitated a corresponding change in key Python libraries.

| Component                 | Previous System (NetCDF) | Current System (GRIB2) | Rationale for Change                                      |
| :------------------------ | :----------------------- | :--------------------- | :-------------------------------------------------------- |
| **HTTP/Data Client**      | `aiohttp`                | `aiobotocore`          | Specialized async client for AWS S3 vs. a generic web API. |
| **Meteorological Parser** | `xarray`                 | `pygrib`               | Specialized, high-performance library for the GRIB format. |

#### Processing Logic Changes

While the overall goal remained the same, a key data transformation step shifted from the external service to our internal pipeline.

*   **Unit Standardization:**
    *   In the previous system, the IEM service provided NetCDF files with a pre-calculated 2-minute precipitation accumulation (`mrms_a2m` product).
    *   In the current system, the `PrecipRate` GRIB2 product provides an instantaneous precipitation rate in `mm/hr`. The pipeline is now responsible for converting this rate into a 2-minute accumulation value (`(value / 60.0) * 2.0`) before it is used, giving us full control over data precision.

#### Performance Impact

The architectural and dependency changes led to a dramatic, order-of-magnitude improvement in performance and a reduction in hardware requirements. While the previous NetCDF system required a supercomputing environment (TACC Lonestar 6) to achieve a rate of **~175 incidents/min**, the current GRIB2 pipeline achieves **~715 incidents/min** on a standard developer laptop. This **~4x increase in throughput** on commodity hardware is a direct consequence of eliminating the API bottleneck and using more efficient, specialized libraries for data handling.

</details>

