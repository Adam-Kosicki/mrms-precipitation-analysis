# MRMS-CRIS Records Mapping System: GRIB2 Workflow Implementation and Rationale

| | |
| :--- | :--- |
| **To:** | TxDOT, including Mark Prindle, Harsha Vardhan ReddyChandapuram, Manikandan Eswaran, and Jim Markham |
| **Cc:** | Benjamin McCulloch, Bethany Wyatt |
| **From:** | The University of Texas at Austin, Center for Transportation Research (CTR) |
| | Center for Transportation Research, The University of Texas at Austin |
| **Date:** | June 26, 2025 |
| **Subject:** | **Recommendation:** Transitioning the TxDOT MRMS-CRIS Data Workflow to a GRIB2-Based System |


### Executive Summary

*   **Background:** An initial CTR analysis, detailed in the [Comparative Analysis of MRMS Precipitation Data](https://github.com/Adam-Kosicki/mrms-precipitation-analysis/blob/main/netcdf_grib2_report.md) report, has uncovered a key opportunity to improve data precision. Previously, a GRIB2-based workflow was not considered viable as it was understood that NOAA did not offer a direct 2-minute precipitation accumulation product, which the current system requires. However, our recent findings confirm that NOAA's `PrecipRate` data (in mm/hr) can be reliably converted into the necessary 2-minute accumulation. This is a crucial development because the current NetCDF provider's process involves a data conversion step that quantizes the data, filtering out real, trace amounts of precipitation. By transitioning to GRIB2, we can source data directly from NOAA, bypassing this intermediate step and preserving the full precision of the data. Following a review of these findings with TxDOT, it was determined that a transition to a GRIB2-based pipeline was warranted.
*   **Primary Recommendation: Transition to the GRIB2 Data Workflow.** The central recommendation of this document is for TxDOT to adopt the new data pipeline that sources MRMS data directly from NOAA in GRIB2 format. This transition is the **main focus** of the analysis and the code detailed in this report. The key benefits of this change are:
    *   **Superior Data Precision:** GRIB2 provides data with full floating-point precision, eliminating the data loss seen in the current NetCDF workflow and enabling more accurate detection of trace precipitation.
    *   **Direct and Reliable Data Source:** Sourcing data directly from NOAA enhances reliability and provides a clear, authoritative data lineage.
    *   **A Mature, High-Performance Workflow Ready for Handoff:** A tested GRIB2 workflow is already implemented and has been shown to process a full month of CRIS data in just 2.5 hours on a standard laptop. This high-performance codebase is ready for use and handoff to TxDOT.
*   **Secondary Effort: Enhancement to Bilinear Interpolation.** A secondary effort, which is an outgrowth of the GRIB2 transition, is the ongoing development of a more advanced **bilinear interpolation method**. This aligns our methodology with NOAA's documented best practices. While this enhancement promises even greater accuracy, it is a separate line of development that will be the subject of a future, more detailed discussion. The main priority addressed in this document is the core transition to the GRIB2 data source itself.
*   **Next Steps:** The CTR team is preparing the necessary deliverables to support this transition. Updated code and revised technical documentation will be delivered to TxDOT by the end of this week to facilitate the internal review cycle.

### 1.0 Background: The Need for Re-evaluation
*   While evaluating the MRMS data for additional use cases, CTR conducted an analysis of the existing NetCDF pipeline.
*   The analysis compared the NetCDF data (from IEM) against the raw GRIB2 data (from NOAA). The full findings are available in the [Comparative Analysis of MRMS Precipitation Data](https://github.com/Adam-Kosicki/mrms-precipitation-analysis/blob/main/netcdf_grib2_report.md) report. Key observations from this analysis include:
    *   **Observation of Data Quantization:** The analysis observed that the NetCDF data from the Iowa Environmental Mesonet (IEM) appears to undergo a lossy quantization step. It seems the original high-precision weather data is converted into an 8-bit PNG image, a process which would limit the data to 256 discrete value bins. A consequence of this process is that the smallest non-zero precipitation value that can be represented is 0.02 mm. Rainfall below this threshold may be recorded as zero, which could affect analyses where trace precipitation is a factor.
    *   **Comparison of Zero-Value Incidents:** An analysis of 400 incidents recorded as having zero precipitation in the NetCDF data revealed that 4.5% of them showed detectable trace precipitation in the corresponding raw GRIB2 data.
*   The initial NetCDF workflow, documented in the [original technical documentation](https://github.com/Adam-Kosicki/mrms-precipitation-analysis/blob/main/TXDOT_Deliverable.md), was a practical solution developed to work with a readily available data service. However, these findings highlighted an opportunity to improve data precision, leading CTR to issue this recommendation to transition from the NetCDF-based workflow to a more direct and scientifically robust approach.

### 2.0 Head-to-Head Comparison

| Attribute | GRIB2 Pipeline (Proposed) | NetCDF Pipeline (Current) | Observation |
| :--- | :--- | :--- | :--- |
| **Data Precision** | **High.** Utilizes original floating-point data from NOAA's `PrecipRate` product, which appears to preserve the full detail of weather events. | **Potentially Limited.** The data is derived from a process that seems to involve conversion to an 8-bit PNG, limiting it to 256 discrete value bins. This quantization process may result in the loss of precision, particularly for trace precipitation events. | **GRIB2 offers a pathway to higher data precision.** |
| **Data Provenance** | **Direct.** Pulls raw GRIB2 files directly from the official NOAA S3 source, providing a clear and auditable data lineage. | **Indirect.** Relies on a third-party service that re-processes the original NOAA data. | **GRIB2 provides a more direct data source.** |
| **Methodological Approach** | **Aligned with Source Guidance.** Employs **bilinear interpolation**, a method documented by NOAA for continuous fields, to estimate weather values at precise coordinates. | **Effective, but Different.** Uses a nearest neighbor approach on data that has already been processed. | **The GRIB2 approach allows for alignment with NOAA's recommended interpolation methods.** |
| **Transparency** | **High.** The data processing workflow is controlled within a single codebase, from the raw source file to the final interpolated value. | **Lower.** Relies on an external, third-party "black box" service for a key data conversion step. | **The GRIB2 workflow offers greater end-to-end transparency.** |

### 3.0 Data Provenance: GRIB2 as the Authoritative Source

CTR's analysis indicates that the NetCDF data is a derivative product, with the original, authoritative source being the GRIB2 files from NOAA. This distinction presents an opportunity to work closer to the source data.

*   **Data Lineage:** As noted in the [comparative analysis report](https://github.com/Adam-Kosicki/mrms-precipitation-analysis/blob/main/netcdf_grib2_report.md), the IEM NetCDF appears to be derived from the same GRIB2 data, rather than being an independent estimate.

*   **Guidance from the Source:** Working directly with GRIB2 data allows for leveraging guidance from the source provider, NOAA. This includes recommendations for processing steps like interpolation. This type of official, scientifically-grounded guidance is a key benefit of using the GRIB2 format and is less readily available for derivative data products.

This direct data lineage means that by transitioning to a GRIB2 workflow, TxDOT has the opportunity to align its process with the original, unmodified, and higher-precision data available, following best practices suggested by the source provider.

### 4.0 Alignment with NOAA's Recommended Methodology

The transition to GRIB2 allows for the adoption of data processing techniques that align with NOAA's documented best practices. For estimating values from continuous grid data like precipitation fields, NOAA recommends **4-point bilinear interpolation**.

The previous NetCDF workflow used a nearest-neighbor approach. While effective, it differs from the bilinear interpolation method recommended by NOAA for continuous data fields. To enhance the methodology, CTR is exploring a transition to NOAA's preferred approach.

Bilinear interpolation can provide a more nuanced estimation of precipitation at a specific point (like a crash location) by calculating a weighted average of the values from the four surrounding grid points. This method better represents the continuous nature of rainfall, which can improve the precision of the data provided to TxDOT.

In line with NOAA's guidance, CTR has developed and is currently testing a bilinear interpolation method. The current implementation with `pygrib2` is a positive step forward from the nearest-neighbor approach. We see a clear path for further optimization, and we anticipate that adopting NOAA's own `wgrib2` utility could yield substantial performance gains.

This highlights a key benefit of the GRIB2 ecosystem: it offers a clear, industry-standard roadmap for best practices. Following this path supports a methodologically sound and continuously improvable system. Such straightforward guidance is less apparent for derivative NetCDF files, which might require more complex, custom solutions to achieve similar data quality.

### 5.0 Processing Efficiency and Accessibility

A promising aspect of the GRIB2 workflow is its performance, even without specialized hardware. The initial implementation (`preciprate_nearest_neighbor`), running on a standard developer laptop, shows processing speeds comparable to the original NetCDF workflow that utilized the TACC Lonestar 6 supercomputing cluster.

*   **High Throughput:** Initial tests show the system achieves approximately **10,000 database updates per minute**.
*   **Rapid Processing:** A one-month dataset of CRIS incidents is processed in approximately **2.5 hours**, at a sustained rate of about 300 incidents per minute. This performance appears to be on par with the previous, resource-intensive workflow.
*   **No Supercomputer Required:** These performance metrics are achieved without a high-performance computing cluster. This lightweight and efficient design suggests the entire workflow could be run on a standard TxDOT server or a sufficiently powerful laptop, which would lower the barrier to entry for running, maintaining, and adapting the system.

This accessibility could reduce operational costs and complexity, empowering the TxDOT team to independently manage and execute the data processing pipeline on demand. CTR is working to implement the more computationally intensive bilinear interpolation method while aiming to preserve these high-performance characteristics.

### 6.0 Next Steps
*   **Deliverables:** The updated code and final documentation will be delivered by E.O.D. this Friday to support TxDOT's internal review.
*   **Code Access and Status:** The initial GRIB2 workflow implementation, which uses the nearest-neighbor method, is available in the private GitHub repository: [https://github.com/Adam-Kosicki/mrms_cris_records_mapping_TxDOT/tree/tacc_lonestar6](https://github.com/Adam-Kosicki/mrms_cris_records_mapping_TxDOT/tree/tacc_lonestar6). This includes all necessary scripts for data ingestion, processing, and analysis (`main.py`, `main_processing.py`, `data_analysis.py`, etc.). TxDOT staff should forward their GitHub usernames to Adam Kosicki to receive collaborator invitations.
*   **Ongoing Development:** CTR has completed the initial implementation of the bilinear interpolation method and is currently finalizing performance benchmarks. While this method is more computationally intensive, our focus is on optimizing the `wgrib2`-based implementation to deliver a solution that offers higher precision without significantly compromising the established processing efficiency. Updated performance metrics for this enhanced workflow will be shared upon completion of testing.
*   **Clarification:** The CTR team is available to answer any questions.

### Contact Information
For questions regarding the GRIB2 workflow, technical implementation, or data analysis:

**Adam Kosicki**  
Engineering Scientist Associate  
Center for Transportation Research  
The University of Texas at Austin  
Email: adam.kosicki@austin.utexas.edu

