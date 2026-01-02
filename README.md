# HLS tile revisit time estimate

Estimate the median revisit time for an MGRS tile

## Usage

### MAAP DPS

To run the algorithm via DPS, you can follow this example. Provide the S3 URI for an input spatial file using the `input_file` argument. This file will be read using `geopandas` then the records will be filtered down to the ones that intersect the HLS raster asset footprint for the selected MGRS tile.

```python
from maap.maap import MAAP

maap = MAAP(maap_host="api.maap-project.org")

jobs = []
for tile in ["14VLQ", "18WXS", "16WFB", "26WMC", "19VDL"]:
    job = maap.submitJob(
        algo_id="HLSRevisit",
        version="v0.2",
        identifier="test-run",
        queue="maap-dps-worker-16gb",
        tile=tile,
        start_date="2025-01-01", 
        end_date="2025-12-31", 
        save_dir=r"/projects/code/hls/Revisit/output", 
        search_source="earthaccess"
    )
    jobs.append(job)

```

The partitioned parquet dataset is available in my shared folder in the `maap-ops-workspace` bucket. The MAAP ADE and DPS both will have permissions to read from the archive.

The algorithm groups HLS observations by sensor (L30/S30) and date, loads all spectral bands and the Fmask layer within the MGRS tile extent, and extracts values at each point location using nearest neighbor sampling (within 20m tolerance). The output includes a STAC catalog with a Parquet file containing the extracted time series values for all requested bands (default: red, green, blue, nir_narrow, swir_1, swir_2, and Fmask).

The algorithm includes retry logic with exponential backoff to handle intermittent failures during data access.
