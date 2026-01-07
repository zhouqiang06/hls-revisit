"""Extract a values from an HLS time series for a set of points in a MGRS tile"""

import argparse
import json
import logging
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Tuple

import pandas as pd
from maap.maap import MAAP
from pystac import Asset, Catalog, CatalogType, Item
from rasterio.session import AWSSession
from rustac import DuckdbClient

import os
from pathlib import Path

import numpy as np
import pandas as pd
from datetime import datetime
# from glob import glob

import geopandas
# from osgeo import gdal
import rasterio as rio
import rioxarray as rxr
import earthaccess

import dask.array as da

from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterio.merge import merge
from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterio.enums import Resampling

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logging.getLogger("botocore").setLevel(logging.WARNING)
logger = logging.getLogger("HLSComposite")

# # GDAL configurations used to successfully access LP DAAC Cloud Assets via vsicurl
# gdal.SetConfigOption('GDAL_HTTP_COOKIEFILE','~/cookies.txt')
# gdal.SetConfigOption('GDAL_HTTP_COOKIEJAR', '~/cookies.txt')
# gdal.SetConfigOption('GDAL_DISABLE_READDIR_ON_OPEN','EMPTY_DIR')
# gdal.SetConfigOption('CPL_VSIL_CURL_ALLOWED_EXTENSIONS','TIF')
# gdal.SetConfigOption('GDAL_HTTP_UNSAFESSL', 'YES')

GDAL_CONFIG = {
    "CPL_TMPDIR": "/tmp",
    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": "TIF,GPKG,SHP,SHX,PRJ,DBF,JSON,GEOJSON",
    "GDAL_CACHEMAX": "512",
    "GDAL_INGESTED_BYTES_AT_OPEN": "32768",
    "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
    "GDAL_HTTP_MERGE_CONSECUTIVE_RANGES": "YES",
    "GDAL_HTTP_MULTIPLEX": "YES",
    "GDAL_HTTP_VERSION": "2",
    "PYTHONWARNINGS": "ignore",
    "VSI_CACHE": "TRUE",
    "VSI_CACHE_SIZE": "536870912",
    "GDAL_NUM_THREADS": "ALL_CPUS",
    # "GDAL_HTTP_COOKIEFILE": str(Path.home() / "cookies.txt"),
    # "GDAL_HTTP_COOKIEJAR": str(Path.home() / "cookies.txt"),
    # "GDAL_HTTP_UNSAFESSL": "YES",
    # "CPL_DEBUG": "ON" if debug else "OFF",
    # "CPL_CURL_VERBOSE": "YES" if debug else "NO",
}

# LPCLOUD S3 CREDENTIAL REFRESH
CREDENTIAL_REFRESH_SECONDS = 50 * 60


class CredentialManager:
    """Thread-safe credential manager for S3 access"""

    def __init__(self):
        self._lock = threading.Lock()
        self._credentials: dict[str, Any] | None = None
        self._fetch_time: float | None = None
        self._session: AWSSession | None = None

    def get_session(self) -> AWSSession:
        """Get current session, refreshing credentials if needed"""
        with self._lock:
            now = time.time()

            # Check if credentials need refresh
            if (
                self._credentials is None
                or self._fetch_time is None
                or (now - self._fetch_time) > CREDENTIAL_REFRESH_SECONDS
            ):
                logger.info("fetching/refreshing S3 credentials")
                self._credentials = self._fetch_credentials()
                self._fetch_time = now
                self._session = AWSSession(**self._credentials)

            return self._session

    @staticmethod
    def _fetch_credentials() -> dict[str, Any]:
        """Fetch new credentials from MAAP"""
        maap = MAAP(maap_host="api.maap-project.org")
        creds = maap.aws.earthdata_s3_credentials(
            "https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials"
        )
        return {
            "aws_access_key_id": creds["accessKeyId"],
            "aws_secret_access_key": creds["secretAccessKey"],
            "aws_session_token": creds["sessionToken"],
            "region_name": "us-west-2",
        }


# Global credential manager instance
_credential_manager = CredentialManager()

HLS_COLLECTIONS = ["HLSL30_2.0", "HLSS30_2.0"]
HLS_STAC_GEOPARQUET_HREF = "s3://nasa-maap-data-store/file-staging/nasa-map/hls-stac-geoparquet-archive/v2/{collection}/**/*.parquet"

URL_PREFIX = "https://data.lpdaac.earthdatacloud.nasa.gov/"
DTYPE = "int16"
FMASK_DTYPE = "uint8"
NODATA = -9999
FMASK_NODATA = 255

sr_scale = 0.0001
ang_scale = 0.01
SR_FILL = -9999
QA_FILL = 255 #FMASK_FILL

QA_BIT = {'cirrus': 0,
'cloud': 1,
'adj_cloud': 2,
'cloud shadow':3,
'snowice':4,
'water':5,
'aerosol_l': 6,
'aerosol_h': 7
}

chunk_size = dict(band=1, x=512, y=512)

BAND_MAPPING = {
    "HLSL30_2.0": {
        "coastal_aerosol": "B01",
        "blue": "B02",
        "green": "B03",
        "red": "B04",
        "nir_narrow": "B05",
        "swir_1": "B06",
        "swir_2": "B07",
        "cirrus": "B09",
        "thermal_infrared_1": "B10",
        "thermal": "B11",
        "Fmask": "Fmask",
    },
    "HLSS30_2.0": {
        "coastal_aerosol": "B01",
        "blue": "B02",
        "green": "B03",
        "red": "B04",
        "red_edge_1": "B05",
        "red_edge_2": "B06",
        "red_edge_3": "B07",
        "nir_broad": "B08",
        "nir_narrow": "B8A",
        "water_vapor": "B09",
        "cirrus": "B10",
        "swir_1": "B11",
        "swir_2": "B12",
        "Fmask": "Fmask",
    },
}
# these are the ones that we are going to use
DEFAULT_BANDS = [
    "red",
    "green",
    "blue",
    "nir_narrow",
    "swir_1",
    "swir_2",
    "Fmask",
]
DEFAULT_RESOLUTION = 30


def get_stac_items(
    mgrs_tile: str, start_datetime: datetime, end_datetime: datetime
) -> list[Item]:
    logger.info("querying HLS archive")
    client = DuckdbClient(use_hive_partitioning=True)
    client.execute(
        """
        CREATE OR REPLACE SECRET secret (
             TYPE S3,
             PROVIDER CREDENTIAL_CHAIN
        );
        """
    )

    items = []
    for collection in HLS_COLLECTIONS:
        items.extend(
            client.search(
                href=HLS_STAC_GEOPARQUET_HREF.format(collection=collection),
                datetime="/".join(
                    dt.isoformat() for dt in [start_datetime, end_datetime]
                ),
                filter={
                    "op": "and",
                    "args": [
                        {
                            "op": "like",
                            "args": [{"property": "id"}, f"%.T{mgrs_tile}.%"],
                        },
                        {
                            "op": "between",
                            "args": [
                                {"property": "year"},
                                start_datetime.year,
                                end_datetime.year,
                            ],
                        },
                    ],
                },
            )
        )

    logger.info(f"found {len(items)} items")

    return [Item.from_dict(item) for item in items]


def fetch_single_asset(
    asset_href: str,
    fill_value=SR_FILL,
    direct_bucket_access: bool = False,
):
    """
    Fetch data from a single asset.
    """
    try:
        # Get session from credential manager if using direct bucket access
        rasterio_env = {}
        if direct_bucket_access:
            rasterio_env["session"] = _credential_manager.get_session()

        with rio.Env(**rasterio_env):
            # return rxr.open_rasterio(asset_href, lock=False, chunks=chunk_size, driver='GTiff').squeeze()
            with rio.open(asset_href) as src:
                return da.from_array(src.read(1), chunks=chunk_size)
            #     raster_crs = src.crs.to_string()
            #     xs_4326, ys_4326 = zip(*coords_4326)
            #     xs_proj, ys_proj = transform("EPSG:4326", raster_crs, xs_4326, ys_4326)
            #     coords_proj = list(zip(xs_proj, ys_proj))

            #     values = list(src.sample(coords_proj))

            #     return item_id, band_name, [v[0] for v in values]

    except Exception as e:
        logger.warning(f"Failed to read {asset_href}: {e}")
        return np.zeros((3660, 3660)) + fill_value


def fetch_with_retry(asset_href: Path, max_retries: int = 3, delay: int = 5, fill_value=SR_FILL, access_type="external"):
    for attempt in range(max_retries):
        try:
            return fetch_single_asset(
                asset_href=asset_href,
                fill_value=fill_value,
                direct_bucket_access=(access_type == "direct"),
            )
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = delay * (2**attempt)
                logger.warning(
                    f"Link {asset_href} attempt {attempt + 1}/{max_retries} failed: {e}. "
                    f"Retrying in {wait_time} seconds..."
                )
                time.sleep(wait_time)
            else:
                logger.error(
                    f"All {max_retries} attempts failed for {asset_href}. Last error: {e}"
                )
                return np.zeros((3660, 3660)) + fill_value


common_bands = ["Blue","Green","Red","NIR_Narrow","SWIR1", "SWIR2", "Fmask"]

L8_bandname = {"B01":"Coastal_Aerosol", "B02":"Blue", "B03":"Green", "B04":"Red", 
               "B05":"NIR_Narrow", "B06":"SWIR 1", "B07":"SWIR 2", "B09":"Cirrus"}
S2_bandname = {"B01":"Coastal_Aerosol", "B02":"Blue", "B03":"Green", "B04":"Red", 
               "B8A":"NIR_Narrow", "B11":"SWIR1", "B12":"SWIR2", "B10":"Cirrus"}

L8_name2index = {'Coastal_Aerosol': 'B01', 'Blue': 'B02', 'Green': 'B03', 'Red': 'B04',
                 'NIR_Narrow': 'B05', 'SWIR1': 'B06', 'SWIR2': 'B07', 'Fmask': 'Fmask'}
S2_name2index = {'Coastal_Aerosol': 'B01', 'Blue': 'B02', 'Green': 'B03', 'Red': 'B04', 
                 'NIR_Edge1': 'B05', 'NIR_Edge2': 'B06', 'NIR_Edge3': 'B07', 
                  'NIR_Broad': 'B08', 'NIR_Narrow': 'B8A', 'SWIR1': 'B11', 'SWIR2': 'B12', 'Fmask': 'Fmask'}


def mask_hls(qa_arr, mask_list=['cloud', 'adj_cloud', 'cloud shadow']):
    # This function takes the HLS QA array as input and exports the cloud mask array. 
    # The mask_list assigns the QA conditions you would like to mask.
    msk = np.zeros_like(qa_arr)#.astype(bool)
    for m in mask_list:
        if m in QA_BIT.keys():
            msk += (qa_arr & 1 << QA_BIT[m]) > 0
        if m == 'aerosol_high':
            msk += ((qa_arr & (1 << QA_BIT['aerosol_h'])) > 0) * ((qa_arr & (1 << QA_BIT['aerosol_l'])) > 0)
        if m == 'aerosol_moderate':
            msk += ((qa_arr & (1 << QA_BIT['aerosol_h'])) > 0) * ((qa_arr | (1 << QA_BIT['aerosol_l'])) != qa_arr)
        if m == 'aerosol_low':
            msk += ((qa_arr | (1 << QA_BIT['aerosol_h'])) != qa_arr) * ((qa_arr & (1 << QA_BIT['aerosol_l'])) > 0)
    return msk > 0


def get_geo(filepath, max_retries: int = 3, delay: int = 5, access_type="external"):
    for attempt in range(max_retries):
        try:
            # Get session from credential manager if using direct bucket access
            rasterio_env = {}
            if access_type == "direct":
                rasterio_env["session"] = _credential_manager.get_session()
            with rio.Env(**rasterio_env):
                with rio.open(filepath) as ds:
                    return ds.transform, ds.crs
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for {filepath}: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
    # raise RuntimeError(f"Failed to read {filepath} after {max_retries} attempts")
    return None, None


def saveGeoTiff(filename, data, template_file, access_type="external"):
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))
    if os.path.exists(filename):
        os.remove(filename)

    if data.ndim == 2:
        nband, height_data, width_data = 1, data.shape[0], data.shape[1]
    else:
        nband, height_data, width_data = data.shape[0], data.shape[1], data.shape[2]
    try:
        # Get session from credential manager if using direct bucket access
        rasterio_env = {}
        if access_type == "direct":
            rasterio_env["session"] = _credential_manager.get_session()
        with rio.Env(**rasterio_env):
            with rio.open(template_file) as ds:
                output_transform, output_crs = ds.transform, ds.crs
                profile = {
                            'driver': 'GTiff',
                            'dtype': data.dtype,
                            'count': nband,  # Number of bands
                            'height': height_data,
                            'width': width_data,
                            'crs': output_crs,
                            'transform': output_transform,
                            'compress': 'lzw' # Optional: add compression
                        }
            with rio.open(filename, 'w', **profile) as dst:
                if nband == 1:
                    dst.write(data, 1)
                else:
                    for i in range(nband):
                        dst.write(data[i], i + 1)
            return True
    except Exception as e:
        print(f"An error occurred: {e}")


def load_band_retry(tif_path: Path, max_retries: int = 3, delay: int = 5, fill_value=SR_FILL, access_type="external"):
    for attempt in range(max_retries):
        try:
            # Get session from credential manager if using direct bucket access
            # return rxr.open_rasterio(tif_path, lock=False, chunks=chunk_size, driver='GTiff').squeeze()
            rasterio_env = {}
            if access_type == "direct":
                rasterio_env["session"] = _credential_manager.get_session()
            with rio.Env(**rasterio_env):
                return rxr.open_rasterio(tif_path, lock=False, chunks=chunk_size, driver='GTiff').squeeze()
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for {tif_path}: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
    # raise RuntimeError(f"Failed to read {tif_path} after {max_retries} attempts")
    return da.zeros((3660, 3660), chunks=chunk_size) + fill_value


def get_meta(file_path: str):
    return rio.open(file_path).meta


def find_tile_bounds(tile: str):
    gdf = geopandas.read_file(r"s3://maap-ops-workspace/shared/zhouqiang06/AuxData/Sentinel-2-Shapefile-Index-master/sentinel_2_index_shapefile.shp")
    # gdf = geopandas.read_file(r"/projects/my-public-bucket/AuxData/Sentinel-2-Shapefile-Index-master/sentinel_2_index_shapefile.shp")
    bounds_list = [np.round(c, 3) for c in gdf[gdf["Name"]==tile].bounds.values[0]]
    return tuple(bounds_list)


def get_HLS_data(tile:str, bandnum:int, start_date:str, end_date:str, access_type="external"):
    print("Searching HLS STAC Geoparquet archive for HLS data...")
    # from rustac import DuckdbClient
    client = DuckdbClient(use_hive_partitioning=True)
    # configure duckdb to find S3 credentials
    client.execute(
        """
        CREATE OR REPLACE SECRET secret (
            TYPE S3,
            PROVIDER CREDENTIAL_CHAIN
        );
        """
    )
    response = client.search(
        href="s3://maap-ops-workspace/shared/henrydevseed/hls-stac-geoparquet-v1/year=*/month=*/*.parquet",
        datetime=f"{start_date}T00:00:00Z/{end_date}T23:59:59Z",
        bbox=find_tile_bounds(tile),
    )
    results = GetBandLists_HLS_STAC(response, tile, bandnum)
    if access_type=="direct":
        results = [r.replace(URL_PREFIX, "s3://") for r in results]
    return results


def GetBandLists_HLS_STAC(response, tile:str, bandnum:int):
    BandList = []
    for i in range(len(response)):
        product_type = response[i]['id'].split('.')[1]
        if product_type=='L30':
            bands = dict({2:'B02', 3:'B03', 4:'B04', 5:'B05', 6:'B06', 7:'B07',8:'Fmask'})
        elif product_type=='S30':
            bands = dict({2:'B02', 3:'B03', 4:'B04', 5:'B8A', 6:'B11', 7:'B12',8:'Fmask'})
        else:
            print("HLS product type not recognized: Must be L30 or S30.")
            os._exit(1)
            
        try:
            getBand = response[i]['assets'][bands[bandnum]]['href']
            if filter_url(getBand, tile, bands[bandnum]):
                BandList.append(getBand)
        except Exception as e:
            print(e)
    return BandList


def filter_url(url: str, tile: str, band: str):
    if (os.path.basename(url).split('.')[2][1:]==tile) & (url.endswith(f"{band}.tif")):
        return True
    return False    


def get_tile_urls(tile:str, bandnum:int, start_date:str, end_date:str, access_type="external"):
    print("Searching EarthAccess for HLS data...")
    url_list = []
    try:
        results = earthaccess.search_data(short_name=f"HLSL30",
                                        cloud_hosted=True,
                                        temporal = (start_date, end_date), #"2022-07-17","2022-07-31"
                                        bounding_box = find_tile_bounds(tile), #bounding_box = (-51.96423,68.10554,-48.71969,70.70529)
                                        )
    except Exception as e:
        print(f"An error occurred searching HLSL30: {e}")
        results = []
    if len(results) > 0:
        bands = dict({2:'B02', 3:'B03', 4:'B04', 5:'B05', 6:'B06', 7:'B07',8:'Fmask'})
        for rec in results:
            for url in rec.data_links(access=access_type):
                if filter_url(url, tile, bands[bandnum]):
                    url_list.append(url)
    try:
        results = earthaccess.search_data(short_name=f"HLSS30",
                                        cloud_hosted=True,
                                        temporal = (start_date, end_date), #"2022-07-17","2022-07-31"
                                        bounding_box = find_tile_bounds(tile), #bounding_box = (-51.96423,68.10554,-48.71969,70.70529)
                                        )
    except Exception as e:
        print(f"An error occurred searching HLSS30: {e}")
        results = []
    if len(results) > 0:
        bands = dict({2:'B02', 3:'B03', 4:'B04', 5:'B8A', 6:'B11', 7:'B12',8:'Fmask'})
        for rec in results:
            for url in rec.data_links(access=access_type):
                if filter_url(url, tile, bands[bandnum]):
                    url_list.append(url)
    return url_list


def find_all_granules(tile: str, bandnum: int, start_date: str, end_date: str, search_source="STAC", access_type="external"):
    if search_source.lower() == "stac":
        url_list = get_HLS_data(tile=tile, bandnum=bandnum, start_date=start_date, end_date=end_date, access_type=access_type)
    elif search_source.lower() == "earthaccess":
        url_list = get_tile_urls(tile=tile, bandnum=bandnum, start_date=start_date, end_date=end_date, access_type=access_type) 
    else:
        print("search_source not recognized. Must be 'STAC' or 'earthaccess'.")
        # os._exit(1)
    if len(url_list) == 0:
        print("No granules found.")
        return pd.DataFrame()
    sat_list = [os.path.basename(g).split('.')[1] for g in url_list] # sat from HLS.L10.T18SUJ.2020010T155225.v2.0
    date_list = [datetime.strptime(os.path.basename(g).split('.')[3][:7], "%Y%j") for g in url_list] # date from HLS.L10.T18SUJ.2020010T155225.v2.0
    return pd.DataFrame({"Date": date_list, "Sat": sat_list, "granule_path": url_list})


def preproccess(filename, factor=1):
    outdir = os.path.join(os.path.dirname(filename), 'temp')
    outname_prj = os.path.join(outdir, os.path.basename(filename).replace('.tif', '_prj.tif'))
    outname_lres = outname_prj.replace('.tif', '_lowres.tif')

    if not os.path.exists(outname_prj):
        #open source raster
        srcRst = rio.open(filename)
        dstCrs = {'init': 'EPSG:4326'}
        #calculate transform array and shape of reprojected raster
        transform, width, height = calculate_default_transform(
                srcRst.crs, dstCrs, srcRst.width, srcRst.height, *srcRst.bounds)
        #working of the meta for the destination raster
        kwargs = srcRst.meta.copy()
        kwargs.update({
                'crs': dstCrs,
                'transform': transform,
                'width': width,
                'height': height
            })
        #open destination raster
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        dstRst = rio.open(outname_prj, 'w', **kwargs)
        #reproject and save raster band data
        for i in range(1, srcRst.count + 1):
            reproject(
                source=rio.band(srcRst, i),
                destination=rio.band(dstRst, i),
                #src_transform=srcRst.transform,
                src_crs=srcRst.crs,
                #dst_transform=transform,
                dst_crs=dstCrs,
                resampling=Resampling.nearest)
        #close destination raster
        dstRst.close()
        if factor != 1:
            # print('Resample.')
            with rio.open(outname_prj) as dataset:
                # resample data to target shape
                data = dataset.read(
                    out_shape=(
                        dataset.count,
                        int(dataset.height * factor),
                        int(dataset.width * factor)
                    ),
                    resampling=Resampling.nearest
                )
                # print('data shape: ', data.shape)
                if data.shape[-1] > 0 and data.shape[-2] > 0:
                    # scale image transform
                    transform = dataset.transform * dataset.transform.scale(
                        # (1 / factor),
                        # (1 / factor)
                        (dataset.width / data.shape[-1]),
                        (dataset.height / data.shape[-2])
                    )
                    out_meta = dataset.meta.copy()
                    out_meta.update({"driver": "GTiff",
                                    "height": data.shape[1],
                                    "width": data.shape[2],
                                    "transform": transform,
                                    })
                    with rio.open(outname_lres, "w", **out_meta) as dest:
                        dest.write(data)
                    return outname_lres
            return
    # if already processed
    if factor == 1:
        return outname_prj
    else:
        return outname_lres


def merge_tiles(file_list, out_name, preprocessing=True, dtype=np.float32, nodata=0): # first check images with overlap
    # print("nodata: ", nodata)
    print('Merging')
    # print(file_list)
    print(len(file_list), ' tiles')
    src_files_to_mosaic = []
    for f in file_list:
        src = rio.open(f)
        if src is not None:
            src_files_to_mosaic.append(src)
        # src_files_to_mosaic.append(replace_none_with_zero(src))
    # print("nodata: ", nodata)
    mosaic, out_trans = merge(src_files_to_mosaic, dtype=dtype, nodata=nodata) #
    out_meta = src.meta.copy()
    out_meta.update({"driver": "GTiff",
                    "height": mosaic.shape[1],
                    "width": mosaic.shape[2],
                    "transform": out_trans,
                    }
                    )
    with rio.open(out_name, "w", **out_meta) as dest:
        dest.write(mosaic)



def run(tile: str, start_date: str, end_date: str, save_dir: str, search_source="STAC", access_type="direct"):
    save_dir = os.path.join(save_dir, tile[:2], tile[2], tile[3], tile[4])
    # save_dir = os.path.join(save_dir)
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    out_revisit_all = os.path.join(save_dir, f'HLS.total.{tile}.revisit.median.date.tif')
    out_revisit_clear = os.path.join(save_dir, f'HLS.total.{tile}.revisit.median.date.clear.tif')
    out_num_all = os.path.join(save_dir, f'HLS.total.{tile}.num.obs.tif')
    out_num_clear = os.path.join(save_dir, f'HLS.total.{tile}.num.obs.clear.tif')
    # check if obs_count are processed
    if os.path.exists(out_revisit_all) and os.path.exists(out_revisit_clear) and os.path.exists(out_num_all) and os.path.exists(out_num_clear):
        return
    # access_type="direct" # direct, or external
    img_list = find_all_granules(tile=tile, bandnum=8, start_date=start_date, end_date=end_date, search_source=search_source, access_type=access_type)['granule_path'].tolist()
    print(img_list[:3])
    if len(img_list) > 0:
        img_list = list(set(img_list)) # Remove duplicates from the list
        print(len(img_list), ' images found')
        date_list = np.asarray([datetime.strptime(os.path.basename(img_file).split('.')[3][:7], '%Y%j') for img_file in img_list])
        img_list = np.asarray(img_list)[np.argsort(date_list)]
        tile = os.path.basename(img_list[0]).split('.')[2]
        # Count number of clear observation
        time_diff_arr = da.zeros((len(img_list)-1, 3660, 3660), chunks=chunk_size, dtype=np.float32)
        time_diff_mask = da.full((len(img_list)-1, 3660, 3660), False, chunks=chunk_size, dtype=bool)
        time_diff_mask_clear = da.full((len(img_list)-1, 3660, 3660), False, chunks=chunk_size, dtype=bool)
        clear_mask_monthly = da.zeros((12, 3660, 3660), chunks=chunk_size, dtype=np.uint8)
        for i_img, img_file in enumerate(img_list):
            img_month = datetime.strptime(os.path.basename(img_file).split('.')[3][:7], '%Y%j').month
            if i_img == 0:
                pre_date = datetime.strptime(os.path.basename(img_file).split('.')[3][:7], '%Y%j')
            else:
                cur_date = datetime.strptime(os.path.basename(img_file).split('.')[3][:7], '%Y%j')
                # arr = load_band_retry(img_file, fill_value=QA_FILL, access_type=access_type).to_numpy()
                arr = fetch_with_retry(img_file, fill_value=QA_FILL, access_type=access_type)#.to_numpy()
                if arr is not None:
                    # print('Calculating time difference for image ', (cur_date - pre_date).days, pre_date.strftime('%Y-%m-%d'), ' to ', cur_date.strftime('%Y-%m-%d'))
                    time_diff_arr[i_img-1, :, :] = (cur_date - pre_date).days
                    time_diff_mask[i_img-1, :, :] = (arr == QA_FILL)
                    clear_mask = (arr == QA_FILL) | mask_hls(arr, mask_list=['cloud', 'adj_cloud', 'cloud shadow'])
                    time_diff_mask_clear[i_img-1, :, :] = clear_mask
                    clear_mask_monthly[img_month-1, :, :] += (~clear_mask).astype(np.uint8)
                pre_date = cur_date
            ## save revisit interval file
        time_diff_arr_ma = da.ma.masked_array(data=time_diff_arr, mask=time_diff_mask, fill_value=np.nan)
        print('time diff array stats (all obs):')
        median_arr = da.nanmedian(time_diff_arr_ma, axis=0)
        median_arr[median_arr==np.nan] = SR_FILL
        saveGeoTiff(filename=out_revisit_all, data=median_arr, template_file=img_list[0], access_type=access_type)
        preproccess(out_revisit_all, factor=1/33)

        time_diff_arr_ma = da.ma.masked_array(data=time_diff_arr, mask=time_diff_mask_clear, fill_value=np.nan)
        median_arr = np.nanmedian(time_diff_arr_ma, axis=0)
        median_arr[median_arr==np.nan] = SR_FILL
        saveGeoTiff(filename=out_revisit_clear, data=median_arr, template_file=img_list[0], access_type=access_type)
        preproccess(out_revisit_clear, factor=1/33)

        del median_arr, time_diff_arr, time_diff_arr_ma
        ## save obs count file
        saveGeoTiff(filename=out_num_all, data=da.sum(~time_diff_mask, axis=0), template_file=img_list[0], access_type=access_type)
        saveGeoTiff(filename=out_num_clear, data=da.sum(~time_diff_mask_clear, axis=0), template_file=img_list[0], access_type=access_type)
        preproccess(out_num_all, factor=1/33)
        preproccess(out_num_clear, factor=1/33)
        ## save monthly clear count file
        for i in range(12):
            out_name = os.path.join(save_dir, f'HLS.{tile}.M{i+1}.num.obs.clear.tif')
            print('Monthly clear obs count for month ', i+1)
            saveGeoTiff(filename=out_name, data=clear_mask_monthly[:, :, i], template_file=img_list[0], access_type=access_type)
            preproccess(out_name, factor=1/33)



if __name__ == "__main__":
    parse = argparse.ArgumentParser(
        description="Queries the HLS STAC geoparquet archive and create composite images"
    )
    parse.add_argument(
        "--tile",
        help="MGRS tile id, e.g. 15XYZ",
        required=True,
        type=str,
    )
    parse.add_argument(
        "--start_date",
        help="start date in ISO format (e.g., 2024-01-01)",
        required=True,
        type=str,
    )
    parse.add_argument(
        "--end_date",
        help="end date in ISO format (e.g., 2024-12-31)",
        required=True,
        type=str,
    )
    parse.add_argument(
        "--output_dir", help="Directory in which to save output", required=True
    )
    parse.add_argument(
        "--search_source",
        help="Either STAC or earthaccess to search for HLS granules",
        action="store_true",
        default="STAC",
    )
    args = parse.parse_args()

    output_dir = Path(args.output_dir)

    logger.info(
        f"setting GDAL config environment variables:\n{json.dumps(GDAL_CONFIG, indent=2)}"
    )
    os.environ.update(GDAL_CONFIG)

    logger.info(
        f"running with mgrs_tile: {args.tile}, start_datetime: {args.start_date}, end_datetime: {args.end_date}"
    )

    run(
        tile=args.tile,
        start_date=args.start_date,
        end_date=args.end_date,
        save_dir=output_dir,
        search_source=args.search_source,
    )
