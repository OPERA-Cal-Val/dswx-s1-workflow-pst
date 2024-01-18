import concurrent.futures
from functools import lru_cache
from pathlib import Path

import asf_search as asf
import geopandas as gpd
import requests
import urllib3
from dotenv import dotenv_values
from dem_stitcher.geojson_io import read_geojson_gzip
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch_dsl import Q, Search
from tqdm import tqdm

urllib3.disable_warnings()


def extract_burst_id(burst_id: str) -> int:
    track_token = burst_id.split("_")[0]
    return int(track_token[1:])


@lru_cache
def get_burst_df() -> gpd.GeoDataFrame:
    df = read_geojson_gzip("opera_burst_ids.geojson.zip")
    df["track"] = df["burst_id_jpl"].map(extract_burst_id)
    return df


def get_rtc_daac_urls(
    burst_ids: list[str], start_range: str = None, end_range: str = None
) -> dict:
    response = asf.search(
        operaBurstID=burst_ids,
        start=start_range,
        end=end_range,
        processingLevel="RTC",
    )

    urls = [[r.properties["url"]] + r.properties["additionalUrls"] for r in response]
    ids = [r.properties["sceneName"] for r in response]
    data = {id_: url_lst for (id_, url_lst) in zip(ids, urls)}
    return data


def download_file(url: str, out_path: str):
    # Source: https://stackoverflow.com/a/16696317
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=2**6):
                f.write(chunk)
    return out_path


@lru_cache
def get_search_client():
    config = dotenv_values()
    ES_USERNAME = config["ES_USERNAME"]
    ES_PASSWORD = config["ES_PASSWORD"]
    GRQ_URL = "https://100.104.62.10/grq_es/"
    # See: https://github.com/nasa/opera-sds-pcm/
    # blob/81ccb1bd40981588754a438b4dd0eb1506301276/tools/ops/cnm_check.py#L40-L47
    grq_client = Elasticsearch(
        GRQ_URL,
        http_auth=(ES_USERNAME, ES_PASSWORD),
        verify_certs=False,
        use_ssl=True,
        connection_class=RequestsHttpConnection,
        read_timeout=50000,
        terminate_after=2500,
        ssl_show_warn=False,
    )
    search = Search(using=grq_client, index="grq_v1.0_l2_rtc_s1-2023.09")

    if not grq_client.ping():
        raise ValueError(
            "Either JPL username/password is wrong or not connected to VPN"
        )

    return search


def get_rtc_docs_from_es(slc_id: str, target_rtc_version="1.0.1") -> list[dict]:
    "Version is determined by latest here: https://github.com/opera-adt/RTC/releases"
    search = get_search_client()
    q_qs = Q(
        "query_string", query=f'"{slc_id}"', default_field="metadata.input_granule_id"
    )

    query = search.query(q_qs)
    total = query.count()
    # using this: https://github.com/elastic/elasticsearch-dsl-py/issues/737
    query = query[0:total]
    resp = query.execute()
    print(f"{total} RTC Products found for {slc_id}")

    hits = list(resp.hits)

    def filter_by_version(hit):
        return hit.metadata.sas_version == target_rtc_version

    hits_f = list(filter(filter_by_version, hits))

    data = [hit.to_dict() for hit in hits_f]
    return data


def get_rtc_urls(
    slc_id: str, df_slc: gpd.GeoDataFrame, from_asf_daac: bool = True
) -> dict:
    """dict is of form {rtc_prod_id: [<list_of_urls>]}"""
    if not from_asf_daac:
        docs = get_rtc_docs_from_es(slc_id)
        url_dict = {doc["id"]: doc["metadata"]["product_urls"] for doc in docs}
    else:
        df_burst = get_burst_df()
        ind_0 = df_burst.intersects(df_slc.geometry.unary_union.buffer(-0.01))
        ind_1 = df_burst.track == df_slc.pathNumber[0]
        df_burst_int = df_burst[ind_0 & ind_1].reset_index(drop=True)
        burst_ids = df_burst_int.burst_id_jpl.tolist()

        range_start = str(df_slc.range_start[0])
        range_end = str(df_slc.range_end[0])
        url_dict = get_rtc_daac_urls(burst_ids, start_range=range_start, end_range=range_end)

    return url_dict


def _get_dst_paths_for_rtc(product_url_dicts: dict, directory=None) -> list[Path]:
    parent = directory or Path(".")
    out_paths = [
        parent / Path(opera_id) / url.split("/")[-1]
        for opera_id, urls in product_url_dicts.items()
        for url in urls
    ]
    return out_paths


def _get_urls_from_dict(product_url_dicts: dict) -> list[str]:
    urls = [url for _, urls in product_url_dicts.items() for url in urls]
    return urls


def download_rtc_products(url_dict: dict, directory: Path = None) -> Path:
    out_paths = _get_dst_paths_for_rtc(url_dict, directory=directory)
    [path.parent.mkdir(exist_ok=True, parents=True) for path in out_paths]
    urls = _get_urls_from_dict(url_dict)

    def download_one(data):
        url, out_path = data
        download_file(url, out_path)
        return out_path

    data_inputs = list(zip(urls, out_paths))
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        _ = list(tqdm(executor.map(download_one, data_inputs), total=len(data_inputs)))
    return out_paths
