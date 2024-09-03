import os
from datetime import datetime, timedelta
from typing import Tuple

import boto3
import cachetools
from connexion import AsyncApp
from mangum import Mangum


app = AsyncApp(__name__)
app.add_api('openapi.yml')
lambda_handler = Mangum(app)

s3 = boto3.client('s3')


def build_url(bucket: str, key: str) -> str:
    return f'https://{bucket}.s3.amazonaws.com/{key}'


@cachetools.cached(cache=cachetools.TTLCache(maxsize=10, ttl=60))
def list_bucket(bucket: str, prefix: str) -> list[str]:
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
    )
    keys = [item['Key'] for page in page_iterator for item in page.get('Contents', [])]
    keys.sort(reverse=True)
    return keys


def get_anx_inclusive_time_range(granule_start_date: str, granule_end_date: str) -> Tuple[str, str]:
    # Orbital period of Sentinel-1 in seconds:
    # 12 days * 86400.0 seconds/day, divided into 175 orbits
    padding_next_minute_int = 60
    t_orbit_int = round((12 * 86400.0) / 175.0)

    # Temporal margin to apply to the start time of a frame
    # to make sure that the ascending node crossing is
    # included when choosing the orbit file.
    padding_next_minute = timedelta(seconds=padding_next_minute_int)
    margin_start_time = timedelta(seconds=t_orbit_int + padding_next_minute_int)

    dt_fmt = '%Y%m%dT%H%M%S'
    granule_start_date = datetime.strftime(datetime.strptime(granule_start_date, dt_fmt) - margin_start_time, dt_fmt)
    granule_end_date = datetime.strftime(datetime.strptime(granule_end_date, dt_fmt) + padding_next_minute, dt_fmt)
    return granule_start_date, granule_end_date


def get_orbit_for_granule(granule: str, bucket: str, orbit_type: str) -> str | None:
    platform = granule[0:3]
    granule_start_date, granule_end_date = get_anx_inclusive_time_range(granule[17:32], granule[33:48])

    keys = list_bucket(bucket=bucket, prefix=f'{orbit_type}/{platform}')
    for key in keys:
        filename = os.path.basename(key)
        orbit_start_date = filename[42:57]
        orbit_end_date = filename[58:73]
        if orbit_start_date <= granule_start_date <= granule_end_date <= orbit_end_date:
            return key
    return None


def get_url(granule: str, bucket: str) -> str | None:
    for orbit_type in ['AUX_POEORB', 'AUX_RESORB']:
        key = get_orbit_for_granule(granule, bucket, orbit_type)
        if key:
            return build_url(bucket, key)
    return None


def get_orbit(scene: str):
    bucket = os.environ['BUCKET_NAME']
    url = get_url(scene, bucket)
    if url:
        return None, 302, {'location': url}
    else:
        return f'No valid orbit file found for Sentinel-1 scene {scene}', 404
