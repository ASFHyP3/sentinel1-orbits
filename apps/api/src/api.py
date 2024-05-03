import os
import re

import boto3
import cachetools

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


def get_orbit_for_granule(granule: str, bucket: str, orbit_type: str):
    platform = granule[0:3]
    granule_start_date = granule[17:32]
    granule_end_date = granule[33:48]

    keys = list_bucket(bucket, prefix=f'{orbit_type}/{platform}')
    for key in keys:
        filename = os.path.basename(key)
        orbit_start_date = filename[42:57]
        orbit_end_date = filename[58:73]
        if orbit_start_date <= granule_start_date <= granule_end_date <= orbit_end_date:
            return key
    return None


def get_url(granule, bucket):
    for orbit_type in ['AUX_POEORB', 'AUX_RESORB']:
        key = get_orbit_for_granule(granule, bucket, orbit_type)
        if key:
            return build_url(bucket, key)
    return None


def is_s1_granule_name(granule: str) -> bool:
    platform = r'S1[AB]'
    beam_mode = r'(S[1-6]|IW|EW|WV)'
    product_type = r'(SLC_|GRD[FHM]|OCN_|RAW_)'
    details = r'[012]S[DSVH][VH]'
    date = r'\d{8}T\d{6}'
    orbit = r'\d{6}'
    datatake_id = r'[A-F\d]{6}'
    unique_id = r'[A-F\d]{4}'
    pattern = f'{platform}_{beam_mode}_{product_type}_{details}_{date}_{date}_{orbit}_{datatake_id}_{unique_id}$'
    return re.match(pattern, granule) is not None


def lambda_handler(event: dict, context: dict) -> dict:
    bucket = os.environ['BUCKET_NAME']
    granule = os.path.basename(event['rawPath'])

    if not is_s1_granule_name(granule):
        return {
            'isBase64Encoded': False,
            'statusCode': 400,
            'body': f'{granule} is not a valid S1 granule name'
        }

    url = get_url(granule, bucket)
    if url:
        return {
            'isBase64Encoded': False,
            'statusCode': 302,
            'headers': {
                'location': url,
            }
        }

    return {
        'isBase64Encoded': False,
        'statusCode': 404,
        'body': f'No orbit file found for {granule}'
    }
