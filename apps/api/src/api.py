import os
import re

import boto3

s3 = boto3.client('s3')


def build_url(bucket: str, key: str) -> str:
    return f'https://{bucket}.s3.amazonaws.com/{key}'


def list_bucket(bucket: str, prefix: str) -> list[str]:
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
    )
    return [item['Key'] for page in page_iterator for item in page.get('Contents', [])]


def get_orbit_for_granule(granule: str, bucket: str, orbit_type: str):
    platform = granule[0:3]
    start_date = granule[17:32]
    end_date = granule[33:48]

    keys = list_bucket(bucket, prefix=f'{orbit_type}/{platform}')
    keys.sort(reverse=True)
    for key in keys:
        filename = os.path.basename(key)
        start = filename[42:57]
        end = filename[58:73]
        if start <= start_date <= end_date <= end:
            return key
    return None


def get_url(granule, bucket):
    for orbit_type in ['AUX_POEORB', 'AUX_RESORB', 'AUX_PREORB']:
        key = get_orbit_for_granule(granule, bucket, orbit_type)
        if key:
            return build_url(bucket, key)
    return None


def is_s1_granule_name(granule: str) -> bool:
    pattern = r'S1[AB]_(S[1-6]|IW|EW|WV)_(SLC_|GRD[FHM]|OCN_)_...._\d{8}T\d{6}_\d{8}T\d{6}_\d{6}_\d{6}_[A-F\d]{4}$'
    return re.match(pattern, granule) is not None


def lambda_handler(event, context):
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
