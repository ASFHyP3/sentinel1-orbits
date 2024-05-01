import os

import boto3

s3 = boto3.client('s3')

def get_orbit_for_granule(granule: str, bucket: str, orbit_type: str):
    platform = granule[0:3]
    start_date = granule[17:32]
    end_date = granule[33:48]

    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(
        Bucket=bucket,
        Prefix=f'{orbit_type}/{platform}',
    )
    for page in page_iterator:
        for item in page['Contents']:
            filename = os.path.basename(item['Key'])
            start = filename[42:57]
            end = filename[58:73]
            if start <= start_date <= end_date <= end:
                return item['Key']
    return None


def get_url(granule, bucket):
    for orbit_type in ['AUX_POEORB', 'AUX_RESORB', 'AUX_PREORB']:
        key = get_orbit_for_granule(granule, bucket, orbit_type)
        if key:
            return f'https://{bucket}.s3.amazonaws.com/{key}'
    return None


def lambda_handler(event, context):
    bucket = os.environ['BUCKET_NAME']
    granule = os.path.basename(event['rawPath'])
    url = get_url(granule, bucket)

    if url:
        response = {
            'isBase64Encoded': False,
            'statusCode': 302,
            'headers': {
                'location': url,
            }
        }
    else:
        response = {
            'isBase64Encoded': False,
            'statusCode': 404,
            'body': f'No orbit file found for {granule}'
        }

    return response
