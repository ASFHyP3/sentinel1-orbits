import json
import os

import boto3
import requests

ESA_CREATE_TOKEN_URL = 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'
ESA_DELETE_TOKEN_URL = 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/account/sessions'

secretsmanager = boto3.client('secretsmanager')
s3 = boto3.client('s3')

session = requests.Session()


class EsaToken:
    """Context manager for authentication tokens for the ESA Copernicus Data Space Ecosystem (CDSE)"""

    def __init__(self, username: str, password: str):
        """
        Args:
            username: CDSE username
            password: CDSE password
        """
        self.username = username
        self.password = password
        self.token = None
        self.session_id = None

    def __enter__(self) -> str:
        data = {
            'client_id': 'cdse-public',
            'grant_type': 'password',
            'username': self.username,
            'password': self.password,
        }
        response = requests.post(ESA_CREATE_TOKEN_URL, data=data)
        response.raise_for_status()
        self.session_id = response.json()['session_state']
        self.token = response.json()['access_token']
        return self.token

    def __exit__(self, exc_type, exc_val, exc_tb):
        response = requests.delete(
            url=f'{ESA_DELETE_TOKEN_URL}/{self.session_id}',
            headers={'Authorization': f'Bearer {self.token}', 'Content-Type': 'application/json'},
        )
        response.raise_for_status()


def get_s3_orbits(bucket_name: str, prefix: str) -> set[str]:
    paginator = s3.get_paginator('list_objects_v2')
    params = {
        'Bucket': bucket_name,
        'Prefix': prefix,
    }
    objects = []
    for page in paginator.paginate(**params):
        if 'Contents' in page:
            objects.extend(page['Contents'])
    return {os.path.basename(obj['Key']) for obj in objects}


def get_cdse_orbits(orbit_type) -> list[dict]:
    url = 'https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel1/search.json'
    cdse_orbits = []

    params = {
        'productType': orbit_type,
        'maxRecords': 1000,
        'page': 1,
    }
    items = True
    while items:
        response = session.get(url, params=params)
        response.raise_for_status()
        items = [{'filename': feature['properties']['title'], 'id': feature['id']} for feature in response.json()['features']]
        cdse_orbits.extend(items)
        params['page'] += 1
    return cdse_orbits


def copy_file(filename: str, file_id: str, token: str, bucket_name: str, orbit_type: str) -> None:
    headers = {'Authorization': f'Bearer {token}'}
    url = f'https://zipper.dataspace.copernicus.eu/download/{file_id}'
    response = session.get(url, headers=headers)
    response.raise_for_status()

    s3.put_object(Bucket=bucket_name, Key=f'{orbit_type}/{filename}', Body=response.text)


def lambda_handler(event, context):
    secret_arn = os.environ['SECRET_ARN']
    bucket_name = os.environ['BUCKET_NAME']
    orbit_type = event['orbit_type']

    response = secretsmanager.get_secret_value(SecretId=secret_arn)
    credentials = json.loads(response['SecretString'])

    print(f'Getting S3 {orbit_type} inventory')
    s3_orbits = get_s3_orbits(bucket_name=bucket_name, prefix=orbit_type)

    print(f'Getting CDSE {orbit_type} inventory')
    cdse_orbits = get_cdse_orbits(orbit_type=orbit_type)

    orbits_to_copy = [orbit for orbit in cdse_orbits if orbit['filename'] not in s3_orbits]
    print(f'Found {len(orbits_to_copy)} {orbit_type} orbit files to fetch')

    with EsaToken(username=credentials['username'], password=credentials['password']) as token:
        for orbit in orbits_to_copy:
            print(f'Fetching {orbit["filename"]}')
            copy_file(orbit['filename'], orbit['id'], token, bucket_name, orbit_type)
