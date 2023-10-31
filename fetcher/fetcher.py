import json
import os
import time

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


def get_s3_orbits(bucket_name: str) -> set[str]:
    params = {
        'Bucket': bucket_name,
    }
    objects = []
    while True:
        response = s3.list_objects_v2(**params)
        objects.extend(response['Contents'])
        if 'ContinuationToken' not in response:
            break
        params['StartAfter'] = response['ContinuationToken']
    return {os.path.basename(obj['Key']) for obj in objects}


def get_cdse_orbits() -> list[dict]:
    url = 'https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel1/search.json'
    cdse_orbits = []

    for product_type in ['AUX_POEORB', 'AUX_RESORB']:
        params = {
            'productType': product_type,
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


def copy_file(filename: str, file_id: str, token: str, bucket_name: str) -> None:
    headers = {'Authorization': f'Bearer {token}'}
    url = f'https://zipper.dataspace.copernicus.eu/download/{file_id}'
    response = session.get(url, headers=headers)
    response.raise_for_status()
    if 'AUX_POEORB' in filename:
        key = f'AUX_POEORB/{filename}'
    else:
        key = f'AUX_RESORB/{filename}'
    s3.put_object(Bucket=bucket_name, Key=key, Body=response.text)


def lambda_handler(event, context):
    secret_arn = os.environ['SECRET_ARN']
    bucket_name = os.environ['BUCKET_NAME']

    response = secretsmanager.get_secret_value(SecretId=secret_arn)
    credentials = json.loads(response['SecretString'])
    s3_orbits = get_s3_orbits(bucket_name=bucket_name)
    cdse_orbits = get_cdse_orbits()
    with EsaToken(username=credentials['username'], password=credentials['password']) as token:
        for orbit in cdse_orbits:
            if orbit['filename'] not in s3_orbits:
                print(f'Fetching {orbit["filename"]}')
                copy_file(orbit['filename'], orbit['id'], token, bucket_name)
                time.sleep(15)
