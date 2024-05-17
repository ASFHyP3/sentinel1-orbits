import pytest
import responses
from botocore.stub import Stubber

import fetcher


@pytest.fixture
def s3_stubber():
    with Stubber(fetcher.s3) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


@responses.activate
def test_esa_token():
    url = fetcher.ESA_CREATE_TOKEN_URL
    request_payload = {
        'client_id': 'cdse-public',
        'grant_type': 'password',
        'username': 'myUsername',
        'password': 'myPassword',
    }
    response_payload = {'access_token': 'ABC123', 'session_state': 'mySessionId'}
    post_request = responses.post(
        url=url,
        match=[responses.matchers.urlencoded_params_matcher(request_payload)],
        json=response_payload,
    )

    url = f'{fetcher.ESA_DELETE_TOKEN_URL}/mySessionId'
    headers = {
        'Authorization': 'Bearer ABC123',
        'Content-Type': 'application/json',
    }
    delete_request = responses.delete(
        url=url,
        match=[responses.matchers.header_matcher(headers)],
    )

    with fetcher.EsaToken(username='myUsername', password='myPassword') as token:
        assert token == 'ABC123'

    assert post_request.call_count == 1
    assert delete_request.call_count == 1


def test_get_s3_orbits(s3_stubber):
    s3_stubber.add_response(
        method='list_objects_v2',
        expected_params={
            'Bucket': 'foo',
            'Prefix': 'bar'
        },
        service_response={
            'Contents': [
                {'Key': 'bar/a'},
                {'Key': 'bar/stuff/e.txt'},
            ],
            'IsTruncated': True,
            'NextContinuationToken': 'token1',
        },
    )

    s3_stubber.add_response(
        method='list_objects_v2',
        expected_params={
            'Bucket': 'foo',
            'Prefix': 'bar',
            'ContinuationToken': 'token1',
        },
        service_response={
            'Contents': [
                {'Key': 'bar/c.zip'},
                {'Key': 'bar/hello/world/f'},
            ],
            'IsTruncated': True,
            'NextContinuationToken': 'token2',
        },
    )

    s3_stubber.add_response(
        method='list_objects_v2',
        expected_params={
            'Bucket': 'foo',
            'Prefix': 'bar',
            'ContinuationToken': 'token2',
        },
        service_response={},
    )

    assert fetcher.get_s3_orbits('foo', 'bar') == {'a', 'e.txt', 'c.zip', 'f'}


@responses.activate
def test_get_cdse_orbits_empty_response():
    url = 'https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel1/search.json'

    params = {
        'productType': 'AUX_POEORB',
        'maxRecords': 1000,
        'page': 1,
    }
    response_payload = {'features': []}
    responses.get(
        url=url,
        match=[responses.matchers.query_param_matcher(params)],
        json=response_payload,
    )

    assert fetcher.get_cdse_orbits('AUX_POEORB') == []


@responses.activate
def test_get_cdse_orbits():
    url = 'https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel1/search.json'

    responses.get(
        url=url,
        match=[responses.matchers.query_param_matcher({'productType': 'AUX_RESORB', 'maxRecords': 1000, 'page': 1})],
        json={
            'features': [
                {'properties': {'title': 'title1'}, 'id': 'id1'},
                {'properties': {'title': 'title2'}, 'id': 'id2'},
            ]
        },
    )

    responses.get(
        url=url,
        match=[responses.matchers.query_param_matcher({'productType': 'AUX_RESORB', 'maxRecords': 1000, 'page': 2})],
        json={
            'features': [
                {'properties': {'title': 'title3'}, 'id': 'id3'},
            ]
        },
    )

    responses.get(
        url=url,
        match=[responses.matchers.query_param_matcher({'productType': 'AUX_RESORB', 'maxRecords': 1000, 'page': 3})],
        json={
            'features': []
        },
    )

    assert fetcher.get_cdse_orbits('AUX_RESORB') == [
        {'filename': 'title1', 'id': 'id1'},
        {'filename': 'title2', 'id': 'id2'},
        {'filename': 'title3', 'id': 'id3'},
    ]


@responses.activate
def test_copy_file(s3_stubber):
    responses.get(
        url='https://zipper.dataspace.copernicus.eu/download/myId',
        match=[responses.matchers.header_matcher({'Authorization': 'Bearer myToken'})],
        body='foo',
    )
    s3_stubber.add_response(
        method='put_object',
        expected_params={
            'Bucket': 'myBucket',
            'Key': 'myOrbitType/myFilename',
            'Body': 'foo',
        },
        service_response={},
    )
    fetcher.copy_file('myFilename', 'myId', 'myToken', 'myBucket', 'myOrbitType')
