import unittest.mock

import pytest
from botocore.stub import Stubber

import api


@pytest.fixture
def s3_stubber():
    with Stubber(api.s3) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


def test_lambda_handler():
    event = {
      'version': '2.0',
      'requestContext': {
        'http': {
          'method': 'GET',
          'path': '/ui',
          'sourceIp': '127.0.0.1',
        },
      },
    }
    assert api.lambda_handler(event, None) == {
        'body': '',
        'headers': {
            'content-length': '0',
            'content-type': 'application/json',
            'location': '/ui/'
        },
        'isBase64Encoded': False,
        'statusCode': 307
    }

    event['requestContext']['http']['path'] = '/ui/'
    assert api.lambda_handler(event, None)['statusCode'] == 200

    event['requestContext']['http']['path'] = '/foo'
    assert api.lambda_handler(event, None)['statusCode'] == 404

    event['requestContext']['http']['path'] = '/scene/foo'
    assert api.lambda_handler(event, None)['statusCode'] == 400


def test_build_url():
    assert api.build_url('foo', 'bar') == 'https://foo.s3.amazonaws.com/bar'
    assert api.build_url('bucket', 'key/with/prefix.txt') == 'https://bucket.s3.amazonaws.com/key/with/prefix.txt'


def test_list_bucket(s3_stubber):
    s3_stubber.add_response(
        method='list_objects_v2',
        expected_params={
            'Bucket': 'foo',
            'Prefix': 'bar'
        },
        service_response={
            'Contents': [
                {'Key': 'a'},
                {'Key': 'e'},
                {'Key': 'b'},
            ],
            'IsTruncated': True,
            'NextContinuationToken': 'token',
        },
    )

    s3_stubber.add_response(
        method='list_objects_v2',
        expected_params={
            'Bucket': 'foo',
            'Prefix': 'bar',
            'ContinuationToken': 'token',
        },
        service_response={
            'Contents': [
                {'Key': 'c'},
                {'Key': 'f'},
            ],
        },
    )

    assert api.list_bucket('foo', 'bar') == ['f', 'e', 'c', 'b', 'a']

    # test cached response is returned without making more requests to S3
    assert api.list_bucket('foo', 'bar') == ['f', 'e', 'c', 'b', 'a']


def test_get_anx_inclusive_time_range():
    start, end = api.get_anx_inclusive_time_range('20240101T000000', '20240102T000000')
    assert isinstance(start, str)
    assert start == '20231231T222015'
    assert isinstance(end, str)
    assert end == '20240102T000100'


def test_get_orbit_for_granule():
    with unittest.mock.patch('api.list_bucket') as mock_list_bucket:
        mock_list_bucket.return_value = [
            'AUX_POEORB/S1A_OPER_AUX_POEORB_OPOD_20230604T080854_V20230514T225942_20230516T005942.EOF',
        ]
        assert api.get_orbit_for_granule(
            granule='S1A_IW_GRDH_1SDV_20230515T075514_20230515T075542_048541_05D6B8_579B',
            bucket='myBucket',
            orbit_type='myOrbitType',
        ) == 'AUX_POEORB/S1A_OPER_AUX_POEORB_OPOD_20230604T080854_V20230514T225942_20230516T005942.EOF'
        mock_list_bucket.assert_called_once_with(bucket='myBucket', prefix='myOrbitType/S1A')

    with unittest.mock.patch('api.list_bucket') as mock_list_bucket:
        mock_list_bucket.return_value = [
            'AUX_POEORB/S1A_OPER_AUX_POEORB_OPOD_20230604T080854_V20230514T225942_20230516T005942.EOF',
        ]
        assert api.get_orbit_for_granule(
            granule='S1A_IW_GRDH_1SDV_20240515T075514_20240515T075542_048541_05D6B8_579B',
            bucket='myBucket',
            orbit_type='myOrbitType',
        ) is None
        mock_list_bucket.assert_called_once_with(bucket='myBucket', prefix='myOrbitType/S1A')

    with unittest.mock.patch('api.list_bucket') as mock_list_bucket:
        mock_list_bucket.return_value = [
            'AUX_POEORB/S1A_OPER_AUX_POEORB_OPOD_20230606T080854_V20240514T225942_20240516T005942.EOF',
            'AUX_POEORB/S1A_OPER_AUX_POEORB_OPOD_20230605T080854_V20240514T225942_20240516T005942.EOF',
            'AUX_POEORB/S1A_OPER_AUX_POEORB_OPOD_20230604T080854_V20240514T225942_20240516T005942.EOF',
        ]
        assert api.get_orbit_for_granule(
            granule='S1A_IW_GRDH_1SDV_20240515T075514_20240515T075542_048541_05D6B8_579B',
            bucket='myBucket',
            orbit_type='myOrbitType',
        ) == 'AUX_POEORB/S1A_OPER_AUX_POEORB_OPOD_20230606T080854_V20240514T225942_20240516T005942.EOF'
        mock_list_bucket.assert_called_once_with(bucket='myBucket', prefix='myOrbitType/S1A')


def test_get_url():
    with unittest.mock.patch('api.get_orbit_for_granule') as mock_get_orbit_for_granule:
        mock_get_orbit_for_granule.return_value = None
        assert api.get_url(granule='myGranule', bucket='myBucket') is None
        assert mock_get_orbit_for_granule.call_count == 2
        mock_get_orbit_for_granule.assert_has_calls([
            unittest.mock.call('myGranule', 'myBucket', 'AUX_POEORB'),
            unittest.mock.call('myGranule', 'myBucket', 'AUX_RESORB'),
        ])

    with unittest.mock.patch('api.get_orbit_for_granule') as mock_get_orbit_for_granule:
        mock_get_orbit_for_granule.return_value = 'foo'
        assert api.get_url(granule='myGranule', bucket='myBucket') == 'https://myBucket.s3.amazonaws.com/foo'
        mock_get_orbit_for_granule.assert_called_once_with('myGranule', 'myBucket', 'AUX_POEORB')

    with unittest.mock.patch('api.get_orbit_for_granule') as mock_get_orbit_for_granule:
        mock_get_orbit_for_granule.side_effect = [None, 'bar']
        assert api.get_url(granule='myGranule', bucket='myBucket') == 'https://myBucket.s3.amazonaws.com/bar'
        assert mock_get_orbit_for_granule.call_count == 2
        mock_get_orbit_for_granule.assert_has_calls([
            unittest.mock.call('myGranule', 'myBucket', 'AUX_POEORB'),
            unittest.mock.call('myGranule', 'myBucket', 'AUX_RESORB'),
        ])


def test_get_orbit(monkeypatch):
    monkeypatch.setenv('BUCKET_NAME', 'myBucket')

    with unittest.mock.patch('api.get_url') as mock_get_url:
        mock_get_url.return_value = None
        assert api.get_orbit('foo') == ('No valid orbit file found for Sentinel-1 scene foo', 404)
        mock_get_url.assert_called_once_with('foo', 'myBucket')

    with unittest.mock.patch('api.get_url') as mock_get_url:
        mock_get_url.return_value = 'https://foo.com/bar'
        assert api.get_orbit('foo') == (None, 302, {'location': 'https://foo.com/bar'})
