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

    # test cached response is returned instead of making more requests to S3
    assert api.list_bucket('foo', 'bar') == ['f', 'e', 'c', 'b', 'a']
