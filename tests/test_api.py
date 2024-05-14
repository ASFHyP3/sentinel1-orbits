import api


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
