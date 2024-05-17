import pytest
from botocore.stub import Stubber


@pytest.fixture
def s3_stubber():
    with Stubber(api.s3) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()
