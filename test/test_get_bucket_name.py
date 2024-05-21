from moto import mock_s3
import boto3
from pytest import raises

from utils.get_bucket_name import get_bucket_name


@mock_s3
class TestGetBucketName:
    # ADD ERROR HANDLING AFTER HAPPY TESTS
    def test_receives_string_output_and_correct_bucket_name(self):
        s3_client = boto3.client('s3')
        s3_client.create_bucket(
            Bucket='testbucket',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )
        expected = 'testbucket'
        result = get_bucket_name("testbucket")

        assert isinstance(result, str)
        assert result == expected

    def test_receives_correct_bucket_name_with_unique_identifier(self):
        s3_client = boto3.client('s3')
        s3_client.create_bucket(
            Bucket='mockbucket-45789',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )
        s3_client.create_bucket(
            Bucket='anotherbucket',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )
        s3_client.create_bucket(
            Bucket='andanotherone',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )

        expected = 'mockbucket-45789'
        result = get_bucket_name("mockbucket")
        assert result == expected

    def test_handles_error_when_s3_is_empty(self, caplog):

        with raises(IndexError):
            get_bucket_name("Error")

        assert len(caplog.records) == 1
        assert caplog.records[0].levelname == "ERROR"
        assert "Buckets need to be created before extraction" in caplog.records[0].message
