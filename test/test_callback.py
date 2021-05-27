import os
import unittest
from unittest.mock import patch
import boto3
from botocore.stub import Stubber, ANY
import callback
import json

STREAM_NAME = 'MySampleStream'


@patch.dict(os.environ, {"STREAM_NAME": STREAM_NAME})
class TestCallback(unittest.TestCase):
    def setUp(self) -> None:
        self.kinesis_client = boto3.client('kinesis', region_name='us-east-1')
        self.kinesis_stub = Stubber(self.kinesis_client)

    @patch('callback.get_kinesis')
    def test_handler(self, get_kinesis):
        get_kinesis.return_value = self.kinesis_client
        self.kinesis_stub.add_response(
            'put_record',
            expected_params={
                'Data': json.dumps({
                    'user': 'test_user',
                    'hk': '1',
                    'status': 'BACKLOG',
                    'est_completion_date': '1618702019',
                    'updated_at': '1618702019',
                    'update_message': 'Foo'
                }).encode('utf-8'),
                'StreamName': STREAM_NAME,
                'PartitionKey': ANY
            },
            service_response={
                'ShardId': 'Shard',
                'SequenceNumber': '0'
            }
        )

        event = {
            "body": "{\"User\": \"test_user\",\"TaskId\": \"1\", \"Status\": \"pending_in_backlog\", "
                    "\"est_completion_date\": 1618702019, \"LastUpdateDate\": 1618702019, \"StatusMessage\": \"Foo\"}",
            "isBase64Encoded": False
        }

        with self.kinesis_stub:
            callback.handler(event, None)

            self.kinesis_stub.assert_no_pending_responses()


if __name__ == '__main__':
    unittest.main()
