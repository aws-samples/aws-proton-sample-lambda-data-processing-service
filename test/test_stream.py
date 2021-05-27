import os
import unittest
from unittest.mock import patch
from unittest.mock import Mock
import boto3
from botocore.stub import Stubber, ANY
import stream
import json
from base64 import b64encode

DISCREPANCY_QUEUE_NAME = "DiscrepancyQueueName"
FIREHOSE_STREAM_NAME = "FirehoseStreamName"


@patch.dict(os.environ, {
    "DISCREPANCY_QUEUE": DISCREPANCY_QUEUE_NAME,
    "FIREHOSE_STREAM": FIREHOSE_STREAM_NAME
})
class TestStream(unittest.TestCase):
    def setUp(self) -> None:
        self.firehose_client = boto3.client('firehose', region_name='us-east-1')
        self.firehose_stub = Stubber(self.firehose_client)

        self.sqs_client = boto3.client('sqs', region_name='us-east-1')
        self.sqs_stub = Stubber(self.sqs_client)

    @patch('utils.tasks.add_progress_update_to_task')
    @patch('utils.tasks.fetch_task')
    @patch('stream.get_firehose')
    def test_handle_no_discrepancies(self, get_firehose: Mock, fetch_task: Mock, add_progress_update_to_task: Mock):
        get_firehose.return_value = self.firehose_client

        update = {
            'user': 'test_user',
            'hk': '1',
            'status': 'BACKLOG',
            'est_completion_date': '1618702019',
            'updated_at': '1618702019',
            'update_message': 'Foo'
        }

        records = [
            {
                'kinesis': {
                    'data': self.get_update_base64(update)
                }
            }
        ]
        event = {'Records': records}

        fetch_task.return_value = {'hk': {'S': '1'}}
        add_progress_update_to_task.return_value = None

        self.firehose_stub.add_response(
            method='put_record',
            expected_params={
                'DeliveryStreamName': FIREHOSE_STREAM_NAME,
                'Record': ANY
            },
            service_response={
                'Encrypted': False,
                'RecordId': '1'
            }
        )

        with self.firehose_stub, self.sqs_stub:
            stream.handler(event, None)

            fetch_task.assert_called_with('1')
            add_progress_update_to_task.assert_called_with('1', update)
            self.firehose_stub.assert_no_pending_responses()

    @patch('utils.tasks.fetch_task')
    @patch('stream.get_sqs')
    def test_task_not_found(self, get_sqs: Mock, fetch_task: Mock):
        get_sqs.return_value = self.sqs_client

        update = {
            'user': 'test_user',
            'hk': '1',
            'status': 'BACKLOG',
            'est_completion_date': '1618702019',
            'updated_at': '1618702019',
            'update_message': 'Foo'
        }

        records = [
            {
                'kinesis': {
                    'data': self.get_update_base64(update)
                }
            }
        ]
        event = {'Records': records}
        fetch_task.return_value = None

        self.sqs_stub.add_response(
            method='send_message',
            expected_params={
                'QueueUrl': DISCREPANCY_QUEUE_NAME,
                'MessageBody': 'No task found for id 1'
            },
            service_response={}
        )

        with self.firehose_stub, self.sqs_stub:
            stream.handler(event, None)

            fetch_task.assert_called_with('1')
            self.sqs_stub.assert_no_pending_responses()
            self.firehose_stub.assert_no_pending_responses()

    """
    This requires some explanation:
        1. We take the update dictionary, call json.dumps on it which converts it to a json string
        2. Then we encode that string using utf-8
        3. Then we base64 encode the utf-8 encoded json dictionary, which results in a base64 encoded object bytes
        4. Finally we convert it back to ascii by decode the bytes, this will give us the string representation of the
           base64 encoded bytes, i.e. the traditional base64 string that looks something like: 
           R2Vla3NGb3JHZWVrcyBpcyB0aGUgYmVzdA==
    """
    @staticmethod
    def get_update_base64(update: dict):
        json_data = json.dumps(update).encode('utf-8')
        return b64encode(json_data).decode('ascii')


if __name__ == '__main__':
    unittest.main()
