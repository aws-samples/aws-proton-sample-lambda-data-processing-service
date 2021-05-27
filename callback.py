import boto3
import json
import logging

from base64 import b64decode
from hashlib import md5
from os import getenv

from utils.notification_utils import parse_update_message

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def get_kinesis():
    return boto3.client('kinesis')


def handler(event, context):
    body = event["body"]
    LOGGER.info("Input Event Body: {}".format(body))

    if event["isBase64Encoded"]:
        body = b64decode(body)
    body_json = json.loads(body)

    transformed_msg = parse_update_message(body_json)

    data = json.dumps(transformed_msg).encode('utf-8')

    m = md5()
    m.update(data)
    pk = m.digest()

    kinesis_stream_name = getenv('STREAM_NAME')

    get_kinesis().put_record(
        StreamName=kinesis_stream_name,
        Data=data,
        PartitionKey=str(pk)
    )
    return {"statusCode": 204}
