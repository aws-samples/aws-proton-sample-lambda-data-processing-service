import boto3
import json
import logging

import utils.tasks as tasks

from base64 import b64decode, b64encode
from os import getenv
from utils.errors import OverloadedTaskId

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def get_firehose():
    return boto3.client('firehose')


def get_sqs():
    return boto3.client('sqs')


def handler(event, context):
    LOGGER.info("Raw Event: {}".format(json.dumps(event)))
    event_batch = []

    discrepancy_queue = getenv('DISCREPANCY_QUEUE')
    firehose_stream = getenv('FIREHOSE_STREAM')

    for record in event['Records']:
        payload = b64decode(record['kinesis']['data'])
        LOGGER.info("Decoded payload: {}".format(str(payload)))

        task_update = json.loads(payload)
        task_id = task_update['hk']

        # Cross-reference with task database.
        try:
            task = tasks.fetch_task(task_id)
            if task is None:
                LOGGER.error("Task ID {} not found in tasks database!".format(task_id))
                message = "No task found for id {}".format(task_id)
                get_sqs().send_message(
                    QueueUrl=discrepancy_queue,
                    MessageBody=message
                )
                continue
        except OverloadedTaskId:
            LOGGER.error("Task ID {} corresponds with multiple tasks, this should not be possible!".format(task_id))
            message = "Multiple tasks corresponded to an id {}".format(task_id)
            get_sqs().send_message(
                QueueUrl=discrepancy_queue,
                MessageBody=message
            )
            continue
        tasks.add_progress_update_to_task(task['hk']['S'], task_update)
        transformed_event = tasks.add_task_details_to_progress_update(task, task_update)

        # Add to event batch for Kinesis Firehose
        event_json = json.dumps(transformed_event)
        event_batch.append(event_json)

    if len(event_batch) != 0:
        combined_events = "\n".join(event_batch) + "\n"
        encoded_combined_events = b64encode(combined_events.encode()).decode()
        get_firehose().put_record(
            DeliveryStreamName=firehose_stream,
            Record={
                'Data': encoded_combined_events
            }
        )
