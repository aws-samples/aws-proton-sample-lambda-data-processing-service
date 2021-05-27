import boto3
import logging

from os import getenv
from utils.errors import OverloadedTaskId

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def get_dynamodb():
    return boto3.client('dynamodb')


def fetch_task(task_id):
    ddb_table_name = getenv("TABLE_NAME")

    resp = get_dynamodb().query(
        TableName=ddb_table_name,
        Select="ALL_ATTRIBUTES",
        KeyConditionExpression="#T = :t AND #R = :r",
        ExpressionAttributeNames={
            "#T": "hk",
            "#R": "rk"
        },
        ExpressionAttributeValues={
            ":t": {
                "S": task_id
            },
            ":r": {
                "S": "TASK"
            }
        }
    )
    items = resp['Items']
    if len(items) == 0:
        return None
    elif len(items) > 1:
        # This should really never happen because task ID is expected to be the primary partition key for
        # the dynamo table.
        raise OverloadedTaskId("Multiple tasks matched task ID, this should never happen.")
    return items[0]


def add_progress_update_to_task(task_id, progress_update):
    # Conceptually, this is adding an entry to a list of progress updates, and updating any global fields that have changed.
    status = progress_update['status']
    est_completion_date = str(progress_update['est_completion_date'])
    latest_update = {
        "user": {"S": progress_update["user"]},
        "status": {"S": status},
        "est_completion_date": {"N": est_completion_date},
        "updated_at": {"N": str(progress_update["updated_at"])},
        "update_message": {"S": progress_update["update_message"]}
    }
    if progress_update["update_message"] is None:
        latest_update["update_message"] = {"NULL": True}
    if progress_update["est_completion_date"] is None:
        latest_update["est_completion_date"] = {"NULL": True}

    ddb_table_name = getenv("TABLE_NAME")

    get_dynamodb().update_item(
        TableName=ddb_table_name,
        Key={
            'hk': {'S': task_id},
            'rk': {'S': 'TASK'}
        },
        UpdateExpression="SET #S = :s, #D = :d, #U = list_append(#U, :l)",
        ExpressionAttributeNames={
            "#S": "status",
            "#D": "est_completion_date",
            "#U": "raw_updates"
        },
        ExpressionAttributeValues={
            ":s": {"S": status},
            ":d": {"N": est_completion_date},
            ":l": {"L": [{"M": latest_update}]}
        }
    )


def add_task_details_to_progress_update(task_record, progress_update):
    task_id = task_record['hk']['S']
    progress_update['hk'] = task_id
    return progress_update
