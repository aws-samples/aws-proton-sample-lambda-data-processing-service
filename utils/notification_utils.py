from uuid import uuid4

STATUS_MAPPING = {
    "pending_in_backlog": "BACKLOG",
    "assigned_not_started": "TODO",
    "work_in_progress": "WIP",
    "in_review": "REVIEW",
    "blocked": "BLOCKED",
    "done": "DONE",
    "cancelled": "CANCELLED"
}


def parse_update_message(msg):
    transformed = {}
    transformed["user"] = msg["User"]
    transformed["hk"] = msg["TaskId"]
    transformed["status"] = STATUS_MAPPING[msg["Status"]]
    transformed["est_completion_date"] = str(msg["est_completion_date"])
    transformed["updated_at"] = str(msg["LastUpdateDate"])
    transformed["update_message"] = msg["StatusMessage"]
    return transformed
