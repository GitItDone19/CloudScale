"""
Custom Airflow alert callbacks and event notifications.
"""

import logging

logger = logging.getLogger(__name__)


def failure_alert_callback(context):
    """
    Callback executed when an Airflow task fails.
    Extracts execution context and logs or pushes to webhook.
    """
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    execution_date = context.get("execution_date")
    exception = context.get("exception")

    alert_message = (
        f"🚨 [CloudScale Pipeline Alert] Task Failed!\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_instance.task_id}\n"
        f"Execution Date: {execution_date}\n"
        f"Exception: {exception}"
    )

    logger.error(alert_message)
    # Note: In production or Advanced stage, dispatch to Slack/Discord webhook URL
    return alert_message
