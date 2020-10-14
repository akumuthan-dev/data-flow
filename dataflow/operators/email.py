import json
import os

from notifications_python_client.notifications import NotificationsAPIClient

from dataflow.utils import logger


def send_dataset_update_emails(update_emails_data_environment_variable):
    if update_emails_data_environment_variable not in os.environ:
        raise ValueError(
            f"Could not find data in environment for `{update_emails_data_environment_variable}`"
        )

    dataset_info = json.loads(os.environ[update_emails_data_environment_variable])

    dataset_url = dataset_info['dataset_url']
    dataset_name = dataset_info['dataset_name']
    emails = dataset_info['emails']

    client = NotificationsAPIClient(os.environ['NOTIFY_API_KEY'])

    logger.info(
        f"Sending `dataset updated` emails to subscribers for "
        f"this pipeline (`{update_emails_data_environment_variable}`)."
    )
    for email in emails:
        client.send_email_notification(
            email_address=email,
            template_id=os.environ['NOTIFY_TEMPLATE_ID__DATASET_UPDATED'],
            personalisation={"dataset_name": dataset_name, "dataset_url": dataset_url},
        )
