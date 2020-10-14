import json
from unittest import mock

import pytest

from dataflow.operators.email import send_dataset_update_emails


class TestSendDatasetUpdateEmails:
    def test_unset_env_var_raises_error(self):
        with pytest.raises(ValueError):
            send_dataset_update_emails("MY_NOT_SET_ENV_VAR")

    def test_sends_emails_using_data_from_env(self, monkeypatch):
        monkeypatch.setenv(
            "MY_VERY_NICE_ENV_VAR",
            json.dumps(
                {
                    "dataset_name": "My dataset",
                    "dataset_url": "https://foo.dataset",
                    "emails": ["test@email.com"],
                }
            ),
        )
        monkeypatch.setenv("NOTIFY_API_KEY", "api-key")
        monkeypatch.setenv("NOTIFY_TEMPLATE_ID__DATASET_UPDATED", "template-id")

        with mock.patch(
            "dataflow.operators.email.NotificationsAPIClient"
        ) as notify_mock:
            send_dataset_update_emails("MY_VERY_NICE_ENV_VAR")

        assert notify_mock.return_value.send_email_notification.call_args_list == [
            mock.call(
                email_address='test@email.com',
                template_id="template-id",
                personalisation={
                    "dataset_name": "My dataset",
                    "dataset_url": "https://foo.dataset",
                },
            )
        ]
