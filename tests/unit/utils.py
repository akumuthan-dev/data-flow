def get_base_dag_tasks(with_modified_date_check=False, fetch_name="fetch-data"):
    tasks = {
        fetch_name,
        "create-temp-tables",
        "insert-into-temp-table",
        "drop-temp-tables",
        "check-temp-table-data",
        "swap-dataset-table",
        "drop-swap-tables",
    }

    if with_modified_date_check:
        tasks = tasks.union(
            {"get-source-modified-date", "branch-on-modified-date", "stop", "continue"}
        )

    return tasks


def get_polling_dag_tasks(with_emails=False):
    tasks = {
        "poll-for-new-data",
        "scrape-and-load-data",
        "swap-dataset-table",
        "drop-swap-tables",
        "drop-temp-tables",
    }

    if with_emails:
        tasks.add("send-dataset-updated-emails")

    return tasks
