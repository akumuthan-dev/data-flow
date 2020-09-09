def get_base_dag_tasks(with_modified_date_check=False, fetch_name="fetch-data"):
    if with_modified_date_check:
        return [
            "get-source-modified-date",
            "branch-on-modified-date",
            "stop",
            "continue",
            fetch_name,
            "create-temp-tables",
            "insert-into-temp-table",
            "drop-temp-tables",
            "check-temp-table-data",
            "swap-dataset-table",
            "drop-swap-tables",
        ]

    return [
        fetch_name,
        "create-temp-tables",
        "insert-into-temp-table",
        "drop-temp-tables",
        "check-temp-table-data",
        "swap-dataset-table",
        "drop-swap-tables",
    ]


def get_polling_dag_tasks():
    return [
        "poll-scrape-and-load-data",
        "swap-dataset-table",
        "drop-swap-tables",
        "drop-temp-tables",
    ]
