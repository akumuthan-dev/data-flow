import os

from airflow.configuration import conf

REMOTE_LOGGING = conf.getboolean("core", "remote_logging")

BASE_LOG_FOLDER = conf.get("core", "BASE_LOG_FOLDER")
REMOTE_BASE_LOG_FOLDER = conf.get("core", "REMOTE_BASE_LOG_FOLDER")

FILENAME_TEMPLATE = "{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log"

LOG_LEVEL = "INFO"
DAG_PARSING_LOG_LEVEL = "WARN"

LOG_FORMAT = (
    "[%(asctime)s] %(levelname)s {%(name)s:%(filename)s:%(lineno)d} - %(message)s"
)

LOGGING_CONFIG: dict = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"airflow.task": {"format": LOG_FORMAT}},
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "airflow.task",
            "stream": "ext://sys.stdout",
        },
        "task": {
            "class": "airflow.utils.log.s3_task_handler.S3TaskHandler",
            "formatter": "airflow.task",
            "base_log_folder": os.path.expanduser(BASE_LOG_FOLDER),
            "s3_log_folder": REMOTE_BASE_LOG_FOLDER,
            "filename_template": FILENAME_TEMPLATE,
        }
        if REMOTE_LOGGING
        else {
            "class": "airflow.utils.log.file_task_handler.FileTaskHandler",
            "formatter": "airflow.task",
            "base_log_folder": os.path.expanduser(BASE_LOG_FOLDER),
            "filename_template": FILENAME_TEMPLATE,
        },
    },
    "loggers": {
        "dataflow": {"handlers": ["task"], "level": LOG_LEVEL, "propagate": False},
        "airflow.task": {"handlers": ["task"], "level": LOG_LEVEL, "propagate": False},
        "airflow.task_runner": {
            "handlers": ["console"],
            "level": LOG_LEVEL,
            "propagate": True,
        },
        "airflow.processor": {
            "handlers": ["console"],
            "level": DAG_PARSING_LOG_LEVEL,
            "propagate": False,
        },
        "airflow": {"handlers": ["console"], "level": LOG_LEVEL, "propagate": False},
    },
    "root": {"handlers": ["console"], "level": LOG_LEVEL},
}
