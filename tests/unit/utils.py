import importlib
import os
from collections import defaultdict
from glob import glob
from typing import Union, List

from dataflow.dags import _PandasPipelineWithPollingSupport, _PipelineDAG


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


def get_polling_dag_tasks(with_emails=False, with_triggered_dags=None):
    tasks = {
        "poll-for-new-data",
        "scrape-and-load-data",
        "check-temp-table-data",
        "swap-dataset-table",
        "drop-swap-tables",
        "drop-temp-tables",
    }

    if with_emails:
        tasks.add("send-dataset-updated-emails")

    if with_triggered_dags:
        for triggered_dag in with_triggered_dags:
            tasks.add(f"trigger-{triggered_dag}")

    return tasks


def get_all_dag_concrete_subclasses(root_classes: Union[type, List[type]]):
    """Import all of the DAGs, find subclasses of `root_classes`, and return them all.

    This will result in every module in `dataflow/dags` being imported into the local namespace. If this causes issues,
    this method could be run via multiprocessing to start up an isolated Python process.
    """
    for module in glob('dataflow/dags/*.py', recursive=True):
        if module == '__init__.py' or module[-3:] != '.py':
            continue
        importlib.import_module(module[:-3].replace(os.path.sep, '.'))

    def get_subclasses(cls):
        classes = cls.__subclasses__()
        for subclass in classes:
            classes.extend(get_subclasses(subclass))
        return classes

    if isinstance(root_classes, type):
        root_classes = [root_classes]

    all_dag_classes = []
    for root_class in root_classes:
        all_dag_classes += get_subclasses(root_class)

    concrete_dag_classes = filter(
        lambda c: not c.__name__.startswith('_'), all_dag_classes
    )

    return concrete_dag_classes


def get_fetch_retries_for_all_concrete_dags():
    """Gets all DAGs which are subclasses of `_PipelineDAG` and checks their `get_fetch_operator` tasks,
    returning a dict with all of the DAG names correlated to the # retries configured on that task.
    """
    concrete_dag_classes = get_all_dag_concrete_subclasses(_PipelineDAG)

    results_dict = {}
    for dag_class in concrete_dag_classes:
        results_dict[dag_class.__name__] = dag_class().get_fetch_operator().retries

    return results_dict


def get_dags_with_non_pk_indexes_on_sqlalchemy_columns():
    concrete_dag_classes = get_all_dag_concrete_subclasses(_PipelineDAG)

    results_dict = defaultdict(list)
    for dag_class in concrete_dag_classes:
        for table in dag_class().table_config.tables:
            for col in table.columns:
                if col.index is True or col.unique is True:
                    results_dict[dag_class.__name__].append(col.name)

    return results_dict


def get_table_definitions_for_all_concrete_dags():
    """Gets all DAGs which are subclasses of `_PipelineDAG` or `_PandasPipelineWithPollingSupport`
    and returns one or more associated TableConfigs.
    """
    concrete_dag_classes = get_all_dag_concrete_subclasses(
        [_PipelineDAG, _PandasPipelineWithPollingSupport]
    )

    results_dict = {}
    for dag_class in concrete_dag_classes:
        table_config = dag_class().table_config
        results_dict[dag_class.__name__] = [table_config] + [
            sub_table for _, sub_table in table_config.related_table_configs
        ]

    return results_dict


def get_dags_with_tables_in_public_schema():
    """Gets all table-making DAGs which have tables in the public schema (which is discouraged these days)"""
    concrete_dag_classes = get_all_dag_concrete_subclasses(
        [_PipelineDAG, _PandasPipelineWithPollingSupport]
    )

    results_dict = defaultdict(set)
    for dag_class in concrete_dag_classes:
        for table in dag_class().table_config.tables:
            if table.schema == 'public':
                results_dict[dag_class.__name__].add((table.schema, table.name))

    return results_dict
