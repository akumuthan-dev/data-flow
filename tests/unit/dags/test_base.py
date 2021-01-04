from datetime import datetime

import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator

from dataflow.dags import _PandasPipelineWithPollingSupport, _PipelineDAG
from dataflow.utils import TableConfig, LateIndex
from tests.unit.utils import get_base_dag_tasks


def test_base_dag_tasks():
    class TestDAG(_PipelineDAG):
        table_config = TableConfig(
            table_name="test-table",
            field_mapping=(
                ("id", sa.Column("id", sa.Integer, primary_key=True)),
                ("data", sa.Column("data", sa.Integer)),
            ),
        )

        def get_fetch_operator(self):
            return PythonOperator(
                task_id="fetch-data",
                python_callable=lambda: None,
                provide_context=True,
            )

    dag = TestDAG().get_dag()

    assert {t.task_id for t in dag.tasks} == get_base_dag_tasks(
        with_modified_date_check=False
    )


def test_dag_tasks_with_source_modified_data_utc_callable():
    class TestDAG(_PipelineDAG):
        table_config = TableConfig(
            table_name="test-table",
            field_mapping=(
                ("id", sa.Column("id", sa.Integer, primary_key=True)),
                ("data", sa.Column("data", sa.Integer)),
            ),
        )

        def get_fetch_operator(self):
            return PythonOperator(
                task_id="fetch-data",
                python_callable=lambda: None,
                provide_context=True,
            )

        @staticmethod
        def get_source_data_modified_utc(self) -> datetime:
            return datetime(2020, 1, 1)

        def get_source_data_modified_utc_callable(self):
            return self.get_source_data_modified_utc

    dag = TestDAG().get_dag()

    assert {t.task_id for t in dag.tasks} == get_base_dag_tasks(
        with_modified_date_check=True
    )


def test_pipeline_dag_task_relationships():
    class TestDAG(_PipelineDAG):
        table_config = TableConfig(
            table_name="test-table",
            field_mapping=(
                ("id", sa.Column("id", sa.Integer, primary_key=True)),
                ("data", sa.Column("data", sa.Integer)),
            ),
        )

        def get_fetch_operator(self):
            return PythonOperator(
                task_id="fetch-data",
                python_callable=lambda: None,
                provide_context=True,
            )

    dag = TestDAG().get_dag()

    fetch_data = dag.get_task('fetch-data')
    create_temp_tables = dag.get_task('create-temp-tables')
    insert_into_temp_table = dag.get_task('insert-into-temp-table')
    drop_temp_tables = dag.get_task('drop-temp-tables')
    check_temp_table_data = dag.get_task('check-temp-table-data')
    swap_dataset_table = dag.get_task('swap-dataset-table')
    drop_swap_tables = dag.get_task('drop-swap-tables')

    assert len(dag.tasks) == 7

    assert fetch_data.upstream_task_ids == set()
    assert fetch_data.downstream_task_ids == {'insert-into-temp-table'}

    assert create_temp_tables.upstream_task_ids == set()
    assert create_temp_tables.downstream_task_ids == {'insert-into-temp-table'}

    assert insert_into_temp_table.upstream_task_ids == {
        'create-temp-tables',
        'fetch-data',
    }
    assert insert_into_temp_table.downstream_task_ids == {
        'check-temp-table-data',
        'drop-temp-tables',
    }

    assert drop_temp_tables.upstream_task_ids == {'insert-into-temp-table'}
    assert drop_temp_tables.downstream_task_ids == set()

    assert check_temp_table_data.upstream_task_ids == {'insert-into-temp-table'}
    assert check_temp_table_data.downstream_task_ids == {'swap-dataset-table'}

    assert swap_dataset_table.upstream_task_ids == {'check-temp-table-data'}
    assert swap_dataset_table.downstream_task_ids == {'drop-swap-tables'}

    assert drop_swap_tables.upstream_task_ids == {'swap-dataset-table'}
    assert drop_swap_tables.downstream_task_ids == set()

    # Add a late index to the table config for the DAG, which should add a new intermediate task.
    TestDAG.table_config.indexes = [LateIndex('data')]
    dag = TestDAG().get_dag()

    fetch_data = dag.get_task('fetch-data')
    create_temp_tables = dag.get_task('create-temp-tables')
    insert_into_temp_table = dag.get_task('insert-into-temp-table')
    drop_temp_tables = dag.get_task('drop-temp-tables')
    create_post_insert_indexes = dag.get_task('create-post-insert-indexes')
    check_temp_table_data = dag.get_task('check-temp-table-data')
    swap_dataset_table = dag.get_task('swap-dataset-table')
    drop_swap_tables = dag.get_task('drop-swap-tables')

    assert len(dag.tasks) == 8

    assert fetch_data.upstream_task_ids == set()
    assert fetch_data.downstream_task_ids == {'insert-into-temp-table'}

    assert create_temp_tables.upstream_task_ids == set()
    assert create_temp_tables.downstream_task_ids == {'insert-into-temp-table'}

    assert insert_into_temp_table.upstream_task_ids == {
        'create-temp-tables',
        'fetch-data',
    }
    assert insert_into_temp_table.downstream_task_ids == {
        'create-post-insert-indexes',
        'drop-temp-tables',
    }

    assert create_post_insert_indexes.upstream_task_ids == {'insert-into-temp-table'}
    assert create_post_insert_indexes.downstream_task_ids == {'check-temp-table-data'}

    assert drop_temp_tables.upstream_task_ids == {'insert-into-temp-table'}
    assert drop_temp_tables.downstream_task_ids == set()

    assert check_temp_table_data.upstream_task_ids == {'create-post-insert-indexes'}
    assert check_temp_table_data.downstream_task_ids == {'swap-dataset-table'}

    assert swap_dataset_table.upstream_task_ids == {'check-temp-table-data'}
    assert swap_dataset_table.downstream_task_ids == {'drop-swap-tables'}

    assert drop_swap_tables.upstream_task_ids == {'swap-dataset-table'}
    assert drop_swap_tables.downstream_task_ids == set()


def test_pandas_dag_task_relationships():
    class TestDAG(_PandasPipelineWithPollingSupport):
        use_polling = False
        data_getter = lambda: None  # noqa: E731
        table_config = TableConfig(
            table_name="test-table",
            field_mapping=(
                ("id", sa.Column("id", sa.Integer, primary_key=True)),
                ("data", sa.Column("data", sa.Integer)),
            ),
        )

    class OtherDAG(_PipelineDAG):
        table_config = TableConfig(
            table_name="test-other-table",
            field_mapping=(
                ("id", sa.Column("id", sa.Integer, primary_key=True)),
                ("data", sa.Column("data", sa.Integer)),
            ),
        )

        def get_fetch_operator(self):
            return PythonOperator(
                task_id="fetch-data",
                python_callable=lambda: None,
                provide_context=True,
            )

    dag = TestDAG().get_dag()

    scrape_and_load_data = dag.get_task('scrape-and-load-data')
    check_temp_table_data = dag.get_task('check-temp-table-data')
    swap_dataset_table = dag.get_task('swap-dataset-table')
    drop_temp_tables = dag.get_task('drop-temp-tables')
    drop_swap_tables = dag.get_task('drop-swap-tables')

    assert len(dag.tasks) == 5

    assert scrape_and_load_data.upstream_task_ids == set()
    assert scrape_and_load_data.downstream_task_ids == {
        'drop-temp-tables',
        'check-temp-table-data',
    }

    assert check_temp_table_data.upstream_task_ids == {'scrape-and-load-data'}
    assert check_temp_table_data.downstream_task_ids == {'swap-dataset-table'}

    assert swap_dataset_table.upstream_task_ids == {'check-temp-table-data'}
    assert swap_dataset_table.downstream_task_ids == {'drop-swap-tables'}

    assert drop_temp_tables.upstream_task_ids == {'scrape-and-load-data'}
    assert drop_temp_tables.downstream_task_ids == set()

    assert drop_swap_tables.upstream_task_ids == {'swap-dataset-table'}
    assert drop_swap_tables.downstream_task_ids == set()

    # Add a late index to the table config for the DAG, which should add a new intermediate task.
    TestDAG.table_config.indexes = [LateIndex('data')]
    dag = TestDAG().get_dag()

    scrape_and_load_data = dag.get_task('scrape-and-load-data')
    check_temp_table_data = dag.get_task('check-temp-table-data')
    create_post_insert_indexes = dag.get_task('create-post-insert-indexes')
    swap_dataset_table = dag.get_task('swap-dataset-table')
    drop_temp_tables = dag.get_task('drop-temp-tables')
    drop_swap_tables = dag.get_task('drop-swap-tables')

    assert len(dag.tasks) == 6

    assert scrape_and_load_data.upstream_task_ids == set()
    assert scrape_and_load_data.downstream_task_ids == {
        'drop-temp-tables',
        'create-post-insert-indexes',
    }

    assert check_temp_table_data.upstream_task_ids == {'create-post-insert-indexes'}
    assert check_temp_table_data.downstream_task_ids == {'swap-dataset-table'}

    assert create_post_insert_indexes.upstream_task_ids == {'scrape-and-load-data'}
    assert create_post_insert_indexes.downstream_task_ids == {'check-temp-table-data'}

    assert swap_dataset_table.upstream_task_ids == {'check-temp-table-data'}
    assert swap_dataset_table.downstream_task_ids == {'drop-swap-tables'}

    assert drop_temp_tables.upstream_task_ids == {'scrape-and-load-data'}
    assert drop_temp_tables.downstream_task_ids == set()

    assert drop_swap_tables.upstream_task_ids == {'swap-dataset-table'}
    assert drop_swap_tables.downstream_task_ids == set()

    # Enable polling on the pipeline, which should add a new task at the start of the DAG.
    TestDAG.use_polling = True
    dag = TestDAG().get_dag()

    poll_for_new_data = dag.get_task('poll-for-new-data')
    scrape_and_load_data = dag.get_task('scrape-and-load-data')
    check_temp_table_data = dag.get_task('check-temp-table-data')
    create_post_insert_indexes = dag.get_task('create-post-insert-indexes')
    swap_dataset_table = dag.get_task('swap-dataset-table')
    drop_temp_tables = dag.get_task('drop-temp-tables')
    drop_swap_tables = dag.get_task('drop-swap-tables')

    assert len(dag.tasks) == 7

    assert poll_for_new_data.upstream_task_ids == set()
    assert poll_for_new_data.downstream_task_ids == {'scrape-and-load-data'}

    assert scrape_and_load_data.upstream_task_ids == {'poll-for-new-data'}
    assert scrape_and_load_data.downstream_task_ids == {
        'drop-temp-tables',
        'create-post-insert-indexes',
    }

    assert check_temp_table_data.upstream_task_ids == {'create-post-insert-indexes'}
    assert check_temp_table_data.downstream_task_ids == {'swap-dataset-table'}

    assert create_post_insert_indexes.upstream_task_ids == {'scrape-and-load-data'}
    assert create_post_insert_indexes.downstream_task_ids == {'check-temp-table-data'}

    assert swap_dataset_table.upstream_task_ids == {'check-temp-table-data'}
    assert swap_dataset_table.downstream_task_ids == {'drop-swap-tables'}

    assert drop_temp_tables.upstream_task_ids == {'scrape-and-load-data'}
    assert drop_temp_tables.downstream_task_ids == set()

    assert drop_swap_tables.upstream_task_ids == {'swap-dataset-table'}
    assert drop_swap_tables.downstream_task_ids == set()

    # Enable email notifications on DAG completion, which adds a new task at the end.
    TestDAG.update_emails_data_environment_variable = 'MY_ENV_VAR'
    dag = TestDAG().get_dag()

    poll_for_new_data = dag.get_task('poll-for-new-data')
    scrape_and_load_data = dag.get_task('scrape-and-load-data')
    check_temp_table_data = dag.get_task('check-temp-table-data')
    create_post_insert_indexes = dag.get_task('create-post-insert-indexes')
    swap_dataset_table = dag.get_task('swap-dataset-table')
    send_dataset_updated_emails = dag.get_task('send-dataset-updated-emails')
    drop_temp_tables = dag.get_task('drop-temp-tables')
    drop_swap_tables = dag.get_task('drop-swap-tables')

    assert len(dag.tasks) == 8

    assert poll_for_new_data.upstream_task_ids == set()
    assert poll_for_new_data.downstream_task_ids == {'scrape-and-load-data'}

    assert scrape_and_load_data.upstream_task_ids == {'poll-for-new-data'}
    assert scrape_and_load_data.downstream_task_ids == {
        'drop-temp-tables',
        'create-post-insert-indexes',
    }

    assert check_temp_table_data.upstream_task_ids == {'create-post-insert-indexes'}
    assert check_temp_table_data.downstream_task_ids == {'swap-dataset-table'}

    assert create_post_insert_indexes.upstream_task_ids == {'scrape-and-load-data'}
    assert create_post_insert_indexes.downstream_task_ids == {'check-temp-table-data'}

    assert swap_dataset_table.upstream_task_ids == {'check-temp-table-data'}
    assert swap_dataset_table.downstream_task_ids == {
        'drop-swap-tables',
        'send-dataset-updated-emails',
    }

    assert drop_temp_tables.upstream_task_ids == {'scrape-and-load-data'}
    assert drop_temp_tables.downstream_task_ids == set()

    assert drop_swap_tables.upstream_task_ids == {'swap-dataset-table'}
    assert drop_swap_tables.downstream_task_ids == set()

    assert send_dataset_updated_emails.upstream_task_ids == {'swap-dataset-table'}
    assert send_dataset_updated_emails.downstream_task_ids == set()

    # Check that tasks are added to trigger downstream DAGs on successful completion of the parent.
    TestDAG.trigger_dags_on_success = [OtherDAG]
    dag = TestDAG().get_dag()

    poll_for_new_data = dag.get_task('poll-for-new-data')
    scrape_and_load_data = dag.get_task('scrape-and-load-data')
    check_temp_table_data = dag.get_task('check-temp-table-data')
    create_post_insert_indexes = dag.get_task('create-post-insert-indexes')
    swap_dataset_table = dag.get_task('swap-dataset-table')
    send_dataset_updated_emails = dag.get_task('send-dataset-updated-emails')
    drop_temp_tables = dag.get_task('drop-temp-tables')
    drop_swap_tables = dag.get_task('drop-swap-tables')
    trigger_other_dag = dag.get_task('trigger-OtherDAG')

    assert len(dag.tasks) == 9

    assert poll_for_new_data.upstream_task_ids == set()
    assert poll_for_new_data.downstream_task_ids == {'scrape-and-load-data'}

    assert scrape_and_load_data.upstream_task_ids == {'poll-for-new-data'}
    assert scrape_and_load_data.downstream_task_ids == {
        'drop-temp-tables',
        'create-post-insert-indexes',
    }

    assert check_temp_table_data.upstream_task_ids == {'create-post-insert-indexes'}
    assert check_temp_table_data.downstream_task_ids == {'swap-dataset-table'}

    assert create_post_insert_indexes.upstream_task_ids == {'scrape-and-load-data'}
    assert create_post_insert_indexes.downstream_task_ids == {'check-temp-table-data'}

    assert swap_dataset_table.upstream_task_ids == {'check-temp-table-data'}
    assert swap_dataset_table.downstream_task_ids == {
        'drop-swap-tables',
        'send-dataset-updated-emails',
        'trigger-OtherDAG',
    }

    assert drop_temp_tables.upstream_task_ids == {'scrape-and-load-data'}
    assert drop_temp_tables.downstream_task_ids == set()

    assert drop_swap_tables.upstream_task_ids == {'swap-dataset-table'}
    assert drop_swap_tables.downstream_task_ids == set()

    assert send_dataset_updated_emails.upstream_task_ids == {'swap-dataset-table'}
    assert send_dataset_updated_emails.downstream_task_ids == set()

    assert trigger_other_dag.upstream_task_ids == {'swap-dataset-table'}
    assert trigger_other_dag.downstream_task_ids == set()
