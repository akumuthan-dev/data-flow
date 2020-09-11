from datetime import datetime, time
from unittest import mock
from unittest.mock import call

import freezegun as freezegun
import pytest
import sqlalchemy

from dataflow.dags import _FastPollingPipeline
from dataflow.operators import db_tables
from dataflow.operators.db_tables import branch_on_modified_date
from dataflow.utils import get_temp_table, TableConfig


@pytest.fixture
def table():
    return sqlalchemy.Table(
        "test_table",
        sqlalchemy.MetaData(),
        sqlalchemy.Column("id", sqlalchemy.Integer(), nullable=False),
        sqlalchemy.Column("data", sqlalchemy.Integer()),
        schema='public',
    )


@pytest.fixture
def s3(mocker):
    s3_mock = mock.MagicMock()
    mocker.patch.object(db_tables, "S3Data", return_value=s3_mock, autospec=True)

    return s3_mock


@pytest.fixture
def postgres_hook(mocker):
    postgres_hook_mock = mock.MagicMock()
    mocker.patch.object(
        db_tables, "PostgresHook", return_value=postgres_hook_mock, autospec=True
    )
    return postgres_hook_mock


def test_get_temp_table(table):
    assert get_temp_table(table, "temp").name == "test_table_temp"
    assert table.name == "test_table"


def test_create_temp_tables(mocker):
    mocker.patch.object(db_tables.sa, "create_engine", autospec=True)

    table = mock.Mock()
    mocker.patch.object(db_tables, "get_temp_table", autospec=True, return_value=table)

    db_tables.create_temp_tables("test-db", mock.Mock(), ts_nodash="123")

    table.create.assert_called_once_with(mock.ANY, checkfirst=True)


def test_insert_data_into_db(mocker, mock_db_conn, s3):
    table = mock.Mock()
    mocker.patch.object(db_tables, "get_temp_table", autospec=True, return_value=table)

    s3.iter_keys.return_value = [
        ('1', [{"id": 1, "extra": "ignored", "data": "text"}]),
        ('2', [{"id": 2}]),
    ]

    db_tables.insert_data_into_db(
        "test-db",
        table,
        [
            ("id", sqlalchemy.Column("id", sqlalchemy.Integer(), nullable=False)),
            ("data", sqlalchemy.Column("data", sqlalchemy.String())),
        ],
        ts_nodash="123",
    )

    mock_db_conn.execute.assert_has_calls(
        [
            mock.call(table.insert(), data="text", id=1),
            mock.call(table.insert(), data=None, id=2),
        ]
    )

    s3.iter_keys.assert_called_once_with()


def test_insert_data_into_db_using_db_config(mocker, mock_db_conn, s3):
    s3.iter_keys.return_value = [
        ('1', [{"id": 1, "extra": "ignored", "data": "text"}]),
        ('2', [{"id": 2}]),
    ]

    table_config = TableConfig(
        table_name="my-table",
        field_mapping=(
            ("id", sqlalchemy.Column("id", sqlalchemy.Integer(), nullable=False)),
            ("data", sqlalchemy.Column("data", sqlalchemy.String())),
        ),
    )
    mock_table = mock.Mock()
    mocker.patch.object(table_config, '_table', mock_table)
    mocker.patch.object(table_config, '_temp_table', mock_table)

    db_tables.insert_data_into_db(
        target_db="test-db", table_config=table_config, ts_nodash="123",
    )

    mock_db_conn.execute.assert_has_calls(
        [
            mock.call(mock_table.insert(), data="text", id=1),
            mock.call(mock_table.insert(), data=None, id=2),
        ]
    )

    s3.iter_keys.assert_called_once_with()


def test_insert_data_into_db_required_field_missing(mocker, mock_db_conn, s3):
    table = mock.Mock()
    mocker.patch.object(db_tables, "get_temp_table", autospec=True, return_value=table)

    s3.iter_keys.return_value = [('1', [{"data": "text"}])]

    with pytest.raises(KeyError):
        db_tables.insert_data_into_db(
            "test-db",
            table,
            [("id", sqlalchemy.Column("id", sqlalchemy.Integer(), nullable=False))],
            ts_nodash="123",
        )


def test_check_table(table):
    conn = mock.Mock()
    conn.execute().fetchone.return_value = [10]

    db_tables._check_table(mock.Mock(), conn, table, table, False)


def test_check_table_raises_if_new_table_size_is_smaller(table):
    conn = mock.Mock()
    conn.execute().fetchone.side_effect = [[8], [10]]

    with pytest.raises(db_tables.MissingDataError):
        db_tables._check_table(mock.Mock(), conn, table, table, False)


def test_check_table_raises_for_empty_columns(table):
    conn = mock.Mock()
    conn.execute().fetchone.side_effect = [[10], [10], 1, None]

    with pytest.raises(db_tables.UnusedColumnError):
        db_tables._check_table(mock.Mock(), conn, table, table, False)


def test_check_table_disable_env_empty_column_check(mocker, table):
    mocker.patch.object(db_tables.config, 'ALLOW_NULL_DATASET_COLUMNS', True)
    conn = mock.Mock()
    conn.execute().fetchone.side_effect = [[10], [10], 1, None]
    db_tables._check_table(mock.Mock(), conn, table, table, False)


def test_check_table_disable_empty_column_check(mocker, table):
    mocker.patch.object(db_tables.config, 'ALLOW_NULL_DATASET_COLUMNS', False)
    conn = mock.Mock()
    conn.execute().fetchone.side_effect = [[10], [10], 1, None]
    db_tables._check_table(mock.Mock(), conn, table, table, True)


def test_check_table_data(mock_db_conn, mocker, table):
    check_table = mocker.patch.object(db_tables, '_check_table')

    db_tables.check_table_data("test-db", table, ts_nodash="123")

    check_table.assert_called_once_with(mock.ANY, mock_db_conn, mock.ANY, table, False)


def test_query_database_fetched_in_batches(mock_db_conn, postgres_hook, s3):
    query = 'query'
    target_db = 'target_db'
    table_name = 'table_name'
    batch_size = 3
    ts_nodash = '2019010101:00'
    connection = mock.Mock()
    cursor = mock.Mock()
    description = [['column_1'], ['column_2'], ['column_3']]
    rows = [[[0, 1, 2], [3, 4, 5]], []]
    cursor.description = description
    cursor.fetchmany.side_effect = rows
    connection.cursor.return_value = cursor
    postgres_hook.get_conn.return_value = connection

    db_tables.query_database(
        query, target_db, table_name, batch_size, ts_nodash=ts_nodash
    )

    cursor.execute.assert_called_once_with(query)
    assert cursor.fetchmany.call_count == 2
    cursor.fetchmany.assert_called_with(batch_size)


def test_query_database_converts_query_result_to_json(mock_db_conn, postgres_hook, s3):
    query = 'query'
    target_db = 'target_db'
    table_name = 'table_name'
    batch_size = 3
    ts_nodash = '2019010101:00'
    connection = mock.Mock()
    cursor = mock.Mock()
    description = [['column_1'], ['column_2'], ['column_3']]
    rows = [[[0, 1, 2], [3, 4, 5]], []]
    cursor.description = description
    cursor.fetchmany.side_effect = rows
    connection.cursor.return_value = cursor
    postgres_hook.get_conn.return_value = connection
    expected_records = [
        {'column_1': 0, 'column_2': 1, 'column_3': 2},
        {'column_1': 3, 'column_2': 4, 'column_3': 5},
    ]

    db_tables.query_database(
        query, target_db, table_name, batch_size, ts_nodash=ts_nodash
    )

    s3.write_key.assert_called_once_with('0000000001.json', expected_records)


def test_query_database_closes_cursor_and_connection(mock_db_conn, postgres_hook, s3):
    query = 'query'
    target_db = 'target_db'
    table_name = 'table_name'
    batch_size = 3
    ts_nodash = '2019010101:00'
    connection = mock.Mock()
    cursor = mock.Mock()
    description = [['column_1'], ['column_2'], ['column_3']]
    rows = [[[0, 1, 2], [3, 4, 5]], []]
    cursor.description = description
    cursor.fetchmany.side_effect = rows
    connection.cursor.return_value = cursor
    postgres_hook.get_conn.return_value = connection

    db_tables.query_database(
        query, target_db, table_name, batch_size, ts_nodash=ts_nodash
    )

    cursor.close.assert_called_once_with()
    connection.close.assert_called_once_with()


def test_swap_dataset_tables(mock_db_conn, table, mocker):
    mock_db_conn.execute().fetchall.return_value = (('testuser',),)
    mocker.patch.object(
        db_tables.config,
        'DEFAULT_DATABASE_GRANTEES',
        ['default-grantee-1', 'default-grantee-2'],
    )
    xcom_mock = mock.Mock()
    xcom_mock.xcom_pull.return_value = datetime(2020, 1, 1, 12, 0, 0)

    with freezegun.freeze_time('20200202t12:00:00'):
        db_tables.swap_dataset_tables(
            "test-db", table, ts_nodash="123", task_instance=xcom_mock
        )

    mock_db_conn.execute.assert_has_calls(
        [
            call(),
            call('SET statement_timeout = 600000'),
            call(
                '''
                SELECT grantee
                FROM information_schema.role_table_grants
                WHERE table_name='QUOTED<test_table>'
                AND privilege_type = 'SELECT'
                AND grantor != grantee
                '''
            ),
            call().fetchall(),
            call(
                '''
                ALTER TABLE IF EXISTS QUOTED<public>.QUOTED<test_table> RENAME TO QUOTED<test_table_123_swap>;
                ALTER TABLE QUOTED<public>.QUOTED<test_table_123> RENAME TO QUOTED<test_table>;
                '''
            ),
            call('GRANT SELECT ON QUOTED<public>.QUOTED<test_table> TO testuser'),
            call(
                'GRANT SELECT ON QUOTED<public>.QUOTED<test_table> TO default-grantee-1'
            ),
            call(
                'GRANT SELECT ON QUOTED<public>.QUOTED<test_table> TO default-grantee-2'
            ),
            call(
                """
                INSERT INTO dataflow.metadata
                (table_schema, table_name, source_data_modified_utc, dataflow_swapped_tables_utc)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    'public',
                    'test_table',
                    datetime(2020, 1, 1, 12, 0),
                    datetime(2020, 2, 2, 12, 0),
                ),
            ),
        ]
    )


def test_drop_temp_tables(mocker, mock_db_conn):
    tables = [mock.Mock()]
    tables[0].name = "test_table"

    target = mocker.patch.object(db_tables, "get_temp_table", side_effect=tables)

    db_tables.drop_temp_tables("test-db", mock.Mock(), ts_nodash="123")

    target.assert_called_once_with(mock.ANY, "123")

    for table in tables:
        table.drop.assert_called_once_with(mock_db_conn, checkfirst=True)


def test_drop_swap_tables(mocker, mock_db_conn):
    tables = [mock.Mock()]
    tables[0].name = "test_table_swap"

    target = mocker.patch.object(db_tables, "get_temp_table", side_effect=tables)

    db_tables.drop_swap_tables("test-db", mock.Mock(), ts_nodash="123")

    target.assert_called_once_with(mock.ANY, "123_swap")

    for table in tables:
        table.drop.assert_called_once_with(mock_db_conn, checkfirst=True)


@pytest.mark.parametrize(
    "db_result, new_modified_utc, expected_result",
    (
        (((None,),), datetime(2020, 1, 2), "continue"),
        (((datetime(2020, 1, 1),),), datetime(2020, 1, 2), "continue"),
        (((datetime(2020, 1, 1),),), datetime(2019, 12, 31), "stop"),
        (tuple(), None, "continue"),
    ),
)
def test_branch_on_modified_date(
    mocker, mock_db_conn, db_result, new_modified_utc, expected_result
):
    target_db = 'target_db'
    table_config = TableConfig(
        table_name="my-table",
        field_mapping=(
            ("id", sqlalchemy.Column("id", sqlalchemy.Integer(), nullable=False)),
            ("data", sqlalchemy.Column("data", sqlalchemy.String())),
        ),
    )
    task_instance_mock = mock.Mock()
    task_instance_mock.xcom_pull.return_value = new_modified_utc
    mock_db_conn.execute().fetchall.return_value = db_result

    next_task = branch_on_modified_date(
        target_db, table_config, task_instance=task_instance_mock
    )

    assert next_task == expected_result


class TestPollForNewData:
    class TestPipeline(_FastPollingPipeline):
        date_checker = lambda: (datetime.now(), datetime.now())  # noqa
        data_getter = mock.Mock()
        daily_end_time_utc = time(17, 0, 0)
        allow_null_columns = False
        table_config = TableConfig(
            schema='test', table_name="test_table", field_mapping=[],
        )

    @pytest.mark.parametrize(
        'run_time_utc, db_data_last_modified_utc, source_last_modified_utc, source_next_release_utc, should_skip',
        (
            (  # We have the latest data, and new data isn't expected today, so skip the rest of the pipeline.
                datetime(2020, 1, 15),
                datetime(2020, 1, 1),
                datetime(2020, 1, 1),
                datetime(2020, 2, 1),
                True,
            ),
            (  # Newer data is available, so we shouldn't skip anything.
                datetime(2020, 1, 15),
                datetime(2019, 12, 1),
                datetime(2020, 1, 1),
                datetime(2020, 2, 1),
                False,
            ),
            (  # Newer data is available but the data provider has missed a release - we should still keep going.
                datetime(2020, 2, 15),
                datetime(2020, 1, 1),
                datetime(2020, 2, 1),
                datetime(2020, 3, 1),
                False,
            ),
        ),
    )
    def test_skips_downstream_tasks_if_has_latest_data_and_not_expecting_imminent_release(
        self,
        run_time_utc,
        db_data_last_modified_utc,
        source_last_modified_utc,
        source_next_release_utc,
        should_skip,
        mocker,
        monkeypatch,
    ):
        engine = mocker.patch.object(db_tables.sa, "create_engine", autospec=True)
        conn = engine.return_value.begin.return_value.__enter__.return_value
        conn.execute.return_value.fetchall.return_value = (
            (db_data_last_modified_utc,),
        )
        mocker.patch("dataflow.operators.db_tables.sleep", autospec=True)
        monkeypatch.setattr(
            self.TestPipeline,
            'date_checker',
            lambda: (source_last_modified_utc, source_next_release_utc),
        )
        mock_skip = mocker.patch.object(
            self.TestPipeline, 'skip_downstream_tasks', autospec=True
        )

        kwargs = dict(
            ts_nodash='123',
            dag=mock.Mock(),
            dag_run=mock.Mock(),
            ti=mock.Mock(),
            task_instance=mock.Mock(),
        )

        with freezegun.freeze_time(run_time_utc):
            db_tables.poll_for_new_data(
                "test-db", self.TestPipeline.table_config, self.TestPipeline(), **kwargs
            )

        if should_skip:
            assert len(mock_skip.call_args_list) == 1
            assert kwargs['task_instance'].xcom_push.call_args_list == []
        else:
            assert len(mock_skip.call_args_list) == 0
            assert kwargs['task_instance'].xcom_push.call_args_list == [
                mock.call('source-modified-date-utc', source_last_modified_utc)
            ]

    def test_skips_downstream_tasks_if_polling_time_is_after_daily_end_time(
        self, mocker, monkeypatch,
    ):
        engine = mocker.patch.object(db_tables.sa, "create_engine", autospec=True)
        conn = engine.return_value.begin.return_value.__enter__.return_value
        conn.execute.return_value.fetchall.return_value = ((datetime(2020, 1, 1),),)
        mocker.patch("dataflow.operators.db_tables.sleep", autospec=True)
        monkeypatch.setattr(
            self.TestPipeline,
            'date_checker',
            lambda: (datetime(2020, 1, 1), datetime(2020, 2, 1)),
        )
        mock_skip = mocker.patch.object(
            self.TestPipeline, 'skip_downstream_tasks', autospec=True
        )

        kwargs = dict(
            ts_nodash='123',
            dag=mock.Mock(),
            dag_run=mock.Mock(),
            ti=mock.Mock(),
            task_instance=mock.Mock(),
        )

        with freezegun.freeze_time('20200115t19:00:00'):
            db_tables.poll_for_new_data(
                "test-db", self.TestPipeline.table_config, self.TestPipeline(), **kwargs
            )

        assert len(mock_skip.call_args_list) == 1
        assert kwargs['task_instance'].xcom_push.call_args_list == []

    def test_pushes_source_modified_date_if_newer_data_is_available(
        self, mocker, monkeypatch,
    ):
        engine = mocker.patch.object(db_tables.sa, "create_engine", autospec=True)
        conn = engine.return_value.begin.return_value.__enter__.return_value
        conn.execute.return_value.fetchall.return_value = ((datetime(2019, 12, 1),),)
        mocker.patch("dataflow.operators.db_tables.sleep", autospec=True)
        monkeypatch.setattr(
            self.TestPipeline,
            'date_checker',
            lambda: (datetime(2020, 1, 1), datetime(2020, 2, 1)),
        )
        mock_skip = mocker.patch.object(
            self.TestPipeline, 'skip_downstream_tasks', autospec=True
        )

        kwargs = dict(
            ts_nodash='123',
            dag=mock.Mock(),
            dag_run=mock.Mock(),
            ti=mock.Mock(),
            task_instance=mock.Mock(),
        )

        with freezegun.freeze_time('20200101t12:00:00'):
            db_tables.poll_for_new_data(
                "test-db", self.TestPipeline.table_config, self.TestPipeline(), **kwargs
            )

        assert len(mock_skip.call_args_list) == 0
        assert kwargs['task_instance'].xcom_push.call_args_list == [
            mock.call('source-modified-date-utc', datetime(2020, 1, 1))
        ]


def test_poll_scrape_and_load_data(mocker):
    class TestPipeline(_FastPollingPipeline):
        date_checker = lambda: (datetime.now(), datetime.now())  # noqa
        data_getter = mock.Mock()
        daily_end_time_utc = time(17, 0, 0)
        allow_null_columns = False
        table_config = TableConfig(
            schema='test', table_name="test_table", field_mapping=[],
        )

    table = mock.Mock()
    engine = mocker.patch.object(db_tables.sa, "create_engine", autospec=True)
    conn = engine.return_value.begin.return_value.__enter__.return_value
    mock_temp_table = mocker.patch.object(
        db_tables, "get_temp_table", autospec=True, return_value=table
    )
    mock_temp_table.return_value.name = 'tmp_test_table'
    mock_temp_table.return_value.schema = 'test'
    mock_check_table_data = mocker.patch.object(
        db_tables, "check_table_data", autospec=True
    )
    mocker.patch("dataflow.operators.db_tables.sleep", autospec=True)

    kwargs = dict(
        ts_nodash='123',
        dag=mock.Mock(),
        dag_run=mock.Mock(),
        ti=mock.Mock(),
        task_instance=mock.Mock(),
    )

    with freezegun.freeze_time('20200101t19:00:00'):
        db_tables.scrape_load_and_check_data(
            "test-db", TestPipeline.table_config, TestPipeline(), **kwargs
        )

    assert any(
        c == mock.call('CREATE SCHEMA IF NOT EXISTS test')
        for c in conn.execute.call_args_list
    )
    assert TestPipeline.data_getter.return_value.to_sql.call_args_list == [
        mock.call(
            name='tmp_test_table',
            schema='test',
            con=engine.return_value.connect.return_value.__enter__.return_value,
            method='multi',
            if_exists='append',
            chunksize=10000,
            index=False,
        )
    ]
    assert mock_check_table_data.call_args_list == [
        mock.call(
            'test-db',
            mock.ANY,
            allow_null_columns=False,
            ts_nodash='123',
            dag=mock.ANY,
            dag_run=mock.ANY,
            ti=mock.ANY,
            task_instance=mock.ANY,
        )
    ]
