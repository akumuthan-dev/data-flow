from datetime import datetime, time
from unittest import mock
from unittest.mock import call

import freezegun
import pandas
import pytest
import sqlalchemy

from dataflow.dags import _PandasPipelineWithPollingSupport
from dataflow.operators import db_tables
from dataflow.operators.db_tables import branch_on_modified_date
from dataflow.utils import get_temp_table, TableConfig, SingleTableConfig, LateIndex


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


@pytest.mark.parametrize(
    "xcom_modified_date, use_utc_now_as_source_modified, expected_result",
    (
        (datetime(2020, 1, 1, 12, 0, 0), False, datetime(2020, 1, 1, 12, 0)),
        (None, True, datetime(2020, 2, 2, 12, 0)),
    ),
)
def test_swap_dataset_tables(
    mock_db_conn,
    table,
    mocker,
    xcom_modified_date,
    use_utc_now_as_source_modified,
    expected_result,
):
    mock_db_conn.execute().fetchall.return_value = (('testuser',),)
    mocker.patch.object(
        db_tables.config,
        'DEFAULT_DATABASE_GRANTEES',
        ['default-grantee-1', 'default-grantee-2'],
    )
    xcom_mock = mock.Mock()
    xcom_mock.xcom_pull.return_value = xcom_modified_date

    mock_get_task_instance = mocker.patch(
        'dataflow.operators.db_tables.get_task_instance'
    )
    mock_get_task_instance().end_date = expected_result

    with freezegun.freeze_time('20200202t12:00:00'):
        db_tables.swap_dataset_tables(
            "test-db",
            table,
            ts_nodash="123",
            task_instance=xcom_mock,
            use_utc_now_as_source_modified=use_utc_now_as_source_modified,
            dag=mock.Mock(),
            execution_date=datetime(2020, 2, 2, 12, 0),
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
                SELECT dataflow.save_and_drop_dependencies('QUOTED<public>', 'QUOTED<test_table>');
                ALTER TABLE IF EXISTS QUOTED<public>.QUOTED<test_table> RENAME TO QUOTED<test_table_123_swap>;
                ALTER TABLE QUOTED<public>.QUOTED<test_table_123> RENAME TO QUOTED<test_table>;
                SELECT dataflow.restore_dependencies('QUOTED<public>', 'QUOTED<test_table>');
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
                ('public', 'test_table', expected_result, datetime(2020, 2, 2, 12, 0),),
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


def test_create_post_insert_indexes(mocker):
    table_config = TableConfig(
        'test_table',
        field_mapping=(
            ('id', sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True)),
            ('val', sqlalchemy.Column('val', sqlalchemy.Integer, primary_key=True)),
        ),
        indexes=[LateIndex('id'), LateIndex('val'), LateIndex(['id', 'val'])],
    )
    mock_create_engine = mocker.patch.object(
        db_tables.sa, "create_engine", autospec=True
    )
    mock_conn = (
        mock_create_engine.return_value.begin.return_value.__enter__.return_value
    )

    table = mock.Mock()
    mocker.patch.object(db_tables, "get_temp_table", autospec=True, return_value=table)

    db_tables.create_temp_table_indexes("test-db", table_config, ts_nodash="123")

    # Check created Index name
    assert (
        mock_conn._run_visitor.call_args_list[0][0][1].name
        == '123_59f1e1147ff2da92e1ca2f3a52ec8157_idx'
    )
    assert (
        mock_conn._run_visitor.call_args_list[1][0][1].name
        == '123_9211871718759480d33d0e473cda168c_idx'
    )
    assert (
        mock_conn._run_visitor.call_args_list[2][0][1].name
        == '123_acf742aa585d24563579f75baa594e81_idx'
    )

    # Check created Index column(s)
    assert mock_conn._run_visitor.call_args_list[0][0][1].expressions == ['id']
    assert mock_conn._run_visitor.call_args_list[1][0][1].expressions == ['val']
    assert mock_conn._run_visitor.call_args_list[2][0][1].expressions == ['id', 'val']


class TestPollForNewData:
    class TestPipeline(_PandasPipelineWithPollingSupport):
        @staticmethod
        def date_checker():
            return (datetime.now(), datetime.now())

        data_getter = mock.Mock()
        daily_end_time_utc = time(17, 0, 0)
        allow_null_columns = False
        table_config = SingleTableConfig(
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
                "test-db",
                self.TestPipeline.table_config,  # pylint: disable=no-member
                self.TestPipeline(),
                **kwargs
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
                "test-db",
                self.TestPipeline.table_config,  # pylint: disable=no-member
                self.TestPipeline(),
                **kwargs
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
                "test-db",
                self.TestPipeline.table_config,  # pylint: disable=no-member
                self.TestPipeline(),
                **kwargs
            )

        assert len(mock_skip.call_args_list) == 0
        assert kwargs['task_instance'].xcom_push.call_args_list == [
            mock.call('source-modified-date-utc', datetime(2020, 1, 1))
        ]


def test_poll_scrape_and_load_data(mocker, monkeypatch):
    class TestPipeline(_PandasPipelineWithPollingSupport):
        @staticmethod
        def date_checker():
            return datetime.now()

        @staticmethod
        def data_getter():
            return [
                pandas.DataFrame(
                    [{"data_id": 1, "val": 50}, {"data_id": 2, "val": 999}]
                )
            ]

        daily_end_time_utc = time(17, 0, 0)
        allow_null_columns = False
        table_config = TableConfig(
            schema='test',
            table_name="test_table",
            field_mapping=[
                ("data_id", sqlalchemy.Column("db_id", sqlalchemy.Integer)),
                ("val", sqlalchemy.Column("val", sqlalchemy.Integer)),
            ],
        )

    monkeypatch.setenv(
        'AIRFLOW_CONN_DATASETS_DB', 'postgresql+psycopg2://foo:foo@db:5432/datasets'
    )
    table = mock.Mock()
    mock_create_tables = mocker.patch.object(
        db_tables, "create_temp_tables", autospec=True
    )
    mock_temp_table = mocker.patch.object(
        db_tables, "get_temp_table", autospec=True, return_value=table
    )
    mock_temp_table.return_value.name = 'tmp_test_table'
    mock_temp_table.return_value.schema = 'test'
    mocker.patch("dataflow.operators.db_tables.sleep", autospec=True)
    mock_postgres = mocker.patch(
        "dataflow.operators.db_tables.psycopg3.connect", autospec=True
    )
    mock_cursor = mock_postgres().__enter__().cursor()
    mock_copy_write = mock_cursor.__enter__().copy().__enter__().write

    kwargs = dict(
        ts_nodash='123',
        dag=mock.Mock(),
        dag_run=mock.Mock(),
        ti=mock.Mock(),
        task_instance=mock.Mock(),
    )

    with freezegun.freeze_time('20200101t19:00:00'):
        db_tables.scrape_load_and_check_data(
            "test-db",
            TestPipeline.table_config,  # pylint: disable=no-member
            TestPipeline(),
            **kwargs
        )

    assert mock_create_tables.call_args_list == [
        mock.call(
            "test-db",
            mock.ANY,
            ts_nodash='123',
            dag=mock.ANY,
            dag_run=mock.ANY,
            ti=mock.ANY,
            task_instance=mock.ANY,
        )
    ]
    assert mock_copy_write.call_args_list == [mock.call("1\t50\n2\t999\n")]


@mock.patch('dataflow.operators.db_tables.sa.create_engine', autospec=True)
def test_insert_bulk_data_into_db_single_table(mock_create_engine, s3):
    table_config = TableConfig(
        schema='test',
        table_name='test_table',
        field_mapping=[
            ('id', sqlalchemy.Column('id', sqlalchemy.Integer)),
            ('data', sqlalchemy.Column('data', sqlalchemy.String)),
        ],
    )
    mock_engine = mock.Mock()
    mock_create_engine.return_value = mock_engine

    s3.iter_keys.return_value = [
        ('1', [{"id": 1, "data": "sometext1"}, {"id": 2, "data": "sometext2"}]),
        (
            '2',
            [
                {
                    "id": 3,
                    "data": "sometext3",
                    "sub_records": [{"name": "a record"}, {"name": "another record"}],
                },
                {"id": 4, "data": "sometext4"},
            ],
        ),
    ]
    table_config._temp_table = mock.Mock()
    db_tables.bulk_insert_data_into_db(
        "test-db", table_config, ts_nodash="123",
    )
    mock_engine.execute.assert_has_calls(
        [
            mock.call(
                table_config.temp_table.insert(),
                [{'id': 1, 'data': 'sometext1'}, {'id': 2, 'data': 'sometext2'}],
            ),
            mock.call(
                table_config.temp_table.insert(),
                [{'id': 3, 'data': 'sometext3'}, {'id': 4, 'data': 'sometext4'}],
            ),
        ]
    )

    s3.iter_keys.assert_called_once_with()


@mock.patch('dataflow.operators.db_tables.sa.create_engine', autospec=True)
def test_insert_bulk_data_into_db_nested_tables(mock_create_engine, s3):
    sub_table_2_config = TableConfig(
        schema='test',
        table_name='subtable2',
        transforms=[
            lambda record, table_config, contexts: {
                **record,
                'parent_id': contexts[0]['id'],
            }
        ],
        field_mapping=[
            ('parent_id', sqlalchemy.Column('parent_id', sqlalchemy.Integer)),
            ('id', sqlalchemy.Column('id', sqlalchemy.Integer)),
            ('name', sqlalchemy.Column('name', sqlalchemy.String)),
        ],
    )
    sub_table_1_config = TableConfig(
        schema='test',
        table_name='subtable1',
        transforms=[
            lambda record, table_config, contexts: {
                **record,
                'parent_id': contexts[0]['id'],
            }
        ],
        field_mapping=[
            ('parent_id', sqlalchemy.Column('parent_id', sqlalchemy.Integer)),
            ('id', sqlalchemy.Column('id', sqlalchemy.Integer)),
            ('name', sqlalchemy.Column('name', sqlalchemy.String)),
            ('sub_records', sub_table_2_config),
        ],
    )
    main_table_config = TableConfig(
        schema='test',
        table_name='test_table',
        field_mapping=[
            ('id', sqlalchemy.Column('id', sqlalchemy.Integer)),
            ('name', sqlalchemy.Column('name', sqlalchemy.String)),
            ('sub_records', sub_table_1_config),
        ],
    )
    mock_engine = mock.Mock()
    mock_create_engine.return_value = mock_engine

    s3.iter_keys.return_value = [
        (
            '1',
            [
                {'id': 1, 'name': 'parent record 1'},
                {'id': 2, 'name': 'parent record 2'},
            ],
        ),
        (
            '2',
            [
                {
                    'id': 3,
                    'name': 'parent record 3',
                    'sub_records': [
                        {
                            'id': 4,
                            'name': 'sub record 1',
                            'sub_records': [
                                {'id': 5, 'name': 'sub-sub record 1'},
                                {'id': 6, 'name': 'sub-sub record 2'},
                            ],
                        },
                        {
                            'id': 7,
                            'name': 'sub record 2',
                            'sub_records': [{'id': 8, 'name': 'sub-sub record 3'}],
                        },
                    ],
                },
                {'id': 9, 'name': 'parent record 4'},
            ],
        ),
    ]
    main_table_config._temp_table = mock.Mock()
    sub_table_1_config._temp_table = mock.Mock()
    sub_table_2_config._temp_table = mock.Mock()
    db_tables.bulk_insert_data_into_db(
        "test-db", main_table_config, ts_nodash="123",
    )
    mock_engine.execute.assert_has_calls(
        [
            mock.call(
                main_table_config.temp_table.insert(),
                [
                    {'id': 1, 'name': 'parent record 1'},
                    {'id': 2, 'name': 'parent record 2'},
                ],
            ),
            mock.call(
                sub_table_2_config.temp_table.insert(),
                [
                    {'parent_id': 4, 'id': 5, 'name': 'sub-sub record 1'},
                    {'parent_id': 4, 'id': 6, 'name': 'sub-sub record 2'},
                    {'parent_id': 7, 'id': 8, 'name': 'sub-sub record 3'},
                ],
            ),
            mock.call(
                sub_table_1_config.temp_table.insert(),
                [
                    {'parent_id': 3, 'id': 4, 'name': 'sub record 1'},
                    {'parent_id': 3, 'id': 7, 'name': 'sub record 2'},
                ],
            ),
            mock.call(
                main_table_config.temp_table.insert(),
                [
                    {'id': 3, 'name': 'parent record 3'},
                    {'id': 9, 'name': 'parent record 4'},
                ],
            ),
        ]
    )

    s3.iter_keys.assert_called_once_with()
