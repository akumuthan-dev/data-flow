from unittest import mock

import pytest
import sqlalchemy

from dataflow.operators import db_tables


@pytest.fixture
def table():
    return sqlalchemy.Table(
        "test_table",
        sqlalchemy.MetaData(),
        sqlalchemy.Column("id", sqlalchemy.Integer(), nullable=False),
        sqlalchemy.Column("data", sqlalchemy.Integer()),
    )


def test_get_temp_table(table):
    assert db_tables._get_temp_table(table, "temp").name == "test_table_temp"
    assert table.name == "test_table"


def test_create_temp_tables(mocker):
    mocker.patch.object(db_tables.sa, "create_engine", autospec=True)

    table = mock.Mock()
    mocker.patch.object(db_tables, "_get_temp_table", autospec=True, return_value=table)

    db_tables.create_temp_tables("test-db", mock.Mock(), ts_nodash="123")

    table.create.assert_called_once_with(mock.ANY, checkfirst=True)


def test_insert_data_into_db(mocker, mock_db_conn):
    table = mock.Mock()
    mocker.patch.object(db_tables, "_get_temp_table", autospec=True, return_value=table)

    redis = mock.Mock()
    mocker.patch.object(
        db_tables, "get_redis_client", return_value=redis, autospec=True
    )
    redis.lrange.return_value = ["page-var-1", "page-var-2"]

    var = mocker.patch.object(db_tables, "Variable", autospec=True)
    var.get.return_value = [{"id": 1, "extra": "ignored", "data": "text"}, {"id": 2}]

    db_tables.insert_data_into_db(
        "test-db",
        table,
        [
            ("id", sqlalchemy.Column("id", sqlalchemy.Integer(), nullable=False)),
            ("data", sqlalchemy.Column("data", sqlalchemy.String())),
        ],
        "task-1",
        ts_nodash="123",
    )

    mock_db_conn.execute.assert_has_calls(
        [
            mock.call(table.insert(), data="text", id=1),
            mock.call(table.insert(), data=None, id=2),
            mock.call(table.insert(), data="text", id=1),
            mock.call(table.insert(), data=None, id=2),
        ]
    )

    redis.lrange.assert_called_once_with("task-1", 0, -1)
    redis.delete.assert_called_once_with("task-1")

    var.get.assert_has_calls(
        [
            mock.call("page-var-1", deserialize_json=True),
            mock.call("page-var-2", deserialize_json=True),
        ]
    )
    var.delete.assert_has_calls([mock.call("page-var-1"), mock.call("page-var-2")])


def test_insert_data_into_db_required_field_missing(mocker, mock_db_conn):
    table = mock.Mock()
    mocker.patch.object(db_tables, "_get_temp_table", autospec=True, return_value=table)

    redis = mock.Mock()
    mocker.patch.object(
        db_tables, "get_redis_client", return_value=redis, autospec=True
    )
    redis.lrange.return_value = ["page-var-1"]

    var = mocker.patch.object(db_tables, "Variable", autospec=True)
    var.get.return_value = [{"data": "text"}]

    with pytest.raises(KeyError):
        db_tables.insert_data_into_db(
            "test-db",
            table,
            [("id", sqlalchemy.Column("id", sqlalchemy.Integer(), nullable=False))],
            "task-1",
            ts_nodash="123",
        )

    assert not redis.delete.called
    assert not var.delete.called


def test_check_table(table):
    conn = mock.Mock()
    conn.execute().fetchone.return_value = [10]

    db_tables._check_table(mock.Mock(), conn, table, table)


def test_check_table_raises_if_new_table_size_is_smaller(table):
    conn = mock.Mock()
    conn.execute().fetchone.side_effect = [[8], [10]]

    with pytest.raises(db_tables.MissingDataError):
        db_tables._check_table(mock.Mock(), conn, table, table)


def test_check_table_raises_for_empty_columns(table):
    conn = mock.Mock()
    conn.execute().fetchone.side_effect = [[10], [10], 1, None]

    with pytest.raises(db_tables.UnusedColumnError):
        db_tables._check_table(mock.Mock(), conn, table, table)


def test_check_table_data(mock_db_conn, mocker, table):
    check_table = mocker.patch.object(db_tables, '_check_table')

    db_tables.check_table_data("test-db", table, ts_nodash="123")

    check_table.assert_called_once_with(mock.ANY, mock_db_conn, mock.ANY, table)


def test_swap_dataset_table(mock_db_conn, table):
    db_tables.swap_dataset_table("test-db", table, ts_nodash="123")

    mock_db_conn.execute.assert_called_once_with(
        """
            ALTER TABLE IF EXISTS QUOTED<test_table> RENAME TO QUOTED<test_table_123_swap>;
            ALTER TABLE QUOTED<test_table_123> RENAME TO QUOTED<test_table>;
            """
    )


def test_drop_temp_tables(mocker, mock_db_conn):
    tables = [mock.Mock(), mock.Mock()]
    tables[0].name = "test_table"
    tables[1].name = "swap_table"

    mocker.patch.object(db_tables, "_get_temp_table", side_effect=tables)

    db_tables.drop_temp_tables("test-db", mock.Mock(), ts_nodash="123")

    for table in tables:
        table.drop.assert_called_once_with(mock_db_conn, checkfirst=True)
