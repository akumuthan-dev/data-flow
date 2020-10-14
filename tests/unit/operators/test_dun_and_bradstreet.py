from unittest import mock
import sqlalchemy as sa

from dataflow.operators import dun_and_bradstreet
from dataflow.utils import TableConfig


def test_update_dnb_table(mock_db_conn):
    # Given a target db, table and a query the query should be run against
    # the db with temp and target table names substituted in the query.
    table_config = TableConfig(
        schema='test_schema',
        table_name='test_table',
        field_mapping=(
            ('id', sa.Column('id', sa.Integer())),
            ('name', sa.Column('name', sa.Text())),
        ),
    )
    query = (
        'UPDATE {schema}.{target_table} '
        'SET name=tmp.name FROM {schema}.{from_table} tmp '
        'WHERE {target_table}.id=tmp.id'
    )
    dun_and_bradstreet.update_table(
        'test_db', table_config.tables[0], query, ts_nodash="123"
    )
    mock_db_conn.execute.assert_has_calls(
        [
            mock.call(
                'UPDATE QUOTED<test_schema>.QUOTED<test_table> SET name=tmp.name '
                'FROM QUOTED<test_schema>.QUOTED<test_table_123> tmp '
                'WHERE QUOTED<test_table>.id=tmp.id'
            ),
        ]
    )
