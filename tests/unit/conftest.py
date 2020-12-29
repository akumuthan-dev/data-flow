from unittest import mock

import pytest


@pytest.fixture
def mock_db_conn(mocker):
    engine = mock.MagicMock()
    mocker.patch("sqlalchemy.create_engine", return_value=engine, autospec=True)

    engine.dialect.identifier_preparer.quote = lambda x: f"QUOTED<{x}>"

    conn = mock.Mock()
    engine.begin.return_value.__enter__.return_value = conn

    return conn
