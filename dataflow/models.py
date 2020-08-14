from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

Base: Any = declarative_base(metadata=sa.MetaData(schema='dataflow'))


class Metadata(Base):
    __tablename__ = 'metadata'

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    table_schema = sa.Column(sa.Text, nullable=False)
    table_name = sa.Column(sa.Text, nullable=False)
    source_data_modified_utc = sa.Column(sa.DateTime, nullable=True)
    dataflow_swapped_tables_utc = sa.Column(sa.DateTime, nullable=False)
