import dateutil
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.base import Executable


class CreateView(Executable, ClauseElement):
    def __init__(self, name, select, materialized):
        self.name = name
        self.select = select
        self.materialized = materialized


@compiles(CreateView)
def create_view_compile(view, compiler, **_):
    query = compiler.process(view.select, literal_binds=True)
    return f'CREATE {"MATERIALIZED" if view.materialized else "OR REPLACE"} VIEW {view.name} AS {query}'


def create_timestamped_view_name(view_name, execution_date):
    start_date = execution_date + dateutil.relativedelta.relativedelta(
        months=+1, days=-1
    )
    return f"{view_name}_{start_date.strftime('%Y_%m_%d')}"
