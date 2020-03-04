import collections
import io
import logging
import zipfile
from typing import Any
from typing import Callable
from typing import List
from typing import Tuple
from typing import Type
from typing import TypeVar

import backoff
import requests
import sqlalchemy as sa

from airflow.hooks.postgres_hook import PostgresHook
from dataflow import config
from dataflow.utils import FieldMapping
from dataflow.utils import get_nested_key
from dataflow.utils import S3Data


def digits(field: str) -> str:
    if field and field.isdigit():
        return field
    raise ValueError(f"Expected string of digits, got {repr(field)}")


def leading_sign_digits(field: str) -> int:
    if field and field[0] in "+-" and field[1:].isdigit():
        return int(field)
    raise ValueError(f"Expected string of digits with leading sign, got {repr(field)}")


def mm_yyyy(field: str) -> str:
    if field[:2].isdigit() and field[2] == "/" and field[3:].isdigit():
        month = int(field[:2])
        if 0 < month < 13:
            return field
    raise ValueError(f"Expected string in MM/YYYY format, got {repr(field)}")


RecordSubclass = TypeVar("RecordSubclass", bound="Record")


class Record(collections.UserDict):
    record_length: int
    schema: List[Tuple[str, Tuple[int, Callable[[str], Any]]]]

    def __init__(self, *args):
        super().__init__()
        if hasattr(self, "schema"):

            if len("|".join(args)) != self.record_length:
                raise ValueError("Invalid record length")

            if len(args) != len(self.schema):
                raise ValueError(
                    f"Wrong number of fields {len(args)}/{len(self.schema)}"
                )

            for i, arg in enumerate(args):
                field_name, field_schema = self.schema[i]
                field_length, field_type = field_schema
                if len(arg) != field_length:
                    raise ValueError(
                        f"Invalid length {len(arg)} for field {field_name}"
                        f" - expected {field_length}"
                    )
                self[field_name] = field_type(arg)


class ExportRecord(Record):
    """
    As described in
    https://www.uktradeinfo.com/Statistics/Documents/Data%20Downloads/Tech_Spec_SMKE19.DOC
    """

    record_length = 141
    schema = [
        ("comcode", (9, digits)),
        ("sitc", (5, digits)),
        ("record_type", (3, digits)),
        ("cod_sequence", (3, digits)),
        ("cod_alpha", (2, str)),
        ("account_mmyyyy", (7, mm_yyyy)),
        ("port_sequence", (3, digits)),
        ("port_alpha", (3, str)),
        ("flag_sequence", (3, digits)),
        ("flag_alpha", (2, str)),
        ("trade_indicator", (1, digits)),
        ("container", (3, digits)),
        ("mode_of_transport", (3, digits)),
        ("inland_mot", (2, digits)),
        ("golo_sequence", (3, digits)),
        ("golo_alpha", (3, str)),
        ("suite_indicator", (3, digits)),
        ("procedure_code", (3, digits)),
        ("value", (16, leading_sign_digits)),
        ("quantity_1", (14, leading_sign_digits)),
        ("quantity_2", (14, leading_sign_digits)),
        ("industrial_plant_comcode", (15, digits)),
    ]

    def __init__(self, *args):
        super().__init__(*args)
        mm, yyyy = self["account_mmyyyy"].split("/")
        self["account_mm"] = int(mm)
        self["account_ccyy"] = int(yyyy)
        del self["account_mmyyyy"]


class ImportRecord(Record):
    """
    As described in
    https://www.uktradeinfo.com/Statistics/Documents/Data%20Downloads/Tech_Spec_SMKI19.DOC
    """

    record_length = 143
    schema = [
        ("comcode", (9, digits)),
        ("sitc", (5, digits)),
        ("record_type", (3, digits)),
        ("cod_sequence", (3, digits)),
        ("cod_alpha", (2, str)),
        ("coo_sequence", (3, digits)),
        ("coo_alpha", (2, str)),
        ("account_mmyyyy", (7, mm_yyyy)),
        ("port_sequence", (3, digits)),
        ("port_alpha", (3, str)),
        ("flag_sequence", (3, digits)),
        ("flag_alpha", (2, str)),
        ("country_sequence", (3, digits)),
        ("country_alpha", (2, str)),
        ("trade_indicator", (1, digits)),
        ("container", (3, digits)),
        ("mode_of_transport", (3, digits)),
        ("inland_mot", (2, digits)),
        ("golo_sequence", (3, digits)),
        ("golo_alpha", (3, str)),
        ("suite_indicator", (3, digits)),
        ("procedure_code", (3, digits)),
        ("cb_code", (3, digits)),
        ("value", (16, leading_sign_digits)),
        ("quantity_1", (14, leading_sign_digits)),
        ("quantity_2", (14, leading_sign_digits)),
    ]

    def __init__(self, *args):
        super().__init__(*args)
        mm, yyyy = self["account_mmyyyy"].split("/")
        self["account_mm"] = int(mm)
        self["account_ccyy"] = int(yyyy)
        del self["account_mmyyyy"]


def fetch_non_eu_data_from_uktradeinfo(table_name: str, base_filename: str, **kwargs):
    s3 = S3Data(table_name, kwargs["ts_nodash"])
    run_date = kwargs.get("run_date", kwargs.get("execution_date"))
    filename = f"{base_filename}{run_date:%y%m}"
    source_url = f"{config.HMRC_UKTRADEINFO_URL}/{filename}.zip"

    data = _extract_zipped_contents(_download(source_url))
    s3.write_key(filename, data)

    logging.info(f"Fetched from source to {filename}")


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
def _download(source_url):
    response = requests.get(source_url)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        logging.error(f"Request failed: {response.text}")
        raise

    return response.content


def _extract_zipped_contents(file_contents):
    with zipfile.ZipFile(io.BytesIO(file_contents)) as zip:
        with zip.open(zip.namelist()[0], "r") as f:
            return f.read()


def insert_data_into_db(
    target_db: str,
    table: sa.Table,
    field_mapping: FieldMapping,
    record_type: Type[RecordSubclass],
    **kwargs,
):
    s3 = S3Data(table.name, kwargs["ts_nodash"])

    engine = sa.create_engine(
        "postgresql+psycopg2://",
        creator=PostgresHook(postgres_conn_id=target_db).get_conn,
    )
    temp_table = _get_temp_table(table, kwargs["ts_nodash"])

    for filename, data in s3.iter_keys():
        logging.info(f"Processing file {filename}")

        records = _parse_uktradeinfo_non_eu_data(data, record_type)

        with engine.begin() as conn:
            for record in _map_fields(records, field_mapping):
                conn.execute(temp_table.insert(), **record)

        logging.info(f"File {filename} ingested successfully")


def _get_temp_table(table, suffix):
    return sa.Table(
        f"{table.name}_{suffix}".lower(),
        table.metadata,
        *[column.copy() for column in table.columns],
    )


def _map_fields(records, field_mapping: FieldMapping):
    for record in records:
        try:
            yield {
                db_column.name: get_nested_key(record, field, not db_column.nullable)
                for field, db_column in field_mapping
                if field is not None
            }
        except KeyError:
            logging.warning(
                f"Failed to load item {record.get('id', '')}, required field is missing"
            )
            raise


def _parse_uktradeinfo_non_eu_data(data, record_type):
    for line in data.decode("utf-8").splitlines():
        if len(line) == record_type.record_length:
            yield record_type(*line.split("|"))
