import collections
import io
import logging
import zipfile

import backoff
import requests

from dataflow import config
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


class SMKE19Record(collections.UserDict):
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


class ExportRecord(SMKE19Record):
    record_length = 141
    schema = (
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
    )

    def __init__(self, *args):
        super().__init__(*args)
        self["account_mm"] = int(self["account_mmyyyy"][:2])
        self["account_ccyy"] = int(self["account_mmyyyy"][3:])
        del self["account_mmyyyy"]


def fetch_from_uktradeinfo(table_name: str, filename: str, **kwargs):
    s3 = S3Data(table_name, kwargs["ts_nodash"])
    source_url = f"{config.HMRC_UKTRADEINFO_URL}/{filename}"

    data = _download_datafile(source_url)
    parsed_records = [
        ExportRecord(*line.split("|"))
        for line in data.decode("utf-8").splitlines()
        if len(line) == ExportRecord.record_length
    ]
    total = len(parsed_records)
    s3.write_key(f"{1:010}.json", parsed_records)

    logging.info(f"Fetching from source completed, total {total} records")


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
def _download_datafile(source_url):
    response = requests.get(source_url)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        logging.error(f"Request failed: {response.text}")
        raise

    with zipfile.ZipFile(io.BytesIO(response.content)) as zip:
        with zip.open(zip.namelist()[0], "r") as f:
            return f.read()
