"""Defines pipeline DAGs for People Finder, the people directory on DIT's Digital Workspace"""
import datetime
from base64 import b64decode
from functools import partial

import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator
from sqlalchemy.dialects.postgresql import UUID, JSONB

from dataflow import config
from dataflow.dags import _PipelineDAG
from dataflow.operators.common import fetch_from_jwt_api
from dataflow.utils import TableConfig


class PeopleFinderPeoplePipeline(_PipelineDAG):
    schedule_interval = "@daily"
    use_utc_now_as_source_modified = True
    path = "/api/v2/data_workspace_export"
    source_url = f"{config.PEOPLE_FINDER_BASE_URL}{path}"
    table_config = TableConfig(
        table_name='people_finder__people',
        field_mapping=[
            (
                "people_finder_id",
                sa.Column("people_finder_id", sa.Integer, primary_key=True),
            ),
            ("staff_sso_id", sa.Column("staff_sso_id", UUID)),
            ("email", sa.Column("email", sa.Text)),
            ("contact_email", sa.Column("contact_email", sa.Text)),
            ("full_name", sa.Column("full_name", sa.Text)),
            ("first_name", sa.Column("first_name", sa.Text)),
            ("last_name", sa.Column("last_name", sa.Text)),
            ("profile_url", sa.Column("profile_url", sa.Text)),
            ("roles", sa.Column("roles", JSONB)),
            ("formatted_roles", sa.Column("formatted_roles", sa.ARRAY(sa.String))),
            (
                "manager_people_finder_id",
                sa.Column("manager_people_finder_id", sa.Integer),
            ),
            ("completion_score", sa.Column("completion_score", sa.Integer)),
            ("is_stale", sa.Column("is_stale", sa.Boolean)),
            ("works_monday", sa.Column("works_monday", sa.Boolean)),
            ("works_tuesday", sa.Column("works_tuesday", sa.Boolean)),
            ("works_wednesday", sa.Column("works_wednesday", sa.Boolean)),
            ("works_thursday", sa.Column("works_thursday", sa.Boolean)),
            ("works_friday", sa.Column("works_friday", sa.Boolean)),
            ("works_saturday", sa.Column("works_saturday", sa.Boolean)),
            ("works_sunday", sa.Column("works_sunday", sa.Boolean)),
            ("primary_phone_number", sa.Column("primary_phone_number", sa.Text)),
            ("secondary_phone_number", sa.Column("secondary_phone_number", sa.Text)),
            ("formatted_location", sa.Column("formatted_location", sa.Text)),
            ("buildings", sa.Column("buildings", sa.ARRAY(sa.String))),
            ("formatted_buildings", sa.Column("formatted_buildings", sa.Text)),
            ("city", sa.Column("city", sa.Text)),
            ("country", sa.Column("country", sa.Text)),
            ("country_name", sa.Column("country_name", sa.Text)),
            ("grade", sa.Column("grade", sa.Text)),
            ("formatted_grade", sa.Column("formatted_grade", sa.Text)),
            ("location_in_building", sa.Column("location_in_building", sa.Text)),
            ("location_other_uk", sa.Column("location_other_uk", sa.Text)),
            ("location_other_overseas", sa.Column("location_other_overseas", sa.Text)),
            ("key_skills", sa.Column("key_skills", sa.ARRAY(sa.String))),
            ("other_key_skills", sa.Column("other_key_skills", sa.Text)),
            ("formatted_key_skills", sa.Column("formatted_key_skills", sa.Text)),
            (
                "learning_and_development",
                sa.Column("learning_and_development", sa.ARRAY(sa.String)),
            ),
            (
                "other_learning_and_development",
                sa.Column("other_learning_and_development", sa.Text),
            ),
            (
                "formatted_learning_and_development",
                sa.Column("formatted_learning_and_development", sa.Text),
            ),
            ("networks", sa.Column("networks", sa.ARRAY(sa.String))),
            ("formatted_networks", sa.Column("formatted_networks", sa.Text)),
            ("professions", sa.Column("professions", sa.ARRAY(sa.String))),
            ("formatted_professions", sa.Column("formatted_professions", sa.Text)),
            (
                "additional_responsibilities",
                sa.Column("additional_responsibilities", sa.ARRAY(sa.String)),
            ),
            (
                "other_additional_responsibilities",
                sa.Column("other_additional_responsibilities", sa.Text),
            ),
            (
                "formatted_additional_responsibilities",
                sa.Column("formatted_additional_responsibilities", sa.Text),
            ),
            ("language_fluent", sa.Column("language_fluent", sa.Text)),
            ("language_intermediate", sa.Column("language_intermediate", sa.Text)),
            ("created_at", sa.Column("created_at", sa.DateTime)),
            (
                "last_edited_or_confirmed_at",
                sa.Column("last_edited_or_confirmed_at", sa.DateTime),
            ),
            ("login_count", sa.Column("login_count", sa.Integer)),
            ("last_login_at", sa.Column("last_login_at", sa.DateTime)),
        ],
    )

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id="run-fetch",
            python_callable=partial(
                fetch_from_jwt_api,
                secret=b64decode(str(config.PEOPLE_FINDER_PRIVATE_KEY), validate=True),
                algorithm="RS512",
                payload_builder=lambda: {
                    "fullpath": self.path,
                    "exp": datetime.datetime.utcnow() + datetime.timedelta(seconds=10),
                },
            ),
            provide_context=True,
            op_args=[self.table_config.table_name, self.source_url],
            retries=self.fetch_retries,
        )
