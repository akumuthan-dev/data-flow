from functools import partial

import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator

from dataflow.dags import _PipelineDAG
from dataflow.operators.common import fetch_from_hosted_csv
from dataflow.utils import TableConfig


class OxfordCovid19GovernmentResponseTracker(_PipelineDAG):
    source_url = 'https://oxcgrtportal.azurewebsites.net/api/CSVDownload'
    allow_null_columns = True
    table_config = TableConfig(
        table_name='oxford_covid19_government_response_tracker',
        field_mapping=[
            ('CountryName', sa.Column('country_name', sa.String)),
            ('CountryCode', sa.Column('country_code', sa.String)),
            ('Date', sa.Column('date', sa.Numeric)),
            ('C1_School closing', sa.Column('c1_school_closing', sa.Numeric)),
            ('C1_Flag', sa.Column('c1_flag', sa.Numeric)),
            ('C1_Notes', sa.Column('c1_notes', sa.Text)),
            ('C2_Workplace closing', sa.Column('c2_workplace_closing', sa.Numeric)),
            ('C2_Flag', sa.Column('c2_flag', sa.Numeric)),
            ('C2_Notes', sa.Column('c2_notes', sa.Text)),
            (
                'C3_Cancel public events',
                sa.Column('c3_cancel_public_events', sa.Numeric),
            ),
            ('C3_Flag', sa.Column('c3_flag', sa.Numeric)),
            ('C3_Notes', sa.Column('c3_notes', sa.Text)),
            (
                'C4_Restrictions on gatherings',
                sa.Column('c4_restrictions_on_gatherings', sa.Numeric),
            ),
            ('C4_Flag', sa.Column('c4_flag', sa.Numeric)),
            ('C4_Notes', sa.Column('c4_notes', sa.Text)),
            (
                'C5_Close public transport',
                sa.Column('c5_close_public_transport', sa.Numeric),
            ),
            ('C5_Flag', sa.Column('c5_flag', sa.Numeric)),
            ('C5_Notes', sa.Column('c5_notes', sa.Text)),
            (
                'C6_Stay at home requirements',
                sa.Column('c6_stay_at_home_requirements', sa.Numeric),
            ),
            ('C6_Flag', sa.Column('c6_flag', sa.Numeric)),
            ('C6_Notes', sa.Column('c6_notes', sa.Text)),
            (
                'C7_Restrictions on internal movement',
                sa.Column('c7_restrictions_on_internal_movement', sa.Numeric),
            ),
            ('C7_Flag', sa.Column('c7_flag', sa.Numeric)),
            ('C7_Notes', sa.Column('c7_notes', sa.Text)),
            (
                'C8_International travel controls',
                sa.Column('c8_international_travel_controls', sa.Numeric),
            ),
            ('C8_Notes', sa.Column('c8_notes', sa.Text)),
            ('E1_Income support', sa.Column('e1_income_support', sa.Numeric)),
            ('E1_Flag', sa.Column('e1_flag', sa.Numeric)),
            ('E1_Notes', sa.Column('e1_notes', sa.Text)),
            (
                'E2_Debt/contract relief',
                sa.Column('e2_debt_contract_relief', sa.Numeric),
            ),
            ('E2_Notes', sa.Column('e2_notes', sa.Text)),
            ('E3_Fiscal measures', sa.Column('e3_fiscal_measures', sa.Numeric)),
            ('E3_Notes', sa.Column('e3_notes', sa.Text)),
            (
                'E4_International support',
                sa.Column('e4_international_support', sa.Numeric),
            ),
            ('E4_Notes', sa.Column('e4_notes', sa.Text)),
            (
                'H1_Public information campaigns',
                sa.Column('h1_public_information_campaigns', sa.Numeric),
            ),
            ('H1_Flag', sa.Column('h1_flag', sa.Numeric)),
            ('H1_Notes', sa.Column('h1_notes', sa.Text)),
            ('H2_Testing policy', sa.Column('h2_testing_policy', sa.Numeric)),
            ('H2_Notes', sa.Column('h2_notes', sa.Text)),
            ('H3_Contact tracing', sa.Column('h3_contact_tracing', sa.Numeric)),
            ('H3_Notes', sa.Column('h3_notes', sa.Text)),
            (
                'H4_Emergency investment in healthcare',
                sa.Column('h4_emergency_investment_in_healthcare', sa.Numeric),
            ),
            ('H4_Notes', sa.Column('h4_notes', sa.Text)),
            (
                'H5_Investment in vaccines',
                sa.Column('h5_investment_in_vaccines', sa.Numeric),
            ),
            ('H5_Notes', sa.Column('h5_notes', sa.Text)),
            ('M1_Wildcard', sa.Column('m1_wildcard', sa.Numeric)),
            ('M1_Notes', sa.Column('m1_notes', sa.Text)),
            ('ConfirmedCases', sa.Column('confirmed_cases', sa.Numeric)),
            ('ConfirmedDeaths', sa.Column('confirmed_deaths', sa.Numeric)),
            ('StringencyIndex', sa.Column('stringency_index', sa.Float)),
            (
                'StringencyIndexForDisplay',
                sa.Column('stringency_index_for_display', sa.Float),
            ),
            ('StringencyLegacyIndex', sa.Column('stringency_legacy_index', sa.Float)),
            (
                'StringencyLegacyIndexForDisplay',
                sa.Column('stringency_legacy_index_for_display', sa.Float),
            ),
            (
                'GovernmentResponseIndex',
                sa.Column('government_response_index', sa.Float),
            ),
            (
                'GovernmentResponseIndexForDisplay',
                sa.Column('government_response_index_for_display', sa.Float),
            ),
            ('ContainmentHealthIndex', sa.Column('containment_health_index', sa.Float)),
            (
                'ContainmentHealthIndexForDisplay',
                sa.Column('containment_health_index_for_display', sa.Float),
            ),
            ('EconomicSupportIndex', sa.Column('economic_support_index', sa.Float)),
            (
                'EconomicSupportIndexForDisplay',
                sa.Column('economic_support_index_for_display', sa.Float),
            ),
        ],
    )

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='run-fetch',
            python_callable=partial(fetch_from_hosted_csv, allow_empty_strings=False),
            provide_context=True,
            op_args=[self.table_config.table_name, self.source_url],
        )
