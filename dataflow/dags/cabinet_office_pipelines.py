import pandas as pd
import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator

from dataflow.dags import _PipelineDAG
from dataflow.operators.cabinet_office import fetch_gender_pay_gap_files
from dataflow.utils import TableConfig


class CabinetOfficeGenderPayGapPipeline(_PipelineDAG):
    schedule_interval = '0 0 10 * *'
    allow_null_columns = True
    use_utc_now_as_source_modified = True
    records_start_year = 2017
    base_url = 'https://gender-pay-gap.service.gov.uk/viewing/download-data/{year}'
    table_config = TableConfig(
        schema='cabinet_office',
        table_name='gender_pay_gap',
        field_mapping=[
            ('EmployerName', sa.Column('employer_name', sa.String)),
            ('Address', sa.Column('address', sa.Text)),
            ('CompanyNumber', sa.Column('company_number', sa.String)),
            ('SicCodes', sa.Column('sic_codes', sa.String)),
            (
                'DiffMeanHourlyPercent',
                sa.Column('diff_mean_hourly_percent', sa.Numeric),
            ),
            (
                'DiffMedianHourlyPercent',
                sa.Column('diff_median_hourly_precent', sa.Numeric),
            ),
            ('DiffMeanBonusPercent', sa.Column('diff_mean_bonus_percent', sa.Numeric)),
            (
                'DiffMedianBonusPercent',
                sa.Column('diff_median_bonus_percent', sa.Numeric),
            ),
            ('MaleBonusPercent', sa.Column('male_bonus_percent', sa.Numeric)),
            ('FemaleBonusPercent', sa.Column('female_bonus_percent', sa.Numeric)),
            ('MaleLowerQuartile', sa.Column('male_lower_quartile', sa.Numeric)),
            ('FemaleLowerQuartile', sa.Column('female_lower_quartile', sa.Numeric)),
            (
                'MaleLowerMiddleQuartile',
                sa.Column('male_lower_middle_quartile', sa.Numeric),
            ),
            (
                'FemaleLowerMiddleQuartile',
                sa.Column('female_lwoer_middle_quartile', sa.Numeric),
            ),
            (
                'MaleUpperMiddleQuartile',
                sa.Column('male_upper_middle_quartile', sa.Numeric),
            ),
            (
                'FemaleUpperMiddleQuartile',
                sa.Column('female_upper_middle_quartile', sa.Numeric),
            ),
            ('MaleTopQuartile', sa.Column('male_top_quartile', sa.Numeric)),
            ('FemaleTopQuartile', sa.Column('female_top_quartile', sa.Numeric)),
            ('CompanyLinkToGPGInfo', sa.Column('company_link_to_gpg_info', sa.String)),
            ('ResponsiblePerson', sa.Column('responsible_person', sa.String)),
            ('EmployerSize', sa.Column('employer_size', sa.String)),
            ('CurrentName', sa.Column('current_name', sa.String)),
            (
                'SubmittedAfterTheDeadline',
                sa.Column('submitted_after_the_deadline', sa.Boolean),
            ),
            ('DueDate', sa.Column('due_date', sa.Date)),
            ('DateSubmitted', sa.Column('date_submitted', sa.DateTime)),
        ],
    )

    def transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df['DueDate'] = pd.to_datetime(df['DueDate'])
        df['DateSubmitted'] = pd.to_datetime(df['DateSubmitted'])
        return df

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='run-fetch',
            python_callable=fetch_gender_pay_gap_files,
            provide_context=True,
            op_args=[
                self.table_config.table_name,  # pylint: disable=no-member
                self.base_url,
                self.records_start_year,
                self.transform_dataframe,
            ],
        )
