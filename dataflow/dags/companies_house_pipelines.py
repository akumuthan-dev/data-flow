import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator

from dataflow.dags import _PipelineDAG
from dataflow.operators.companies_house import (
    fetch_companies_house_companies,
    fetch_companies_house_significant_persons,
)
from dataflow.utils import TableConfig


class CompaniesHouseCompaniesPipeline(_PipelineDAG):
    schedule_interval = '0 0 10 * *'
    allow_null_columns = True
    use_utc_now_as_source_modified = True
    number_of_files = 6
    source_url = 'http://download.companieshouse.gov.uk/BasicCompanyData-{file_date}-part{file_num}_{num_files}.zip'
    table_config = TableConfig(
        schema='companieshouse',
        table_name='companies',
        field_mapping=[
            ('CompanyNumber', sa.Column('id', sa.String(8), primary_key=True)),
            ('CompanyName', sa.Column('company_name', sa.String)),
            ('CompanyNumber', sa.Column('company_number', sa.String)),
            ('RegAddress.CareOf', sa.Column('care_of', sa.String)),
            ('RegAddress.POBox', sa.Column('po_box', sa.String)),
            ('RegAddress.AddressLine1', sa.Column('address_line_1', sa.Text)),
            ('RegAddress.AddressLine2', sa.Column('address_line_2', sa.Text)),
            ('RegAddress.PostTown', sa.Column('post_town', sa.String)),
            ('RegAddress.County', sa.Column('county', sa.String)),
            ('RegAddress.Country', sa.Column('country', sa.String)),
            ('RegAddress.PostCode', sa.Column('postcode', sa.String)),
            ('CompanyCategory', sa.Column('company_category', sa.String)),
            ('CompanyStatus', sa.Column('company_status', sa.String)),
            ('CountryOfOrigin', sa.Column('country_of_origin', sa.String)),
            ('DissolutionDate', sa.Column('dissolution_date', sa.String)),
            ('IncorporationDate', sa.Column('incorporation_date', sa.String)),
            ('Accounts.AccountRefDay', sa.Column('account_ref_day', sa.String)),
            ('Accounts.AccountRefMonth', sa.Column('account_ref_month', sa.String)),
            ('Accounts.NextDueDate', sa.Column('account_next_due_date', sa.String)),
            (
                'Accounts.LastMadeUpDate',
                sa.Column('account_last_made_up_date', sa.String),
            ),
            ('Accounts.AccountCategory', sa.Column('account_category', sa.String)),
            ('Returns.NextDueDate', sa.Column('return_next_due_date', sa.String)),
            (
                'Returns.LastMadeUpDate',
                sa.Column('return_last_made_up_date', sa.String),
            ),
            ('Mortgages.NumMortCharges', sa.Column('num_mort_charges', sa.String)),
            (
                'Mortgages.NumMortOutstanding',
                sa.Column('num_mort_outstanding', sa.String),
            ),
            (
                'Mortgages.NumMortPartSatisfied',
                sa.Column('num_mort_parts_satisified', sa.String),
            ),
            ('Mortgages.NumMortSatisfied', sa.Column('num_mort_satisfied', sa.String)),
            ('SICCode.SicText_1', sa.Column('sic_code_1', sa.String)),
            ('SICCode.SicText_2', sa.Column('sic_code_2', sa.String)),
            ('SICCode.SicText_3', sa.Column('sic_code_3', sa.String)),
            ('SICCode.SicText_4', sa.Column('sic_code_4', sa.String)),
            (
                'LimitedPartnerships.NumGenPartners',
                sa.Column('num_gen_partners', sa.String),
            ),
            (
                'LimitedPartnerships.NumLimPartners',
                sa.Column('num_lim_partners', sa.String),
            ),
            ('URI', sa.Column('uri', sa.String)),
            (
                'PreviousName_1.CONDATE',
                sa.Column('previous_name_1_change_date', sa.String),
            ),
            ('PreviousName_1.CompanyName', sa.Column('previous_name_1', sa.String)),
            (
                'PreviousName_2.CONDATE',
                sa.Column('previous_name_2_change_date', sa.String),
            ),
            ('PreviousName_2.CompanyName', sa.Column('previous_name_2', sa.String)),
            (
                'PreviousName_3.CONDATE',
                sa.Column('previous_name_3_change_date', sa.String),
            ),
            ('PreviousName_3.CompanyName', sa.Column('previous_name_3', sa.String)),
            (
                'PreviousName_4.CONDATE',
                sa.Column('previous_name_4_change_date', sa.String),
            ),
            ('PreviousName_4.CompanyName', sa.Column('previous_name_4', sa.String)),
            (
                'PreviousName_5.CONDATE',
                sa.Column('previous_name_5_change_date', sa.String),
            ),
            ('PreviousName_5.CompanyName', sa.Column('previous_name_5', sa.String)),
            (
                'PreviousName_6.CONDATE',
                sa.Column('previous_name_6_change_date', sa.String),
            ),
            ('PreviousName_6.CompanyName', sa.Column('previous_name_6', sa.String)),
            (
                'PreviousName_7.CONDATE',
                sa.Column('previous_name_7_change_date', sa.String),
            ),
            ('PreviousName_7.CompanyName', sa.Column('previous_name_7', sa.String)),
            (
                'PreviousName_8.CONDATE',
                sa.Column('previous_name_8_change_date', sa.String),
            ),
            ('PreviousName_8.CompanyName', sa.Column('previous_name_8', sa.String)),
            (
                'PreviousName_9.CONDATE',
                sa.Column('previous_name_9_change_date', sa.String),
            ),
            ('PreviousName_9.CompanyName', sa.Column('previous_name_9', sa.String)),
            (
                'PreviousName_10.CONDATE',
                sa.Column('previous_name_10_change_date', sa.String),
            ),
            ('PreviousName_10.CompanyName', sa.Column('previous_name_10', sa.String)),
            (
                'ConfStmtNextDueDate',
                sa.Column('conf_statement_next_due_date', sa.String),
            ),
            (
                'ConfStmtLastMadeUpDate',
                sa.Column('conf_statement_last_made_up_date', sa.String),
            ),
            ('publish_date', sa.Column('publish_date', sa.Date)),
        ],
    )

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='run-fetch',
            python_callable=fetch_companies_house_companies,
            provide_context=True,
            op_args=[
                self.table_config.table_name,  # pylint: disable=no-member
                self.source_url,
                self.number_of_files,
            ],
            retries=self.fetch_retries,
        )


class CompaniesHousePeopleWithSignificantControlPipeline(_PipelineDAG):
    use_utc_now_as_source_modified = True
    # Files names include a file number and a total number of files. Changes to the total number
    # of files can be set in the COMPANIES_HOUSE_PSC_TOTAL_FILES environment variable
    source_url = (
        'http://download.companieshouse.gov.uk/psc-snapshot-{{execution_date.strftime("%Y-%m-%d")}}'
        '_{file}of{total}.zip'
    )
    table_config = TableConfig(
        schema='companieshouse',
        table_name='people_with_significant_control',
        field_mapping=[
            ('company_number', sa.Column('company_number', sa.String)),
            (
                ('data', 'address', 'address_line_1'),
                sa.Column('address_line_1', sa.String),
            ),
            (('data', 'address', 'country'), sa.Column('address_country', sa.String)),
            (('data', 'address', 'locality'), sa.Column('address_locality', sa.String)),
            (
                ('data', 'address', 'postal_code'),
                sa.Column('address_postcode', sa.String),
            ),
            (('data', 'address', 'premises'), sa.Column('address_premises', sa.String)),
            (('data', 'ceased_on'), sa.Column('ceased_on', sa.Date)),
            (
                ('data', 'country_of_residence'),
                sa.Column('country_of_residence', sa.String),
            ),
            (
                ('data', 'date_of_birth', 'month'),
                sa.Column('date_of_birth_month', sa.Integer),
            ),
            (
                ('data', 'date_of_birth', 'year'),
                sa.Column('date_of_birth_year', sa.Integer),
            ),
            (('data', 'kind'), sa.Column('kind', sa.String)),
            (('data', 'name'), sa.Column('name', sa.String)),
            (('data', 'nationality'), sa.Column('nationality', sa.String)),
            (
                ('data', 'natures_of_control'),
                sa.Column('natures_of_control', sa.ARRAY(sa.String)),
            ),
            (('data', 'notified_on'), sa.Column('notified_on', sa.Date)),
        ],
    )

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='run-fetch',
            python_callable=fetch_companies_house_significant_persons,
            provide_context=True,
            queue='high-memory-usage',
            op_args=[
                self.table_config.table_name,  # pylint: disable=no-member
                self.source_url,
            ],
            retries=self.fetch_retries,
        )
