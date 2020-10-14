import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator

from dataflow.dags import _PipelineDAG
from dataflow.operators.companies_house import fetch_companies_house_companies
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
            (None, sa.Column("id", sa.Integer, primary_key=True, autoincrement=True)),
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
                self.table_config.table_name,
                self.source_url,
                self.number_of_files,
            ],
        )
