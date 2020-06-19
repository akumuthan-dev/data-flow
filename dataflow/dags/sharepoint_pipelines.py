"""A module that defines Airflow DAGS for sharepoint pipelines."""
import sqlalchemy as sa

from airflow.operators.python_operator import PythonOperator

from dataflow.dags import _PipelineDAG
from dataflow.operators.sharepoint import fetch_from_sharepoint_list
from dataflow.utils import TableConfig


class _SharepointPipeline(_PipelineDAG):
    site_name: str
    list_name: str

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='run-fetch',
            python_callable=fetch_from_sharepoint_list,
            provide_context=True,
            op_args=[self.table_config.table_name, self.site_name, self.list_name],
        )


class InformationAssetRegisterPipeline(_SharepointPipeline):
    site_name = 'DataWorkspaceIntegrationTesting'
    list_name = 'Information Asset Register'
    allow_null_columns = True
    table_config = TableConfig(
        table_name='information_asset_register',
        field_mapping=[
            ('ID', sa.Column('id', sa.String)),
            ('Area', sa.Column('area', sa.String)),
            ('Directorate', sa.Column('directorate', sa.String)),
            ('DIT Business Area', sa.Column('dit_business_area', sa.String)),
            (
                'Information Asset Owner',
                sa.Column('information_asset_owner', sa.String),
            ),
            ('IAO Role', sa.Column('iao_role', sa.String)),
            (
                'Information Asset Manager',
                sa.Column('information_asset_manager', sa.String),
            ),
            ('Name of Asset', sa.Column('name_of_asset', sa.String)),
            ('What does it do?', sa.Column('what_does_it_do', sa.String)),
            ('Asset Type', sa.Column('asset_type', sa.String)),
            ('Asset Format', sa.Column('asset_format', sa.String)),
            ('Who has access?', sa.Column('who_has_access', sa.String)),
            ('Who is it shared with?', sa.Column('who_is_it_shared_with', sa.String)),
            ('Location', sa.Column('location', sa.String)),
            ('Date the asset was created', sa.Column('date_asset_created', sa.Date),),
            ('Retention Period', sa.Column('retention_period', sa.String)),
            (
                'Is a sharing agreement in place?',
                sa.Column('is_sharing_agreement_in_place', sa.Boolean),
            ),
            ('Sharing agreement link', sa.Column('sharing_agreement_link', sa.String)),
            (
                'Security handling classification',
                sa.Column('security_handling_classification', sa.String),
            ),
            ('Security controls', sa.Column('security_controls', sa.String)),
            ('Business Value', sa.Column('business_value', sa.String)),
            ('Risks to the Asset', sa.Column('risks_to_the_asset', sa.String)),
            ('Risk status', sa.Column('risk_status', sa.String)),
            ('Is personal data held?', sa.Column('is_personal_data_held', sa.Boolean)),
            ('Type of Personal Data', sa.Column('type_of_personal_data', sa.String)),
            (
                'Lawful basis for processing personal data',
                sa.Column('lawful_basis_for_process_personal_data', sa.String),
            ),
            (
                'Type of agreement for collecting personal data',
                sa.Column('type_of_agreement_personal_data_agreement', sa.String),
            ),
            (
                'Link to agreement for collecting personal data',
                sa.Column('link_to_personal_data_agreement', sa.String),
            ),
            (
                'Policy for business process this asset has been created under',
                sa.Column('business_process_asset_created_under', sa.String),
            ),
            ('GDPR Compliant?', sa.Column('is_gdpr_compliant', sa.Boolean)),
            ('IAM Approved', sa.Column('is_iam_approved', sa.Date)),
            ('KIM Validated', sa.Column('is_kim_validated', sa.Date)),
            ('IAO Approved', sa.Column('is_iao_approved', sa.Date)),
            ('Public IAR entry', sa.Column('public_iar_entry', sa.String)),
            (
                'Public IAR entry:Information Asset',
                sa.Column('public_iar_entry_information_asset', sa.String),
            ),
            ('Next Review Deadline', sa.Column('next_review_deadline', sa.Date)),
            ('Modified', sa.Column('modified', sa.DateTime)),
            (('lastModifiedBy', 'email'), sa.Column('modified_by', sa.String)),
            ('IAR Change Approval', sa.Column('iar_change_approval', sa.String)),
            ('Created', sa.Column('created', sa.DateTime)),
            (('createdBy', 'email'), sa.Column('created_by', sa.String)),
            ('IAO', sa.Column('iao', sa.String)),
            ('IAM', sa.Column('iam', sa.String)),
            ('Item Type', sa.Column('item_type', sa.String)),
            ('Path', sa.Column('path', sa.String)),
        ],
    )


class PublicInformationAssetRegisterPipeline(_SharepointPipeline):
    site_name = 'DataWorkspaceIntegrationTesting'
    list_name = 'Public Information Asset Register'
    table_config = TableConfig(
        table_name='public_information_asset_register',
        field_mapping=[
            ('#', sa.Column('id', sa.String)),
            ('Information Asset', sa.Column('information_asset', sa.String)),
            ('Description', sa.Column('description', sa.String)),
            ('Item Type', sa.Column('item_type', sa.String)),
            ('Path', sa.Column('path', sa.String)),
        ],
    )