from datetime import datetime

from dataflow.meta.dataset_pipelines import (
    OMISDatasetPipeline,
    InvestmentProjectsDatasetPipeline
)


class CompletedOMISOrderViewPipeline:
    view_name = 'completed_omis_orders'
    dataset_pipeline = OMISDatasetPipeline
    start_date = datetime(2019, 8, 1)
    end_date = None
    catchup = True
    fields = [
        ('company_name', 'Company Name'),
        ('dit_team', 'DIT Team'),
        ('subtotal', 'Subtotal'),
        ('uk_region', 'UK Region'),
        ('market', 'Market'),
        ('sector', 'Sector'),
        ('services', 'Services'),
        ('delivery_date', 'Delivery Date'),
        ('payment_received_date', 'Payment Received Date'),
        ('completion_date', 'Completion Date'),
    ]
    where_clause = """
        order_status = 'complete' AND
        date_trunc('month', completion_date)::DATE =
            date_trunc('month', to_date('{{ yesterday_ds }}', 'YYYY-MM-DD'));
    """
    schedule_interval = '@monthly'


class CancelledOMISOrderViewPipeline:
    view_name = 'cancelled_omis_orders'
    dataset_pipeline = OMISDatasetPipeline
    start_date = datetime(2019, 8, 1)
    end_date = None
    catchup = True
    fields = [
        ('omis_order_reference', 'OMIS Order Reference'),
        ('company_name', 'Company Name'),
        ('net_price', 'Net Price'),
        ('dit_team', 'DIT Team'),
        ('market', 'Market'),
        ('sector', 'Sector'),
        ('created_date', 'Created Date'),
        ('cancelled_date', 'Cancelled Date'),
        ('cancellation_reason', 'Cancellation Reason'),
    ]
    where_clause = """
        order_status = 'cancelled' AND
        cancelled_date::DATE >=
        {% if macros.datetime.strptime(ds, '%Y-%m-%d') <= macros.datetime.strptime('{0}-{1}'.format(macros.ds_format(ds, '%Y-%m-%d', '%Y'), month_day_financial_year), '%Y-%m-%d') %}
            date_trunc('month', to_date('{{ macros.ds_format(macros.ds_add(ds, -365), '%Y-%m-%d', '%Y') }}-{{ month_day_financial_year }}', 'YYYY-MM-DD'));
        {% else %}
            date_trunc('month', to_date('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}-{{ month_day_financial_year }}', 'YYYY-MM-DD'));
        {% endif %}
    """
    params = {
        'month_day_financial_year': '04-01'
    }
    schedule_interval = '@monthly'


class OMISClientSurveyViewPipeline:
    view_name = 'omis_client_survey'
    dataset_pipeline = OMISDatasetPipeline
    start_date = datetime(2019, 8, 1)
    end_date = None
    catchup = True
    fields = [
        ('company_name', 'Company Name'),
        ('contact_first_name', 'Contact First Name'),
        ('contact_last_name', 'Contact Last Name'),
        ('contact_email', 'Contact Email'),
        ('company_trading_address_line_1', 'Company Trading Address Line 1'),
        ('company_trading_address_line_2', 'Company Trading Address Line 2'),
        ('company_trading_address_town', 'Company Trading Address Town'),
        ('company_trading_address_county', 'Company Trading Address County'),
        ('company_trading_address_postcode', 'Company Trading Address Postcode'),
        ('company_registered_address_1', 'Company Registered Address 1'),
        ('company_registered_address_2', 'Company Registered Address 2'),
        ('company_registered_address_town', 'Company Registered Address Town'),
        ('company_registered_address_county', 'Company Registered Address County'),
        ('company_registered_address_country', 'Company Registered Address Country'),
        ('company_registered_address_postcode', 'Company Registered Address Postcode'),
    ]
    where_clause = """
        order_status = 'complete' AND
        date_trunc('month', completion_date)::DATE =
            date_trunc('month', to_date('{{ yesterday_ds }}', 'YYYY-MM-DD'));
    """
    schedule_interval = '@monthly'


class InvestmentProjectsViewPipeline:
    view_name = 'investment_projects'
    dataset_pipeline = InvestmentProjectsDatasetPipeline
    start_date = datetime(2019, 8, 1)
    end_date = None
    catchup = True
    fields = '__all__'
    where_clause = """
        date_trunc('month', created_on)::DATE =
            date_trunc('month', to_date('{{ yesterday_ds }}', 'YYYY-MM-DD'));
    """
    schedule_interval = '@monthly'
