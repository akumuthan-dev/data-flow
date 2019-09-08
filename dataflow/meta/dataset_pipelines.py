from datetime import datetime

from dataflow import constants


class OMISDatasetPipeline:
    table_name = 'omis_dataset'
    source_url = '{}/v4/dataset/omis-dataset'.format(constants.DATAHUB_BASE_URL)
    target_db = 'datasets_db'
    start_date = datetime.now().replace(day=1)
    end_date = None
    field_mapping = [
        (
            'reference',
            'omis_order_reference',
            'character varying(100) PRIMARY KEY'
        ),
        (
            'company__name',
            'company_name',
            'character varying(255) NOT NULL'
        ),
        (
            'status',
            'order_status',
            'character varying(100) NOT NULL'
        ),
        (
            'contact__first_name',
            'contact_first_name',
            'character varying(255) NOT NULL'
        ),
        (
            'contact__last_name',
            'contact_last_name',
            'character varying(255) NOT NULL'
        ),
        (
            'contact__email',
            'contact_email',
            'character varying(255) NOT NULL'
        ),
        (
            'contact__telephone_number',
            'contact_phone_number',
            'character varying(255)'
        ),
        (
            'invoice__subtotal_cost',
            'subtotal',
            'integer'
        ),
        (
            'subtotal_cost',
            'net_price',
            'integer'
        ),
        (
            'sector__segment',
            'sector',
            'character varying(255)'
        ),
        (
            'primary_market__name',
            'market',
            'text'
        ),
        (
            'created_by__dit_team__name',
            'dit_team',
            'text'
        ),
        (
            'uk_region__name',
            'uk_region',
            'text'
        ),
        (
            'created_on',
            'created_date',
            'timestamp with time zone'
        ),
        (
            'cancelled_on',
            'cancelled_date',
            'timestamp with time zone'
        ),
        (
            'cancellation_reason__name',
            'cancellation_reason',
            'text'
        ),
        (
            'completed_on',
            'completion_date',
            'timestamp with time zone'
        ),
        (
            'delivery_date',
            'delivery_date',
            'date'
        ),
        (
            'paid_on',
            'payment_received_date',
            'timestamp with time zone'
        ),
        (
            'company__address_1',
            'company_trading_address_line_1',
            'character varying(255)'
        ),
        (
            'company__address_2',
            'company_trading_address_line_2',
            'character varying(255)'
        ),
        (
            'company__address_town',
            'company_trading_address_town',
            'character varying(255)'
        ),
        (
            'company__address_county',
            'company_trading_address_county',
            'character varying(255)'
        ),
        (
            'company__address_country__name',
            'company_trading_address_country',
            'character varying(255)'
        ),
        (
            'company__address_postcode',
            'company_trading_address_postcode',
            'character varying(255)'
        ),
        (
            'company__registered_address_1',
            'company_registered_address_1',
            'character varying(255)'
        ),
        (
            'company__registered_address_2',
            'company_registered_address_2',
            'character varying(255)'
        ),
        (
            'company__registered_address_town',
            'company_registered_address_town',
            'character varying(255)'
        ),
        (
            'company__registered_address_county',
            'company_registered_address_county',
            'character varying(255)'
        ),
        (
            'company__registered_address_country__name',
            'company_registered_address_country',
            'character varying(255)'
        ),
        (
            'company__registered_address_postcode',
            'company_registered_address_postcode',
            'character varying(255)'
        ),
        (
            'services',
            'services',
            'text'
        ),
    ]
