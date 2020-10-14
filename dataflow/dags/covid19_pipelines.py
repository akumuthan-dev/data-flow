from functools import partial

import pandas as pd
import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator

from dataflow.dags import _PipelineDAG
from dataflow.operators.common import fetch_from_hosted_csv
from dataflow.operators.covid19 import fetch_apple_mobility_data
from dataflow.operators.csv_inputs import fetch_mapped_hosted_csvs
from dataflow.operators.db_tables import query_database
from dataflow.utils import TableConfig


class OxfordCovid19GovernmentResponseTracker(_PipelineDAG):
    source_url = 'https://oxcgrtportal.azurewebsites.net/api/CSVDownload'
    allow_null_columns = True
    use_utc_now_as_source_modified = True
    table_config = TableConfig(
        table_name='oxford_covid19_government_response_tracker',
        field_mapping=[
            ('CountryName', sa.Column('country_name', sa.String)),
            ('CountryCode', sa.Column('country_code', sa.String)),
            ('RegionName', sa.Column('region_name', sa.String)),
            ('RegionCode', sa.Column('region_code', sa.String)),
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
            op_args=[
                self.table_config.table_name,  # pylint: disable=no-member
                self.source_url,
            ],
        )


class CSSECovid19TimeSeriesGlobal(_PipelineDAG):
    # Run after the daily update of data ~4am
    schedule_interval = '0 7 * * *'
    use_utc_now_as_source_modified = True
    _endpoint = "https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series"
    source_urls = {
        "confirmed": f"{_endpoint}/time_series_covid19_confirmed_global.csv",
        "recovered": f"{_endpoint}/time_series_covid19_recovered_global.csv",
        "deaths": f"{_endpoint}/time_series_covid19_deaths_global.csv",
    }

    table_config = TableConfig(
        table_name="csse_covid19_time_series_global",
        field_mapping=[
            ("Province/State", sa.Column("province_or_state", sa.String)),
            ("Country/Region", sa.Column("country_or_region", sa.String)),
            ("Lat", sa.Column("lat", sa.Numeric)),
            ("Long", sa.Column("long", sa.Numeric)),
            ("source_url_key", sa.Column("type", sa.String)),
            ("Date", sa.Column("date", sa.Date)),
            ("Value", sa.Column("value", sa.Numeric)),
        ],
    )

    def transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.melt(
            id_vars=["Province/State", "Country/Region", "Lat", "Long"],
            var_name="Date",
            value_name="Value",
        )

        df["Date"] = pd.to_datetime(df['Date'])

        return df

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id="run-fetch",
            python_callable=fetch_mapped_hosted_csvs,
            provide_context=True,
            op_args=[
                self.table_config.table_name,  # pylint: disable=no-member
                self.source_urls,
                self.transform_dataframe,
            ],
        )


class CSSECovid19TimeSeriesGlobalGroupedByCountry(_PipelineDAG):
    schedule_interval = '0 7 * * *'
    use_utc_now_as_source_modified = True
    dependencies = [CSSECovid19TimeSeriesGlobal]

    query = """
    WITH unmatched_codes (country, iso2, iso3) AS (VALUES
        ('Bahamas', 'BS', 'BSS'),
        ('Bonaire, Sint Eustatius and Saba', 'BQ', 'BES'),
        ('Burma', 'MM', 'MMR'),
        ('Cabo Verde', 'CV', 'CPV'),
        ('Channel Islands', 'GB', 'GBR'),
        ('Congo (Brazzaville)', 'CG', 'COG'),
        ('Congo (Kinshasa)', 'CD', 'COD'),
        ('Cote d''Ivoire', 'CI', 'CIV'),
        ('Curacao', 'CW', 'CUW'),
        ('Falkland Islands (Malvinas)', 'FK', 'FLK'),
        ('Gambia', 'GM', 'GMB'),
        ('Holy See', 'VA', 'VAT'),
        ('Korea, South', 'KR', 'KOR'),
        ('Kosovo', 'XK', 'XKX'),
        ('Reunion', 'RE', 'REU'),
        ('Saint Barthelemy', 'BL', 'BLM'),
        ('Saint Kitts and Nevis', 'KN', 'KNA'),
        ('Saint Lucia', 'LC', 'LCA'),
        ('Saint Vincent and the Grenadines', 'VC', 'VCT'),
        ('Sint Maarten', 'SX', 'SXM'),
        ('St Martin', 'MF', 'MAF'),
        ('Taiwan*', 'TW', 'TWN'),
        ('Timor-Leste', 'TL', 'TLS'),
        ('United Kingdom', 'GB', 'GBR'),
        ('US', 'US', 'USA'),
        ('West Bank and Gaza', 'PS', 'PSE')
    )
    SELECT
        COALESCE(ref_countries_territories_and_regions.iso2_code, unmatched_codes.iso2) AS iso2_code,
        COALESCE(ref_countries_territories_and_regions.iso3_code, unmatched_codes.iso3) AS iso3_code,
        country_or_region,
        CASE
           WHEN csse_data.type = 'confirmed' THEN 'cases'
           ELSE csse_data.type
        END AS type,
        date,
        sum(value) as value,
        sum(daily_value) as daily_value
    FROM
        (SELECT
            province_or_state,
            CASE
                WHEN country_or_region IN ('Denmark', 'United Kingdom', 'France', 'Netherlands') AND province_or_state IS NOT NULL then province_or_state
                ELSE country_or_region
            END AS country_or_region,
            type,
            date,
            value,
            value - lag(value) OVER (PARTITION BY country_or_region, province_or_state, type ORDER BY date) as daily_value
        FROM csse_covid19_time_series_global) AS csse_data
        LEFT JOIN ref_countries_territories_and_regions ON csse_data.country_or_region = ref_countries_territories_and_regions.name
        LEFT JOIN unmatched_codes ON csse_data.country_or_region = unmatched_codes.country
    GROUP BY (date, country_or_region, ref_countries_territories_and_regions.iso2_code, ref_countries_territories_and_regions.iso3_code, unmatched_codes.iso2, unmatched_codes.iso3, csse_data.type)
    ORDER BY date desc, country_or_region asc, type;
    """

    table_config = TableConfig(
        table_name="csse_covid19_time_series_global_by_country",
        field_mapping=[
            ("iso2_code", sa.Column("iso2_code", sa.String)),
            ("iso3_code", sa.Column("iso3_code", sa.String)),
            ("country_or_region", sa.Column("country_or_region", sa.String)),
            ("type", sa.Column("type", sa.String)),
            ("date", sa.Column("date", sa.Date)),
            ("value", sa.Column("value", sa.Numeric)),
            ("daily_value", sa.Column("daily_value", sa.Numeric)),
        ],
    )

    def get_fetch_operator(self):
        op = PythonOperator(
            task_id='query-database',
            provide_context=True,
            python_callable=query_database,
            op_args=[
                self.query,
                self.target_db,
                self.table_config.table_name,  # pylint: disable=no-member
            ],
        )
        return op


class GoogleCovid19MobilityReports(_PipelineDAG):
    use_utc_now_as_source_modified = True
    source_urls = {
        'global': 'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv',
    }

    table_config = TableConfig(
        table_name="covid19_global_mobility_report",
        schema='google',
        field_mapping=[
            ("country_region_code", sa.Column("country_region_code", sa.String)),
            ("country_region", sa.Column("country_region", sa.String)),
            ("sub_region_1", sa.Column("sub_region_1", sa.String)),
            ("sub_region_2", sa.Column("sub_region_2", sa.String)),
            ("adm_area_1", sa.Column("adm_area_1", sa.String)),
            ("adm_area_1", sa.Column("adm_area_1", sa.String)),
            ("metro_area", sa.Column("metro_area", sa.String)),
            ("iso_3166_2_code", sa.Column("iso_3166_2_code", sa.String)),
            ("census_fips_code", sa.Column("census_fips_code", sa.String)),
            ("date", sa.Column("date", sa.Date)),
            (
                "retail_and_recreation_percent_change_from_baseline",
                sa.Column("retail_recreation", sa.Numeric),
            ),
            (
                "grocery_and_pharmacy_percent_change_from_baseline",
                sa.Column("grocery_pharmacy", sa.Numeric),
            ),
            ("parks_percent_change_from_baseline", sa.Column("parks", sa.Numeric),),
            (
                "transit_stations_percent_change_from_baseline",
                sa.Column("transit_stations", sa.Numeric),
            ),
            (
                "workplaces_percent_change_from_baseline",
                sa.Column("workplace", sa.Numeric),
            ),
            (
                "residential_percent_change_from_baseline",
                sa.Column("residential", sa.Numeric),
            ),
        ],
    )

    def transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df['adm_area_1'] = None
        df['adm_area_2'] = None

        # Update administrative areas from sub regions
        df.loc[df.sub_region_1.notnull(), 'adm_area_1'] = df.loc[
            df.sub_region_1.notnull(), 'sub_region_1'
        ].str.strip()
        df.loc[df.sub_region_2.notnull(), 'adm_area_2'] = df.loc[
            df.sub_region_2.notnull(), 'sub_region_2'
        ].str.strip()

        # Clean US administrative areas
        df.loc[df.country_region_code == 'US', 'adm_area_2'] = (
            df.loc[df.country_region_code == 'US', 'adm_area_2']
            .replace(r'\s*County\s*|\s*Parish\s*', '', regex=True)
            .replace(r'St\.', 'Saint', regex=True)
            .str.strip()
        )

        # Do not set admin area 1 for GB
        df.loc[df.country_region_code == 'GB', 'adm_area_2'] = df.loc[
            df.country_region_code == 'GB', 'adm_area_1'
        ]
        df.loc[df.country_region_code == 'GB', 'adm_area_1'] = None

        # Clean Jamaican administrative areas
        df.loc[df.country_region_code == 'JM', 'adm_area_1'] = (
            df.loc[df.country_region_code == 'JM', 'adm_area_1']
            .replace('Parish', '')
            .replace('St.', 'Saint')
            .str.strip()
        )

        # Clean admin area 1 for remaining countries
        df.loc[~df.country_region_code.isin(['US', 'GB', 'JM']), 'adm_area_1'] = (
            df.loc[~df.country_region_code.isin(['US', 'GB', 'JM']), 'adm_area_1']
            .replace(
                r'\s*Province\s*|\s*District\s*|\s*County\s*|\s*Region\s*|\s*Governorate\s*|\s*State of\s*|\s*Department\s*',
                '',
                regex=True,
            )
            .str.strip()
        )

        return df

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='run-fetch',
            python_callable=fetch_mapped_hosted_csvs,
            provide_context=True,
            queue='high-memory-usage',
            op_args=[
                self.table_config.table_name,  # pylint: disable=no-member
                self.source_urls,
                self.transform_dataframe,
            ],
        )


class AppleCovid19MobilityTrendsPipeline(_PipelineDAG):
    base_url = 'https://covid19-static.cdn-apple.com'
    config_path = '/covid19-mobility-data/current/v3/index.json'
    use_utc_now_as_source_modified = True
    table_config = TableConfig(
        schema='apple',
        table_name='covid19_mobility_data',
        field_mapping=[
            ('geo_type', sa.Column('geo_type', sa.String)),
            ('country', sa.Column('country', sa.String)),
            ('region', sa.Column('region', sa.String)),
            ('sub-region', sa.Column('sub_region', sa.String)),
            ('adm_area_1', sa.Column('adm_area_1', sa.String)),
            ('adm_area_2', sa.Column('adm_area_2', sa.String)),
            ('date', sa.Column('date', sa.Date)),
            ('driving', sa.Column('driving', sa.Numeric)),
            ('transit', sa.Column('transit', sa.Numeric)),
            ('walking', sa.Column('walking', sa.Numeric)),
        ],
    )

    def transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df.drop(columns=['alternative_name'], inplace=True)

        # Unpivot dates columns into a single date column
        df = df.melt(
            id_vars=[
                'geo_type',
                'region',
                'sub-region',
                'transportation_type',
                'country',
            ],
            var_name='date',
        )
        df['value'] = df['value'] - 100

        # Separate values out by transportation transportation type (driving, transit, walking)
        df = (
            df.set_index(
                [
                    'geo_type',
                    'region',
                    'sub-region',
                    'country',
                    'date',
                    'transportation_type',
                ]
            )
            .unstack('transportation_type')
            .reset_index()
        )

        # Clean up transportation type field names
        df.columns = [t + (v if v != 'value' else '') for v, t in df.columns]

        # Set country to region name if geo type is "country/region"
        df.loc[df.geo_type == 'country/region', 'country'] = df.loc[
            df.geo_type == 'country/region', 'region'
        ]

        # Create administrative areas
        df['adm_area_1'] = None
        df['adm_area_2'] = None

        # Clean administrative area for records with a geo type of "sub-region"
        df.loc[df.geo_type == 'sub-region', 'adm_area_1'] = df.loc[
            df.geo_type == 'sub-region', 'region'
        ]
        df.loc[df.geo_type == 'sub-region', 'adm_area_1'] = (
            df.loc[df.geo_type == 'sub-region', 'adm_area_1']
            .replace(
                r'\s*County\s*|\s*Region\s*|\s*Province\s*|\s*Prefecture\s*',
                '',
                regex=True,
            )
            .str.strip()
        )

        # Clean administrative area for records with a geo type of "county"
        df.loc[df.geo_type == 'county', 'adm_area_1'] = df.loc[
            df.geo_type == 'county', 'sub-region'
        ]
        df.loc[df.geo_type == 'county', 'adm_area_2'] = df.loc[
            df.geo_type == 'county', 'region'
        ]
        df.loc[df.geo_type == 'county', 'adm_area_2'] = (
            df.loc[df.geo_type == 'county', 'adm_area_2']
            .replace(r'\s*County\s*|\s*Parish\s*', '', regex=True)
            .replace(r'St\.', 'Saint', regex=True)
            .str.strip()
        )

        # Fix country name
        df['country'] = df['country'].replace('Republic of Korea', 'South Korea')

        df = df.sort_values(
            by=['country', 'region', 'sub-region', 'adm_area_1', 'adm_area_2', 'date']
        ).reset_index(drop=True)

        return df

    def get_fetch_operator(self):
        return PythonOperator(
            task_id='run-fetch-apple-mobility-data',
            provide_context=True,
            python_callable=fetch_apple_mobility_data,
            op_args=[
                self.table_config.table_name,  # pylint: disable=no-member
                self.base_url,
                self.config_path,
                self.transform_dataframe,
            ],
        )
