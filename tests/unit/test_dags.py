import pytest
from airflow.models.dagbag import DagBag

from tests.unit.utils import (
    get_fetch_retries_for_all_concrete_dags,
    get_dags_with_non_pk_indexes_on_sqlalchemy_columns,
    get_table_definitions_for_all_concrete_dags,
    get_dags_with_tables_in_public_schema,
)


def test_pipelines_dags():
    dagbag = DagBag('dataflow', include_examples=False)
    assert set(dagbag.dag_ids) == {
        'AdvisersDatasetPipeline',
        'AdvisersLastInteractionPipeline',
        'AppleCovid19MobilityTrendsPipeline',
        'CabinetOfficeGenderPayGapPipeline',
        'CSSECovid19TimeSeriesGlobal',
        'CSSECovid19TimeSeriesGlobalGroupedByCountry',
        'ComtradeGoodsPipeline',
        'ComtradeServicesPipeline',
        'CompaniesDatasetPipeline',
        'CompaniesHouseCompaniesPipeline',
        'CompaniesHouseMatchingPipeline',
        'CompaniesHousePeopleWithSignificantControlPipeline',
        'CompanyExportCountry',
        'CompanyExportCountryHistory',
        'ConsentPipeline',
        'ContactsDatasetPipeline',
        'ContactsLastInteractionPipeline',
        'CoronavirusInteractionsDashboardPipeline',
        'CountriesOfInterestServicePipeline',
        'DNBCompanyPipeline',
        'DNBGlobalCompanyUpdatePipeline',
        'DSSGenericPipeline',
        'DSSHMRCFieldForceMatchingPipeline',
        'DSSHMRCExportersMatchingPipeline',
        'DailyCSVRefreshPipeline',
        'DataHubCompanyReferralsDatasetPipeline',
        'DataHubExportClientSurveyStaticCSVPipeline',
        'DataHubFDIDailyCSVPipeline',
        'DataHubFDIMonthlyStaticCSVPipeline',
        'DataHubInteractionsCurrentYearDailyCSVPipeline',
        'DataHubInteractionsPreviousYearDailyCSVPipeline',
        'DataHubMatchingPipeline',
        'DataHubMonthlyInvesmentProjectsPipline',
        'DataHubOMISAllOrdersCSVPipeline',
        'DataHubOMISCancelledOrdersCSVPipeline',
        'DataHubOMISClientSurveyStaticCSVPipeline',
        'DataHubOMISCompletedOrdersCSVPipeline',
        'DataHubSPIPipeline',
        'DataHubServiceDeliveriesCurrentYearDailyCSVPipeline',
        'DataHubServiceDeliveriesPreviousYearDailyCSVPipeline',
        'DataHubServiceDeliveryInteractionsCSVPipeline',
        'DataWorkspaceApplicationInstancePipeline',
        'DataWorkspaceCatalogueItemsPipeline',
        'DataWorkspaceEventLogPipeline',
        'DataWorkspaceUserPipeline',
        'DNBCompanyMatchingPipeline',
        'EnquiryMgmtEnquiriesPipeline',
        'ERPPipeline',
        'EventsDatasetPipeline',
        'ExampleTensorflowPipeline',
        'ExportWinsAdvisersDatasetPipeline',
        'ExportWinsBreakdownsDatasetPipeline',
        'ExportWinsByFinancialYearCSVPipeline',
        'ExportWinsCurrentFinancialYearDailyCSVPipeline',
        'ExportWinsDashboardPipeline',
        'ExportWinsDerivedReportTablePipeline',
        'ExportWinsHVCDatasetPipeline',
        'ExportWinsMatchingPipeline',
        'ExportWinsWinsDatasetPipeline',
        'ExportWinsYearlyCSVPipeline',
        'FDIDashboardPipeline',
        'GatewayToResearchFundsPipeline',
        'GatewayToResearchOrganisationsPipeline',
        'GatewayToResearchPersonsPipeline',
        'GatewayToResearchProjectsPipeline',
        'GlobalUKTariffPipeline',
        'GoogleCovid19MobilityReports',
        'GreatGovUKCompanyPipeline',
        'GreatGOVUKExportOpportunitiesPipeline',
        'GreatGOVUKExportOpportunityEnquiriesMatchingPipeline',
        'GreatGOVUKExportOpportunityEnquiriesPipeline',
        'GreatGovUKFormsPipeline',
        'HMRCControl',
        'HMRCEUExports',
        'HMRCEUExportsEstimates',
        'HMRCEUImports',
        'HMRCEUImportsEstimates',
        'HMRCNonEUExports',
        'HMRCNonEUImports',
        'InformationAssetRegisterPipeline',
        'InteractionsDatasetPipeline',
        'InteractionsExportCountryDatasetPipeline',
        'InvestmentProjectsDatasetPipeline',
        'LITECaseChangesPipeline',
        'LITECasesPipeline',
        'Maintenance',
        'MarketAccessTradeBarriersPipeline',
        'MaxemailCampaignsPipeline',
        'MaxemailEventsPipeline',
        'MinisterialInteractionsDashboardPipeline',
        'OMISDatasetPipeline',
        'ONSPostcodePipeline',
        'ONSUKSATradeInGoodsCSV',
        'ONSUKSATradeInGoodsPollingPipeline',
        'ONSUKTotalTradeAllCountriesNSACSVPipeline',
        'ONSUKTotalTradeAllCountriesNSAPollingPipeline',
        'ONSUKTradeInGoodsByCountryAndCommodityCSVPipeline',
        'ONSUKTradeInGoodsByCountryAndCommodityPollingPipeline',
        'ONSUKTradeInServicesByPartnerCountryNSACSV',
        'ONSUKTradeInServicesByPartnerCountryNSAPollingPipeline',
        'OxfordCovid19GovernmentResponseTracker',
        'PeopleFinderPeoplePipeline',
        'RawWorldBankBoundRatePipeline',
        'RawWorldBankTariffPipeline',
        'ReturnToOfficeBookingsPipeline',
        'StaffSSOUsersPipeline',
        'TagsClassifierPredictionPipeline',
        'TagsClassifierTrainPipeline',
        'TeamsDatasetPipeline',
        'UKCovid19LocalAuthorityPrevalencePipeline',
        'UKCovid19RegionalPrevalencePipeline',
        'UKCovid19NationalPrevalencePipeline',
        'ZendeskDITTicketsPipeline',
        'ZendeskDITGroupsPipeline',
        'ZendeskUKTradeTicketsPipeline',
        'ZendeskUKTRadeGroupsPipeline',
    }


def test_standard_dags_have_some_fetch_retries():
    fetch_retries_by_dag_name = get_fetch_retries_for_all_concrete_dags()

    for dag_class_name, fetch_retries in fetch_retries_by_dag_name.items():
        assert (
            fetch_retries > 0
        ), f"{dag_class_name} does not have any retries configured for its fetch operator"


@pytest.mark.xfail
def test_standard_dags_do_not_use_indexes_directly_on_sqlalchemy_column_definitions():
    """
    We should be moving away from using sa.Column(index=True) to using TableConfig.indexes=[LateIndex(), ...] as
    the latter is more efficient.

    This test can be removed when we have migrated all of the existing pipelines across to the new standard and is
    expected (and accepted) to fail until then.
    """
    dags_with_indexes_on_sa_columns = (
        get_dags_with_non_pk_indexes_on_sqlalchemy_columns()
    )
    assert dags_with_indexes_on_sa_columns == []


def test_table_definitions_are_within_postgres_bounds():
    """
    Postgres limits schema and table names to 63 chars. However, as part of a dag run we need to append
    a timestamp and a suffix to the table name. This effectively leaves us with 42 characters
    available for a table and the full 63 for a schema.
    """
    max_schema_length = 63
    max_table_length = 42

    dag_table_definitions = get_table_definitions_for_all_concrete_dags()
    for dag_class_name, table_configs in dag_table_definitions.items():
        for schema in [x.schema for x in table_configs]:
            assert (
                len(schema) <= max_schema_length
            ), f"{dag_class_name}: Schema {schema} is > {max_schema_length} chars long and will be truncated by postgres"
        for table in [x.table_name for x in table_configs]:
            assert (
                len(table) <= max_table_length
            ), f"{dag_class_name}: Table {table} is > {max_table_length} chars long and will be truncated by postgres"


def test_pipelines_should_declare_tables_outside_of_public_schema():
    """
    The majority of tables created via data-flow should go into a schema other than public, so let's help enforce/remind
    that condition.
    """
    dag_table_definitions = get_dags_with_tables_in_public_schema()
    assert dag_table_definitions == {
        'CoronavirusInteractionsDashboardPipeline': {
            ('public', 'coronavirus_interactions_dashboard_data')
        },
        'CountriesOfInterestServicePipeline': {
            ('public', 'countries_of_interest_dataset')
        },
        'CSSECovid19TimeSeriesGlobal': {('public', 'csse_covid19_time_series_global')},
        'CSSECovid19TimeSeriesGlobalGroupedByCountry': {
            ('public', 'csse_covid19_time_series_global_by_country')
        },
        'DataHubSPIPipeline': {
            ('public', 'datahub_spi'),
            ('public', 'datahub_spi_propositions'),
        },
        'DataWorkspaceApplicationInstancePipeline': {
            ('public', 'dataworkspace__application_instances')
        },
        'DataWorkspaceCatalogueItemsPipeline': {
            ('public', 'dataworkspace__catalogue_items'),
            ('public', 'dataworkspace__source_tables'),
        },
        'DataWorkspaceEventLogPipeline': {('public', 'dataworkspace__event_log')},
        'DataWorkspaceUserPipeline': {('public', 'dataworkspace__users')},
        'ERPPipeline': {('public', 'erp')},
        'FDIDashboardPipeline': {('public', 'fdi_dashboard_data')},
        'MarketAccessTradeBarriersPipeline': {
            ('public', 'market_access_trade_barriers'),
            ('public', 'market_access__ea_impact_assessments'),
            ('public', 'market_access__economic_assessments'),
            ('public', 'market_access_trade_barrier_status_history'),
        },
        'MinisterialInteractionsDashboardPipeline': {
            ('public', 'ministerial_interactions')
        },
        'OxfordCovid19GovernmentResponseTracker': {
            ('public', 'oxford_covid19_government_response_tracker')
        },
        'PeopleFinderPeoplePipeline': {('public', 'people_finder__people')},
        'TagsClassifierTrainPipeline': {
            ('public', 'interactions_tags_classifier_metrics')
        },
        'TagsClassifierPredictionPipeline': {
            ('public', 'interactions_dataset_with_tags')
        },
        'RawWorldBankTariffPipeline': {('public', 'raw_world_bank_tariffs')},
        'RawWorldBankBoundRatePipeline': {('public', 'raw_world_bank_bound_rates')},
    }, "We shouldn't be adding any more tables to the `public` schema."
