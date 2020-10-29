from airflow.models.dagbag import DagBag


def test_pipelines_dags():
    dagbag = DagBag('dataflow', include_examples=False)
    assert set(dagbag.dag_ids) == {
        'AdvisersDatasetPipeline',
        'AdvisersLastInteractionPipeline',
        'AppleCovid19MobilityTrendsPipeline',
        'CabinetOfficeGenderPayGapPipeline',
        'CSSECovid19TimeSeriesGlobal',
        'CSSECovid19TimeSeriesGlobalGroupedByCountry',
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
        'DataWorkspaceEventLogPipeline',
        'DataWorkspaceUserPipeline',
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
        'GlobalUKTariffPipeline',
        'GoogleCovid19MobilityReports',
        'GreatGOVUKExportOpportunitiesPipeline',
        'GreatGOVUKExportOpportunityEnquiriesMatchingPipeline',
        'GreatGOVUKExportOpportunityEnquiriesPipeline',
        'GreatGovUKFormsPipeline',
        'HMRCEUExports',
        'HMRCEUImports',
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
        'MaxemailCampaignsSentPipeline',
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
        'TagsClassifierPipeline',
        'TeamsDatasetPipeline',
        'ZendeskDITTicketsPipeline',
        'ZendeskUKTradeTicketsPipeline',
    }
