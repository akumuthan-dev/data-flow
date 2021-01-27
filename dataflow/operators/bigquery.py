import json
from google.cloud import bigquery as bq
from datetime import timedelta, datetime
from dataflow.utils import S3Data, logger

def fetch_from_bigquery(
    table_name: str, ts_nodash, **kwargs,
):
    #service_account_file = "/home/akumuthan/workspace/data-flow/keys/bigquery-great-ga360-7d9ec550dce0.json"
    service_account_file = "/home/vcap/app/keys/bigquery-great-ga360-7d9ec550dce0.json"

    #bq_date = '20201106'
    #bq_table = "bigquery-great-ga360.189502937.ga_sessions_" + bq_date
    d = datetime.today().date() - timedelta(days=1)
    bq_table = "bigquery-great-ga360.189502937.ga_sessions_" + str(d).replace("-","")
    #print("Big Query Table is:")
    #print(bq_table)

    bq_query = '''
    SELECT clientId
        ,fullVisitorId
        ,userId
        ,visitNumber
        ,visitId
        ,visitStartTime
        ,date
        ,totals.bounces
        ,totals.newVisits
        ,totals.pageviews
        ,totals.visits
        ,device.browser
        ,device.browserVersion
        ,device.deviceCategory
        ,device.mobileDeviceInfo
        ,device.mobileDeviceModel
        ,device.operatingSystem
        ,device.operatingSystemVersion
        ,device.language
        ,geoNetwork.continent
        ,geoNetwork.subContinent
        ,geoNetwork.country
        ,geoNetwork.region
        ,geoNetwork.metro
        ,geoNetwork.city
        ,geoNetwork.cityId
        ,geoNetwork.latitude
        ,geoNetwork.longitude
        ,h.dataSource
        ,h.type
        ,h.page.pagePath
        ,h.page.pagePathLevel1
        ,h.page.pagePathLevel2
        ,h.page.pagePathLevel3
        ,h.page.pagePathLevel4
        ,h.page.hostname
        ,h.page.pageTitle
        ,h.page.searchKeyword
    FROM ''' + bq_table + '''
    LEFT JOIN UNNEST(hits) as h
    LIMIT 5
    '''
    #print("\nQuery to be run on Big Query is:")
    #print(bq_query)

    #bq_client = bq.Client() #when using exported environment variable
    bq_client = bq.Client.from_service_account_json(service_account_file)

    dataset = bq_client.query(bq_query)

    #debug
    print("\nBig Query Results are:")
    fields = []
    for d in dataset:
        fields = list(d.keys())
        break
    print(fields)
    for record in dataset:
        fullrecord = []
        for i in range(len(record)):
            if i==(len(record)-1):
                fullrecord.append(record[i])
            else:
                fullrecord.append(record[i])
        print(fullrecord)
        fullrecord.clear()

    s3 = S3Data(table_name, ts_nodash)
    s3.write_key(
        '000001.json',
        [
            {'id': 'id-1', 'SomeName': 'some-name-1'},
            {'id': 'id-2', 'SomeName': 'some-name-2'},
        ],
    )
    logger.info('Done!')
