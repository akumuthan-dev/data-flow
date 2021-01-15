from dataflow.utils import S3Data, logger


def fetch_from_bigquery(
    table_name: str, ts_nodash, **kwargs,
):
    s3 = S3Data(table_name, ts_nodash)
    s3.write_key(
        '000001.json',
        [
            {'id': 'id-1', 'SomeName': 'some-name-1'},
            {'id': 'id-2', 'SomeName': 'some-name-2'},
        ],
    )
    logger.info('Done!')
