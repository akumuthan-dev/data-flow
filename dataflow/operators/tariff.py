from collections import namedtuple
import contextlib
import csv
import os
import subprocess
import threading
from urllib.parse import urlparse

import boto3
import psycopg2
import psycopg2.extras
from smart_open import smart_open
from streaming_left_join import join

from dataflow.config import (
    UK_TARIFF_BUCKET_NAME,
    UK_TARIFF_BUCKET_AWS_ACCESS_KEY_ID,
    UK_TARIFF_BUCKET_AWS_SECRET_ACCESS_KEY,
)
from dataflow.utils import logger

from .tariff_publish_sql.tariff_publish_sql import (
    temporary_tables_sql,
    nomenclature_sql,
    nomenclature_footnotes_sql,
    measures_sql,
    measures_components_sql,
    measures_conditions_sql,
    measures_condition_components_sql,
    measures_excluded_geographical_areas_sql,
    measures_footnotes_sql,
)


duty_expression_prefixes = {
    '04': '+ ',
    '12': '+ ',
    '14': '+ ',
    '19': '+ ',
    '20': '+ ',
    '21': '+ ',
    '25': '+ ',
    '27': '+ ',
    '29': '+ ',
    '02': '- ',
    '36': '- ',
    '17': 'MAX ',
    '35': 'MAX ',
    '15': 'MIN ',
    '37': 'NIHIL ',
}

duty_expression_amount_codes = {
    '12': 'AC ',
    '14': 'AC (reduced) ',
    '21': 'SD ',
    '25': 'SD (reduced) ',
    '27': 'FD ',
    '29': 'FD (reduced) ',
}

duty_expression_measurement_unit_codes = {
    # The empty values are not necessarily meant to be empty, it's just that we don't know what they mean
    # or how they should be displayed
    'ASV': '/ % vol ',
    'CCT': '/ ct/l ',
    'CEN': '/ 100 p/st ',
    'CTM': '/ c/k ',
    'DAP': '/ 10 000 kg/polar ',
    'DTN': '/ 100 kg ',
    'EUR': '',
    'FC1': '',
    'GFI': '/ gi F/S ',
    'GRM': '/ g ',
    'GRT': '',
    'HLT': '/ hl ',
    'HMT': '/ 100 m ',
    'KGM': '/ kg ',
    'KAC': '',
    'KCC': '',
    'KCL': '',
    'KLT': '/ 1,000 l ',
    'KMA': '/ kg met.am. ',
    'KMT': '',
    'KNI': '/ kg N ',
    'KNS': '/ kg H2O2 ',
    'KPH': '/ kg KOH ',
    'KPO': '/ kg K2O ',
    'KPP': '/ kg P2O5 ',
    'KSD': '/ kg 90 % sdt ',
    'KSH': '/ kg NaOH ',
    'KUR': '/ kg U ',
    'LPA': '/ l alc. 100% ',
    'LTR': '/ l ',
    'MIL': '/ 1,000 items ',
    'MPR': '',
    'MTK': '/ m2 ',
    'MTQ': '/ m3 ',
    'MTR': '/ m ',
    'MWH': '/ 1,000 kWh ',
    'NAR': '/ item ',
    'NCL': '/ ce/el ',
    'NPR': '/ pa ',
    'RET': '',
    'TJO': '/ TJ ',
    'TNE': '/ tonne ',
    'WAT': '',
    None: '',
}

duty_expression_measurement_unit_qualifiers = {
    'A': '/ tot alc ',
    'B': '/ flask',  # Confirm text
    'C': '/ 1 000 ',
    'E': '/ net drained wt ',
    'F': '/ of common wheat ',  # Confirm text
    'G': '/ gross ',
    'I': '/ of biodiesel content ',
    'J': '/ of fuel content ',  # Confirm text
    'K': '/ of bioethanol content ',  # Confirm text
    'L': '/ of live wt ',  # Confirm text
    'M': '/ net dry ',
    'N': '',  # Confirm text
    'P': '/ lactic matter ',
    'R': '/ std qual ',
    'S': '/ raw sugar ',
    'T': '/ dry lactic matter ',
    'X': '/ hl ',
    'Z': '/ % sacchar. ',
    None: '',
}


# Each item in the tree is a node, and we keep track of its ancestors and descendants for conversion to JSON
Node = namedtuple(
    'Node',
    [
        'item',  # e.g. {'goods_nomenclature_item_id': ... }
        'item_footnotes',  # a list of the item's measures
        'item_measures',  # a list of the item's footnotes
        'item_type',  # i.e. {'name': 'commodity', 'leaft': True...
        'ancestors',  # the ancestors of the item
        'descendants',  # the descendants of the item
    ],
)


def get_duty_expression(duty_expression_parts):
    return ' '.join(
        duty_expression_prefixes.get(part['duty_expression_id'], '')
        + (
            '{:.2f}'.format(part['duty_amount'])
            if part['duty_amount'] is not None
            else ''
        )
        + duty_expression_amount_codes.get(
            part['duty_expression_id'],
            (' ' + part['monetary_unit_code'] + ' ')
            if part['monetary_unit_code'] is not None
            else '% ',
        )
        + duty_expression_measurement_unit_codes[part['measurement_unit_code']]
        + duty_expression_measurement_unit_qualifiers[
            part['measurement_unit_qualifier_code']
        ]
        for part in duty_expression_parts
    ).strip()


def with_basic_duty_rate(commodity):
    # We can have multiple additional codes, and the rates can be different
    expressions = list(
        set(
            (
                measure['duty_expression']['base']
                for measure in commodity['import_measures']
                if measure['measure_type']['id'] in ['103', '105']
            )
        )
    )

    return {
        **commodity,
        'basic_duty_rate': expressions[0] if len(expressions) == 1 else None,
    }


def join_measures(ancestors_with_measures, measures):
    if ancestors_with_measures is not None:
        for ancestor_node in ancestors_with_measures:
            (
                item,
                item_footnotes,
                item_measures_with_excluded_geo_and_components_and_conditions_and_footnotes,
                item_type,
                parent,
                children,
            ) = ancestor_node
            for (
                i
            ) in item_measures_with_excluded_geo_and_components_and_conditions_and_footnotes:
                yield i
    if measures is not None:
        for i in measures:
            yield i


def csv_date_allow_null(date_obj):
    return date_obj.isoformat()[:10] if date_obj is not None else None


def ingest_uk_tariff_to_temporary_db(task_instance, **kwargs):
    # The UK Tariff comes as a pgdump, to be ingested into the DB into its own, temporary, database
    # using pg_restore. pg_restore is an application run in a separate process. While technically,
    # we could use Python to drop/create the database, pg_restore is (usually) part of the same
    # package, and has similar command line arguments to psql, so we use the same pattern for both
    # both

    ######

    logger.info('Extracting credentials from environent')

    parsed_db_uri = urlparse(os.environ['AIRFLOW_CONN_DATASETS_DB'])
    host, port, dbname, user, password = (
        parsed_db_uri.hostname,
        parsed_db_uri.port or '5432',
        parsed_db_uri.path.strip('/'),
        parsed_db_uri.username,
        parsed_db_uri.password or '',  # No password locally
    )
    logger.info('host: %s, port: %s, dbname: %s, user:%s', host, port, dbname, user)

    ######

    logger.info('(Re)creating temporary database')

    completed_recreate_process = subprocess.run(
        [
            "psql",
            "-h",
            host,
            "-p",
            port,
            "-U",
            user,
            "-d",
            dbname,
            "-c",
            "SELECT pg_terminate_backend(pg_stat_activity.pid) "
            "FROM pg_stat_activity "
            "WHERE pg_stat_activity.datname = 'temporary__uk_tariff'"
            "AND pid <> pg_backend_pid();",
            "-c",
            "DROP DATABASE IF EXISTS temporary__uk_tariff;",
            "-c",
            "CREATE DATABASE temporary__uk_tariff;",
        ],
        bufsize=1048576,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env={**os.environ, 'PGPASSWORD': password},
    )
    for line in completed_recreate_process.stdout.splitlines():
        logger.info('psql >>>> ' + line.rstrip().decode('utf-8'))
    completed_recreate_process.check_returncode()

    ######

    logger.info('Finding latest key')

    s3_client = boto3.client(
        's3',
        aws_access_key_id=UK_TARIFF_BUCKET_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=UK_TARIFF_BUCKET_AWS_SECRET_ACCESS_KEY,
    )

    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterable = paginator.paginate(
        Bucket=UK_TARIFF_BUCKET_NAME, Prefix='uk/', Delimiter='/'
    )

    latest_key, latest_etag = None, None
    for page in page_iterable:
        for item in page.get('Contents', []):
            latest_key = item['Key']
            latest_etag = item['ETag']
    if latest_key is None:
        raise Exception('No keys found in bucket')

    latest_etag = latest_etag.strip('"')
    logger.info('Latest key: %s', latest_key)
    logger.info('Latest etag: %s', latest_etag)

    ######

    logger.info('Fetching object and piping to pg_restore')

    s3_response = s3_client.get_object(Bucket=UK_TARIFF_BUCKET_NAME, Key=latest_key)

    import_process = subprocess.Popen(
        [
            "pg_restore",
            "-h",
            host,
            "-p",
            port,
            "-d",
            "temporary__uk_tariff",
            "-U",
            user,
        ],
        bufsize=1048576,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.PIPE,
        env={**os.environ, 'PGPASSWORD': password},
    )

    def log_stdout(proc):
        for line in iter(import_process.stdout.readline, b''):
            logger.info(line.rstrip().decode('utf-8'))

    threading.Thread(target=log_stdout).start()

    for chunk in s3_response['Body'].iter_chunks(65536):
        import_process.stdin.write(chunk)

    import_process.stdin.close()
    import_process.wait()

    # Unfortunately, there are lots of "errors" in the restore, so pg_restore exits with a
    # non-zero code, but it is a success in terms of the import of data
    logger.info('pg_restore existed wth code %s', import_process.returncode)

    task_instance.xcom_push('latest_tariff_s3_key', latest_key)
    task_instance.xcom_push('latest_tariff_s3_etag', latest_etag)

    logger.info('Done')


def create_uk_tariff_csvs(task_instance, **kwargs):
    latest_key = task_instance.xcom_pull(key='latest_tariff_s3_key')
    latest_etag = task_instance.xcom_pull(key='latest_tariff_s3_etag')

    logger.info('latest_key: %s', latest_key)
    logger.info('latest_etag: %s', latest_etag)

    logger.info('Creating temporary tables... (takes about a minute)')

    # The SQL takes a parameter, active_at, to include measures that are active at a certain date
    # (At some point, this will probably need to be become a range of dates)
    active_at = '2021-01-01'

    @contextlib.contextmanager
    def get_conn():
        parsed_db_uri = urlparse(os.environ['AIRFLOW_CONN_DATASETS_DB'])
        host, port, user, password = (
            parsed_db_uri.hostname,
            parsed_db_uri.port or '5432',
            parsed_db_uri.username,
            parsed_db_uri.password or '',  # No password locally
        )
        conn = psycopg2.connect(
            host=host,
            user=user,
            port=port,
            password=password,
            dbname="temporary__uk_tariff",
        )
        try:
            conn.set_session(autocommit=False, isolation_level='REPEATABLE READ')
            yield conn
        finally:
            conn.close()

    def get(conn, sql, name):
        with conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor, name=name
        ) as cur:
            cur.itersize = 200
            cur.arraysize = 200
            cur.execute(sql, {'active_at': active_at})
            yield from cur

    def run(conn, sql):
        with conn.cursor() as cur:
            cur.execute(sql, {'active_at': active_at})

    def get_nomenclature_measures_type(
        nomenclature__with__footnotes_and_measures_with_components_and_conditions_and_footnotes,
    ):
        # We have to defer a decision for the type of each item until we iterate the next one since
        # we only know if an item is a leaf if the next indent level is equal to or less than it
        #
        # We iterate using "next" variables, yield "curr" variable (populated form the previous
        # iterations "next" variables), at the end of each iteration copy curr->prev, and next->curr,
        # and make a decision on the type of "curr" using all 3 of these
        #
        # The below may be more complex than it needs to be: we don't need prev, curr and next
        # available. This is a remnant of when it was thought to be needed, but keeping for now
        # just in case.

        def get_name(item):
            # fmt: off
            return \
                'chapter' if item['number_indents'] == 0 and item['goods_nomenclature_item_id'][2:] == '00000000' else \
                'heading-l1' if item['number_indents'] == 0 and item['productline_suffix'] == '10' else \
                'heading-l2' if item['number_indents'] == 0 else \
                'commodity'
            # fmt: on

        def get_pseudo_indent(name, item):
            # fmt: off
            return \
                item['number_indents'] + 1 if name == 'chapter' else \
                item['number_indents'] + 2 if name == 'heading-l1' else \
                item['number_indents'] + 3 if name == 'heading-l2' else \
                item['number_indents'] + 3  # Commodities real indent starts at 1, so pseudo-indent start at 4
            # fmt: on

        def get_type_curr():
            name_curr = get_name(item_curr)
            name_next = get_name(item_next)
            pseudo_indent_curr = get_pseudo_indent(name_curr, item_curr)
            pseudo_indent_next = get_pseudo_indent(name_next, item_next)

            return {
                'name': name_curr,
                'leaf': pseudo_indent_next is None
                or pseudo_indent_next <= pseudo_indent_curr,
            }

        _, _, _, _ = (None, None, None, None)
        item_curr, foot_curr, meas_curr, type_curr = next(
            nomenclature__with__footnotes_and_measures_with_components_and_conditions_and_footnotes
        ) + (None,)

        for (
            item_next,
            foot_next,
            meas_next,
        ) in nomenclature__with__footnotes_and_measures_with_components_and_conditions_and_footnotes:

            type_curr = get_type_curr()
            yield item_curr, foot_curr, meas_curr, type_curr

            _, _, _, _ = (
                item_curr,
                foot_curr,
                meas_curr,
                type_curr,
            )
            item_curr, foot_curr, meas_curr, type_curr = (
                item_next,
                foot_next,
                meas_next,
                None,
            )  # The type of next iteration's "curr" is not known until the next iteration

        item_next, foot_next, meas_next = (None, None, None)
        yield item_curr, foot_curr, meas_curr, get_type_curr()

    def augment_ancestors_and_descendants(
        nomenclature__with__footnotes_and_measures_with_excluded_geo_with_components_and_conditions_and_footnotes__and__type,
    ):
        # Augments the stream with more objects / some required parts of the tree structure
        #
        # The ancestor nodes are known when we first iterate to the node. The descendant nodes are
        # populated as we iterate later nodes.
        #
        # Some of this is more complex than necessary _just_ for the CSV generation, since it is
        # taken from code used to generate JSON that needed more structure

        latest_node_at_level = {}
        latest_node_at_level[-1] = Node(
            None, [], [], {'name': 'sections'}, None, []
        )  # Hardcoded
        latest_node_at_level[0] = Node(
            {'goods_nomenclature_sid': 'fake'}, [], [], {'name': 'chapters'}, None, []
        )  # Will be appended to

        def yield_if_same_level_or_going_down(from_level, to_level):
            for level in range(from_level, to_level - 1, -1):
                try:
                    node = latest_node_at_level[level]
                except KeyError:
                    # `heading-l1` (level 2) is not present in all chapters, and in Chapter 99, we
                    # have `heading-l1` without the previous heading being an `heading-l2` (level 3).
                    # We could refine exactly when we raise to catch more error conditions, but KISS
                    if level not in [2, 3]:
                        raise
                    else:
                        continue

                yield node

                # Clear the node's children from memory. This node could be a child of another
                # node, but we shouldn't need to access that node's grandchildren. The exception
                # is heading-l1. We need its children (which are heading-l2) when populating
                # chapters.
                if node.item_type['name'] != 'heading-l1':
                    node.descendants.clear()
                    node.descendants.append(
                        None
                    )  # A chance that an exception will be raised if used

                # We shouldn't need access to this node again, other than if it's a child of another node
                del latest_node_at_level[level]

        previous_pseudo_indent = 0
        for (
            item,
            foot,
            meas,
            item_type,
        ) in nomenclature__with__footnotes_and_measures_with_excluded_geo_with_components_and_conditions_and_footnotes__and__type:
            # Artifically raise the indent level to get a "real" level per indent to simpify the cases
            pseudo_indent = (
                item['number_indents'] + 1
                if item_type['name'] == 'chapter'
                else item['number_indents'] + 2
                if item_type['name'] == 'heading-l1'
                else item['number_indents'] + 3
                if item_type['name'] == 'heading-l2'
                else item['number_indents'] + 3
            )  # Commodities real indent starts at 1, so pseudo-indent start at 4

            yield from yield_if_same_level_or_going_down(
                previous_pseudo_indent, pseudo_indent
            )

            def raise_exception():
                raise Exception('Unable to find ancestor nodes for {}'.format(item))

            ancestors = []
            for ancestor_level in range(-1, pseudo_indent):
                try:
                    ancestors.append(latest_node_at_level[ancestor_level])
                except KeyError:
                    pass
            descendants = []
            node = Node(item, foot, meas, item_type, ancestors, descendants)

            # Heading level 2 needs to be added to chapter _and_ heading level 1
            ancestors_to_populate_descendants = (
                [latest_node_at_level[3]]
                if pseudo_indent >= 4
                else [latest_node_at_level[1], latest_node_at_level[2]]
                if pseudo_indent == 3 and 2 in latest_node_at_level
                else [latest_node_at_level[1]]
                if pseudo_indent in [2, 3]
                else [latest_node_at_level[0]]
                if pseudo_indent == 1
                else [(lambda: raise_exception())]
            )

            for ancestor in ancestors_to_populate_descendants:
                ancestor.descendants.append(node)

            latest_node_at_level[pseudo_indent] = node
            previous_pseudo_indent = pseudo_indent

        yield from yield_if_same_level_or_going_down(previous_pseudo_indent, -1)

    csv_fieldnames_as_defined = [
        'id',
        'commodity__sid',
        'commodity__code',
        'commodity__indent',
        'commodity__description',
        'measure__sid',
        'measure__type__id',
        'measure__type__description',
        'measure__additional_code__code',
        'measure__additional_code__description',
        'measure__duty_expression',
        'measure__effective_start_date',
        'measure__effective_end_date',
        'measure__reduction_indicator',
        'measure__footnotes',
        'measure__conditions',
        'measure__geographical_area__sid',
        'measure__geographical_area__id',
        'measure__geographical_area__description',
        'measure__excluded_geographical_areas__ids',
        'measure__excluded_geographical_areas__descriptions',
        'measure__quota__order_number',
    ]
    csv_fieldnames_on_commodities = [
        'id',
        'commodity__sid',
        'commodity__code',
        'commodity__description',
        'measure__sid',
        'measure__type__id',
        'measure__type__description',
        'measure__additional_code__code',
        'measure__additional_code__description',
        'measure__duty_expression',
        'measure__effective_start_date',
        'measure__effective_end_date',
        'measure__reduction_indicator',
        'measure__footnotes',
        'measure__conditions',
        'measure__geographical_area__sid',
        'measure__geographical_area__id',
        'measure__geographical_area__description',
        'measure__excluded_geographical_areas__ids',
        'measure__excluded_geographical_areas__descriptions',
        'measure__quota__order_number',
    ]

    def get_full_csv_row(
        row_id,
        item,
        item_footnotes,
        item_measures_with_excluded_geo_and_components_and_conditions_and_footnotes,
        item_type,
        ancestors,
        children,
        measure,
        measure_excluded_geo,
        measure_components,
        measure_conditions,
        measure_footnotes,
    ):
        return {
            'id': row_id,
            'commodity__sid': item['goods_nomenclature_sid'],
            'commodity__code': item['goods_nomenclature_item_id'],
            'commodity__indent': item['number_indents'],
            'commodity__description': item['description'],
            'measure__sid': measure['measure_sid'],
            'measure__type__id': measure['measure_type_id'],
            'measure__type__description': measure['description'],
            'measure__additional_code__code': (
                measure['additional_code_type_id'] + measure['additional_code_id']
            )
            if measure['additional_code_type_id'] is not None
            else None,
            'measure__additional_code__description': measure[
                'additional_code_description'
            ]
            if measure['additional_code_description'] is not None
            else None,
            'measure__duty_expression': get_duty_expression(measure_components),
            'measure__effective_start_date': csv_date_allow_null(
                measure['effective_start_date']
            ),
            'measure__effective_end_date': csv_date_allow_null(
                measure['effective_end_date']
            ),
            'measure__reduction_indicator': measure['reduction_indicator'],
            'measure__footnotes': '|'.join(
                [
                    measure_footnote['footnote_type_id']
                    + measure_footnote['footnote_id']
                    for measure_footnote in measure_footnotes
                ]
            ),
            'measure__conditions': '|'.join(
                [
                    ','.join(
                        filter(
                            None,
                            [
                                ('condition:' + measure_condition['condition_code'])
                                if measure_condition['condition_code']
                                else None,
                                (
                                    'certificate:' + measure_condition['document_code']
                                    if measure_condition['document_code']
                                    else None
                                ),
                                ('action:' + measure_condition['action_code'])
                                if measure_condition['action_code']
                                else None,
                                (
                                    'duty:'
                                    + get_duty_expression(measure_condition_components)
                                )
                                if get_duty_expression(measure_condition_components)
                                else None,
                            ],
                        )
                    )
                    for measure_condition, measure_condition_components in measure_conditions
                ]
            ),
            'measure__geographical_area__sid': measure['geographical_area_sid'],
            'measure__geographical_area__id': measure['geographical_area_id'],
            'measure__geographical_area__description': measure[
                'geographical_area_description'
            ],
            'measure__excluded_geographical_areas__ids': '|'.join(
                [
                    excluded_area['geographical_area_id']
                    for excluded_area in measure_excluded_geo
                ]
            ),
            'measure__excluded_geographical_areas__descriptions': '|'.join(
                [
                    excluded_area['geographical_area_description']
                    for excluded_area in measure_excluded_geo
                ]
            ),
            'measure__quota__order_number': measure['quota_order_number_id'],
        }

    with get_conn() as conn:

        run(conn, temporary_tables_sql)

        logger.info(
            'Starting conversion... (this takes about a minute to output the first measure)'
        )

        nomenclature = get(conn, nomenclature_sql, 'nomenclature')
        nomenclature_footnotes = get(
            conn, nomenclature_footnotes_sql, 'nomenclature_footnotes'
        )
        measures = get(conn, measures_sql, 'measures')
        measures_excluded_geographical_areas = get(
            conn,
            measures_excluded_geographical_areas_sql,
            'measures_excluded_geographical_areas',
        )
        measures_components = get(conn, measures_components_sql, 'measures_components')
        measures_footnotes = get(conn, measures_footnotes_sql, 'measures_footnotes')
        measures_conditions = get(conn, measures_conditions_sql, 'measures_conditions')
        measures_conditions_components = get(
            conn, measures_condition_components_sql, 'measures_condition_components'
        )

        measures_conditions_with_components = join(
            (
                measures_conditions,
                lambda measure_condition: measure_condition['measure_condition_sid'],
            ),
            (
                measures_conditions_components,
                lambda component: component['measure_condition_sid'],
            ),
        )
        measures_with_excluded_geo_and_components_and_conditions_and_footnotes = join(
            (measures, lambda measure: measure['measure_sid']),
            (measures_excluded_geographical_areas, lambda area: area['measure_sid']),
            (measures_components, lambda component: component['measure_sid']),
            (
                measures_conditions_with_components,
                lambda condition_with_components: condition_with_components[0][
                    'measure_sid'
                ],
            ),
            (measures_footnotes, lambda footnote: footnote['measure_sid']),
        )
        nomenclature__with__footnotes_and_measures_with_excluded_geo_with_components_and_conditions_and_footnotes = join(
            (nomenclature, lambda item: item['goods_nomenclature_sid']),
            (
                nomenclature_footnotes,
                lambda footnote: footnote['goods_nomenclature_sid'],
            ),
            (
                measures_with_excluded_geo_and_components_and_conditions_and_footnotes,
                lambda measure_with_excluded_geo_and_components_and_conditions_and_footnotes: measure_with_excluded_geo_and_components_and_conditions_and_footnotes[
                    0
                ][
                    'goods_nomenclature_sid'
                ],
            ),
        )

        nomenclature__with__footnotes_and_measures_with_excluded_geo_with_components_and_conditions_and_footnotes__and__type = get_nomenclature_measures_type(
            nomenclature__with__footnotes_and_measures_with_excluded_geo_with_components_and_conditions_and_footnotes
        )
        nomenclature__with__footnotes_and_measures_with_excluded_geo_with_components_and_conditions_and_footnotes__and__type__augmented = augment_ancestors_and_descendants(
            nomenclature__with__footnotes_and_measures_with_excluded_geo_with_components_and_conditions_and_footnotes__and__type
        )

        logger.info('bucket %s', UK_TARIFF_BUCKET_NAME)

        transport_params = {
            'session': boto3.Session(
                aws_access_key_id=UK_TARIFF_BUCKET_AWS_ACCESS_KEY_ID,
                aws_secret_access_key=UK_TARIFF_BUCKET_AWS_SECRET_ACCESS_KEY,
            )
        }
        with smart_open(
            f's3://{UK_TARIFF_BUCKET_NAME}/{latest_key}/{latest_etag}/measures-as-defined.csv',
            'w',
            newline='',
            transport_params=transport_params,
        ) as csv_as_defined, smart_open(
            f's3://{UK_TARIFF_BUCKET_NAME}/{latest_key}/{latest_etag}/measures-on-declarable-commodities.csv',
            'w',
            newline='',
            transport_params=transport_params,
        ) as csv_commodities:

            as_defined_writer = csv.DictWriter(
                csv_as_defined,
                fieldnames=csv_fieldnames_as_defined,
                delimiter=',',
                quoting=csv.QUOTE_NONNUMERIC,
            )
            as_defined_writer.writeheader()
            commodity_writer = csv.DictWriter(
                csv_commodities,
                fieldnames=csv_fieldnames_on_commodities,
                delimiter=',',
                quoting=csv.QUOTE_NONNUMERIC,
            )
            commodity_writer.writeheader()
            commodity_row_id = 0
            as_defined_row_id = 0

            for (
                item,
                item_footnotes,
                item_measures_with_excluded_geo_and_components_and_conditions_and_footnotes,
                item_type,
                ancestors,
                children,
            ) in nomenclature__with__footnotes_and_measures_with_excluded_geo_with_components_and_conditions_and_footnotes__and__type__augmented:
                if item_type.get('leaf', False):
                    for (
                        measure,
                        measure_excluded_geo,
                        measure_components,
                        measure_conditions,
                        measure_footnotes,
                    ) in join_measures(
                        ancestors,
                        item_measures_with_excluded_geo_and_components_and_conditions_and_footnotes,
                    ):
                        commodity_row_id += 1
                        full_row = get_full_csv_row(
                            commodity_row_id,
                            item,
                            item_footnotes,
                            item_measures_with_excluded_geo_and_components_and_conditions_and_footnotes,
                            item_type,
                            ancestors,
                            children,
                            measure,
                            measure_excluded_geo,
                            measure_components,
                            measure_conditions,
                            measure_footnotes,
                        )
                        commodity_writer.writerow(
                            {k: full_row[k] for k in csv_fieldnames_on_commodities}
                        )

                for (
                    measure,
                    measure_excluded_geo,
                    measure_components,
                    measure_conditions,
                    measure_footnotes,
                ) in item_measures_with_excluded_geo_and_components_and_conditions_and_footnotes:
                    as_defined_row_id += 1
                    full_row = get_full_csv_row(
                        as_defined_row_id,
                        item,
                        item_footnotes,
                        item_measures_with_excluded_geo_and_components_and_conditions_and_footnotes,
                        item_type,
                        ancestors,
                        children,
                        measure,
                        measure_excluded_geo,
                        measure_components,
                        measure_conditions,
                        measure_footnotes,
                    )
                    as_defined_writer.writerow(
                        {k: full_row[k] for k in csv_fieldnames_as_defined}
                    )

        logger.info('Done')
