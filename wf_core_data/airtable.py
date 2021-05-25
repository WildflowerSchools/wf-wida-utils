import wf_core_data.utils
import requests
import pandas as pd
from collections import OrderedDict
# import pickle
# import json
import datetime
import time
import logging
# import os

logger = logging.getLogger(__name__)

DEFAULT_DELAY = 0.25
DEFAULT_MAX_REQUESTS = 50

SCHOOLS_BASE_ID = 'appJBT9a4f3b7hWQ2'

class AirtableClient:
    def __init__(
        self,
        api_key=None,
        url_base='https://api.airtable.com/v0/'
    ):
        self.api_key = api_key
        self.url_base = url_base
        if self.api_key is None:
            self.api_key = os.getenv('AIRTABLE_API_KEY')

    def fetch_school_data(
        self,
        pull_datetime=None,
        params=None,
        base_id=SCHOOLS_BASE_ID,
        format='dataframe',
        delay=DEFAULT_DELAY,
        max_requests=DEFAULT_MAX_REQUESTS
    ):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching school data from Transparent Classroom for all schools')
        records = self.bulk_get(
            base_id=base_id,
            endpoint='Schools',
            params=params
        )
        school_data=list()
        for record in records:
            fields = record.get('fields', {})
            datum = OrderedDict([
                ('school_id_at', record.get('id')),
                ('school_created_datetime_at', wf_core_data.utils.to_datetime(record.get('createdTime'))),
                ('pull_datetime', pull_datetime),
                ('hub_at', fields.get('Hub')),
                ('pod_at', fields.get('Pod')),
                ('school_name_at', fields.get('Name')),
                ('school_status_at', fields.get('Status')),
                ('school_ssj_stage_at', fields.get('SSJ Stage')),
                ('school_governance_model_at', fields.get('Governance Model')),
                ('school_ages_served_at', fields.get('Ages served')),
                ('school_phone_number_at', fields.get('Phone Number')),
                ('school_location_ids_at', fields.get('Locations')),
                ('school_time_zone_at', fields.get('Time Zone')),
                ('school_tls_at', fields.get('TLs')),
                ('school_id_tc', fields.get('TC_school_ID'))
            ])
            school_data.append(datum)
        if format == 'dataframe':
            school_data = convert_school_data_to_df(school_data)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return school_data

    def bulk_get(
        self,
        base_id,
        endpoint,
        params=None,
        delay=DEFAULT_DELAY,
        max_requests=DEFAULT_MAX_REQUESTS
    ):
        if params is None:
            params = dict()
        num_requests = 0
        records = list()
        while True:
            data = self.get(
                base_id=base_id,
                endpoint=endpoint,
                params=params
            )
            if 'records' in data.keys():
                logging.info('Returned {} records'.format(len(data.get('records'))))
                records.extend(data.get('records'))
            num_requests += 1
            if num_requests >= max_requests:
                logger.warning('Reached maximum number of requests ({}). Terminating.'.format(
                    max_requests
                ))
                break
            offset = data.get('offset')
            if offset is None:
                break
            params['offset'] = offset
            time.sleep(delay)
        return records

    def get(
        self,
        base_id,
        endpoint,
        params=None
    ):
        headers = dict()
        if self.api_key is not None:
            headers['Authorization'] = 'Bearer {}'.format(self.api_key)
        r = requests.get(
            '{}{}/{}'.format(
                self.url_base,
                base_id,
                endpoint
            ),
            params=params,
            headers=headers
        )
        if r.status_code != 200:
            error_message = 'Airtable GET request returned status code {}'.format(r.status_code)
            r.raise_for_status()
        return r.json()

def convert_school_data_to_df(school_data):
    if len(school_data) == 0:
        return pd.DataFrame()
    school_data_df = pd.DataFrame(
        school_data,
        dtype='object'
    )
    school_data_df['pull_datetime'] = pd.to_datetime(school_data_df['pull_datetime'])
    school_data_df['school_created_datetime_at'] = pd.to_datetime(school_data_df['school_created_datetime_at'])
    school_data_df['school_id_tc'] = pd.to_numeric(school_data_df['school_id_tc']).astype('Int64')
    school_data_df = school_data_df.astype({
        'school_id_at': 'string',
        'hub_at': 'string',
        'pod_at': 'string',
        'school_name_at': 'string',
        'school_status_at': 'string',
        'school_ssj_stage_at': 'string',
        'school_governance_model_at': 'string',
        'school_phone_number_at': 'string',
        'school_time_zone_at': 'string',
        'school_tls_at': 'string'
    })
    school_data_df.set_index('school_id_at', inplace=True)
    return school_data_df
