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

    def fetch_tl_data(
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
        logger.info('Fetching TL data from Airtable')
        records = self.bulk_get(
            base_id=base_id,
            endpoint='TLs',
            params=params
        )
        tl_data=list()
        for record in records:
            fields = record.get('fields', {})
            datum = OrderedDict([
                ('teacher_id_at', record.get('id')),
                ('teacher_created_datetime_at', wf_core_data.utils.to_datetime(record.get('createdTime'))),
                ('pull_datetime', pull_datetime),
                ('teacher_full_name_at', fields.get('Full Name')),
                ('teacher_first_name_at', fields.get('First Name')),
                ('teacher_middle_name_at', fields.get('Middle Name')),
                ('teacher_last_name_at', fields.get('Last Name')),
                ('teacher_title_at', fields.get('Title')),
                ('teacher_ethnicity_at', fields.get('Race & Ethnicity')),
                ('teacher_ethnicity_other_at', fields.get('Race & Ethnicity - Other')),
                ('teacher_income_background_at', fields.get('Income Background')),
                ('teacher_email_at', fields.get('Email')),
                ('teacher_email_2_at', fields.get('Email 2')),
                ('teacher_email_3_at', fields.get('Email 3')),
                ('teacher_phone_at', fields.get('Phone Number')),
                ('teacher_phone_2_at', fields.get('Phone Number 2')),
                ('teacher_employer_at', fields.get('Employer')),
                ('hub_at', fields.get('Hub')),
                ('pod_at', fields.get('Pod')),
                ('user_id_tc', fields.get('TC User ID'))
            ])
            tl_data.append(datum)
        if format == 'dataframe':
            tl_data = convert_tl_data_to_df(tl_data)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return tl_data

    def fetch_location_data(
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
        logger.info('Fetching location data from Airtable')
        records = self.bulk_get(
            base_id=base_id,
            endpoint='Locations',
            params=params
        )
        location_data=list()
        for record in records:
            fields = record.get('fields', {})
            datum = OrderedDict([
                ('location_id_at', record.get('id')),
                ('location_created_datetime_at', wf_core_data.utils.to_datetime(record.get('createdTime'))),
                ('pull_datetime', pull_datetime),
                ('location_address_at', fields.get('Address')),
                ('school_id_at', wf_core_data.utils.to_singleton(fields.get('School Name'))),
                ('school_location_start_at', wf_core_data.utils.to_date(fields.get('Start of time at location'))),
                ('school_location_end_at', wf_core_data.utils.to_date(fields.get('End of time at location')))
            ])
            location_data.append(datum)
        if format == 'dataframe':
            location_data = convert_location_data_to_df(location_data)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return location_data

    def fetch_teacher_school_data(
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
        logger.info('Fetching teacher school association data from Airtable')
        records = self.bulk_get(
            base_id=base_id,
            endpoint='Teachers x Schools',
            params=params
        )
        teacher_school_data=list()
        for record in records:
            fields = record.get('fields', {})
            datum = OrderedDict([
                ('teacher_school_id_at', record.get('id')),
                ('teacher_school_created_datetime_at', wf_core_data.utils.to_datetime(record.get('createdTime'))),
                ('pull_datetime', pull_datetime),
                ('teacher_id_at', fields.get('TL')),
                ('school_id_at', fields.get('School')),
                ('teacher_school_start_at', wf_core_data.utils.to_date(fields.get('Start Date'))),
                ('teacher_school_end_at', wf_core_data.utils.to_date(fields.get('End Date'))),
                ('teacher_school_active_at', wf_core_data.utils.to_boolean(fields.get('Currently Active')))
            ])
            teacher_school_data.append(datum)
        if format == 'dataframe':
            teacher_school_data = convert_teacher_school_data_to_df(teacher_school_data)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return teacher_school_data

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
        logger.info('Fetching school data from Airtable')
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

    def post(
        self,
        base_id,
        endpoint,
        data
    ):
        headers = dict()
        if self.api_key is not None:
            headers['Authorization'] = 'Bearer {}'.format(self.api_key)
        r = requests.post(
            '{}{}/{}'.format(
                self.url_base,
                base_id,
                endpoint
            ),
            headers=headers,
            json=data
        )
        if r.status_code != 200:
            error_message = 'Airtable POST request returned status code {}'.format(r.status_code)
            r.raise_for_status()
        return r.json()

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

def convert_tl_data_to_df(tl_data):
    if len(tl_data) == 0:
        return pd.DataFrame()
    tl_data_df = pd.DataFrame(
        tl_data,
        dtype='object'
    )
    tl_data_df['pull_datetime'] = pd.to_datetime(tl_data_df['pull_datetime'])
    tl_data_df['teacher_created_datetime_at'] = pd.to_datetime(tl_data_df['teacher_created_datetime_at'])
    # school_data_df['user_id_tc'] = pd.to_numeric(tl_data_df['user_id_tc']).astype('Int64')
    tl_data_df = tl_data_df.astype({
        'teacher_full_name_at': 'string',
        'teacher_middle_name_at': 'string',
        'teacher_last_name_at': 'string',
        'teacher_title_at': 'string',
        'teacher_ethnicity_at': 'string',
        'teacher_ethnicity_other_at': 'string',
        'teacher_income_background_at': 'string',
        'teacher_email_at': 'string',
        'teacher_email_2_at': 'string',
        'teacher_email_3_at': 'string',
        'teacher_phone_at': 'string',
        'teacher_phone_2_at': 'string',
        'teacher_employer_at': 'string',
        'hub_at': 'string',
        'pod_at': 'string',
        'user_id_tc': 'string'
    })
    tl_data_df.set_index('teacher_id_at', inplace=True)
    return tl_data_df

def convert_location_data_to_df(location_data):
    if len(location_data) == 0:
        return pd.DataFrame()
    location_data_df = pd.DataFrame(
        location_data,
        dtype='object'
    )
    location_data_df['pull_datetime'] = pd.to_datetime(location_data_df['pull_datetime'])
    location_data_df['location_created_datetime_at'] = pd.to_datetime(location_data_df['location_created_datetime_at'])
    location_data_df = location_data_df.astype({
        'location_id_at': 'string',
        'location_address_at': 'string',
        'school_id_at': 'string'
    })
    location_data_df.set_index('location_id_at', inplace=True)
    return location_data_df

def convert_teacher_school_data_to_df(teacher_school_data):
    if len(teacher_school_data) == 0:
        return pd.DataFrame()
    teacher_school_data_df = pd.DataFrame(
        teacher_school_data,
        dtype='object'
    )
    teacher_school_data_df['pull_datetime'] = pd.to_datetime(teacher_school_data_df['pull_datetime'])
    teacher_school_data_df['teacher_school_created_datetime_at'] = pd.to_datetime(teacher_school_data_df['teacher_school_created_datetime_at'])
    teacher_school_data_df = teacher_school_data_df.astype({
        'teacher_school_active_at': 'bool'
    })
    teacher_school_data_df.set_index('teacher_school_id_at', inplace=True)
    return teacher_school_data_df

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
    })
    school_data_df.set_index('school_id_at', inplace=True)
    return school_data_df
