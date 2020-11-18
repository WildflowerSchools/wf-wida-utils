import requests
import pandas as pd
import json
import logging
import os

logger = logging.getLogger(__name__)

class TransparentClassroomClient:
    def __init__(
        self,
        username=None,
        password=None,
        api_token=None,
        url_base='https://www.transparentclassroom.com/api/v1/'
    ):
        self.username = username
        self.password = password
        self.api_token = api_token
        self.url_base = url_base
        if self.api_token is None:
            self.api_token = os.getenv('TRANSPARENT_CLASSROOM_API_TOKEN')
        if self.api_token is None:
            logger.info('Transparent Classroom API token not specified. Attempting to generate token.')
            if self.username is None:
                self.username = os.getenv('TRANSPARENT_CLASSROOM_USERNAME')
            if self.username is None:
                raise ValueError('Transparent Classroom username not specified')
            if self.password is None:
                self.password = os.getenv('TRANSPARENT_CLASSROOM_PASSWORD')
            if self.password is None:
                raise ValueError('Transparent Classroom password not specified')
            output_json = self.transparent_classroom_request(
                'authenticate.json',
                auth=(self.username, self.password)
            )
            self.api_token = output_json['api_token']

    def fetch_school_ids(self):
        output_json = self.transparent_classroom_request('schools.json')
        school_ids = list()
        for school in output_json:
            if school.get('type') == 'School':
                school_ids.append(int(school.get('id')))
        return school_ids

    def fetch_school_data(self):
        output_json = self.transparent_classroom_request('schools.json')
        school_data_df = pd.DataFrame(output_json)
        school_data_df = school_data_df.loc[school_data_df['type'] == "School"].copy()
        school_data_df.rename(
            columns= {
                'id': 'tc_school_id',
                'name': 'tc_school_name',
                'address': 'tc_school_address',
                'phone': 'tc_school_phone',
                'time_zone': 'tc_school_time_zone'
            },
            inplace=True
        )
        school_data_df['tc_school_id'] = school_data_df['tc_school_id'].astype('Int64')
        school_data_df['tc_school_name'] = school_data_df['tc_school_name'].astype('string')
        school_data_df['tc_school_address'] = school_data_df['tc_school_address'].astype('string')
        school_data_df['tc_school_phone'] = school_data_df['tc_school_phone'].astype('string')
        school_data_df['tc_school_time_zone'] = school_data_df['tc_school_time_zone'].astype('string')
        school_data_df = school_data_df.reindex(columns=[
                'tc_school_id',
                'tc_school_name',
                'tc_school_address',
                'tc_school_phone',
                'tc_school_time_zone'
        ])
        return school_data_df

    def transparent_classroom_request(
        self,
        endpoint,
        params=None,
        school_id=None,
        masquerade_id=None,
        auth=None
    ):
        headers = dict()
        if self.api_token is not None:
            headers['X-TransparentClassroomToken'] = self.api_token
        if school_id is not None:
            headers['X-TransparentClassroomSchoolId'] = str(school_id)
        if masquerade_id is not None:
            headers['X-TransparentClassroomMasqueradeId'] = str(masquerade_id)
        r = requests.get(
            '{}{}'.format(self.url_base, endpoint),
            params=params,
            headers=headers,
            auth=auth
        )
        if r.status_code != 200:
            error_message = 'Transparent Classroom request returned error'
            if r.json().get('errors') is not None:
                error_message += '\n{}'.format(json.dumps(r.json().get('errors'), indent=2))
            raise Exception(error_message)
        return r.json()
