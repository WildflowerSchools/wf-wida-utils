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

    def fetch_student_data(self):
        logger.info('Fetching student data from Transparent Classroom for all schools')
        school_ids = self.fetch_school_ids()
        logger.info('Fetched {} school IDs'.format(len(school_ids)))
        student_data_school_dfs = list()
        for school_id in school_ids:
            student_data_school_df = self.fetch_student_data_school(school_id)
            student_data_school_dfs.append(student_data_school_df)
        student_data_df = pd.concat(student_data_school_dfs, ignore_index=True)
        return student_data_df

    def fetch_student_data_school(self, school_id):
        logger.info('Fetching student data from Transparent Classroom for school ID {}'.format(school_id))
        output_json = self.transparent_classroom_request('children.json', school_id=school_id)
        student_data_school_df = pd.DataFrame(output_json)
        student_data_school_df = student_data_school_df.reindex(columns= [
            'id',
            'first_name',
            'last_name',
            'birth_date',
            'gender',
            'hours_string',
            'dominant_language',
            'allergies',
            'ethnicity',
            'household_income',
            'approved_adults_string',
            'emergency_contacts_string',
            'classroom_ids',
            'parent_ids',
            'program',
            'middle_name',
            'grade',
            'last_day',
            'exit_reason',
            'student_id',
            'notes',
            'exit_survey_id',
            'exit_notes'
        ])
        student_data_school_df.rename(
            columns= {
                'id': 'student_id_tc',
                'first_name': 'student_first_name',
                'last_name': 'student_last_name',
                'birth_date': 'student_birth_date',
                'gender': 'student_gender',
                'hours_string': 'student_hours_string',
                'dominant_language': 'student_dominant_language',
                'allergies': 'student_allergies',
                'ethnicity': 'student_ethnicity',
                'household_income': 'student_household_income',
                'approved_adults_string': 'student_approved_adults_string',
                'emergency_contacts_string': 'student_emergency_contacts_string',
                'classroom_ids': 'student_classroom_ids',
                'parent_ids': 'student_parent_ids',
                'program': 'student_program',
                'middle_name': 'student_middle_name',
                'grade': 'student_grade',
                'last_day': 'student_last_day',
                'exit_reason': 'student_exit_reason',
                'student_id': 'student_id_tc_alt',
                'notes': 'student_notes',
                'exit_survey_id': 'student_exit_survey_id',
                'exit_notes': 'student_exit_notes'
            },
            inplace=True
        )
        student_data_school_df['student_id_tc'] = student_data_school_df['student_id_tc'].astype('Int64')
        student_data_school_df['student_first_name'] = student_data_school_df['student_first_name'].astype('string')
        student_data_school_df['student_last_name'] = student_data_school_df['student_last_name'].astype('string')
        student_data_school_df['student_birth_date'] = pd.DatetimeIndex(pd.to_datetime(student_data_school_df['student_birth_date'])).date
        student_data_school_df['student_gender'] = student_data_school_df['student_gender'].astype('string')
        student_data_school_df['student_hours_string'] = student_data_school_df['student_hours_string'].astype('string')
        student_data_school_df['student_dominant_language'] = student_data_school_df['student_dominant_language'].astype('string')
        student_data_school_df['student_allergies'] = student_data_school_df['student_allergies'].astype('string')
        student_data_school_df['student_household_income'] = student_data_school_df['student_household_income'].astype('string')
        student_data_school_df['student_approved_adults_string'] = student_data_school_df['student_approved_adults_string'].astype('string')
        student_data_school_df['student_emergency_contacts_string'] = student_data_school_df['student_emergency_contacts_string'].astype('string')
        student_data_school_df['student_program'] = student_data_school_df['student_program'].astype('string')
        student_data_school_df['student_middle_name'] = student_data_school_df['student_middle_name'].astype('string')
        student_data_school_df['student_grade'] = student_data_school_df['student_grade'].astype('string')
        student_data_school_df['student_last_day'] = pd.DatetimeIndex(pd.to_datetime(student_data_school_df['student_last_day'])).date
        student_data_school_df['student_exit_reason'] = student_data_school_df['student_exit_reason'].astype('string')
        student_data_school_df['student_id_tc_alt'] = student_data_school_df['student_id_tc_alt'].astype('string')
        student_data_school_df['student_notes'] = student_data_school_df['student_notes'].astype('string')
        student_data_school_df['student_exit_survey_id'] = student_data_school_df['student_exit_survey_id'].astype('Int64')
        student_data_school_df['student_exit_notes'] = student_data_school_df['student_exit_notes'].astype('string')
        student_data_school_df = student_data_school_df.reindex(columns=[
            'student_id_tc',
            'student_first_name',
            'student_middle_name',
            'student_last_name',
            'student_birth_date',
            'student_gender',
            'student_ethnicity',
            'student_dominant_language',
            'student_household_income',
            'student_grade',
            'student_classroom_ids',
            'student_program',
            'student_hours_string',
            'student_id_tc_alt',
            'student_allergies',
            'student_parent_ids',
            'student_approved_adults_string',
            'student_emergency_contacts_string',
            'student_notes',
            'student_last_day',
            'student_exit_reason',
            'student_exit_survey_id',
            'student_exit_notes'
        ])
        return student_data_school_df

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
