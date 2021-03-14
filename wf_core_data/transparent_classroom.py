from wf_core_data.utils import to_date
import requests
import pandas as pd
import json
import datetime
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

    def fetch_student_data(self, pull_datetime=None):
        if pull_datetime is None:
            pull_datetime = pd.Timestamp.utcnow()
        logger.info('Fetching student data from Transparent Classroom for all schools')
        school_ids = self.fetch_school_ids()
        logger.info('Fetched {} school IDs'.format(len(school_ids)))
        student_data_school_dfs = list()
        for school_id in school_ids:
            student_data_school_df = self.fetch_student_data_school(school_id, pull_datetime=pull_datetime)
            student_data_school_dfs.append(student_data_school_df)
        student_data_df = pd.concat(student_data_school_dfs, ignore_index=True)
        return student_data_df

    def fetch_student_data_school(self, school_id, pull_datetime=None):
        if pull_datetime is None:
            pull_datetime = pd.Timestamp.utcnow()
        logger.info('Fetching student data from Transparent Classroom for school ID {}'.format(school_id))
        output_json = self.transparent_classroom_request('children.json', school_id=school_id)
        student_data_school_df = pd.DataFrame(output_json, dtype='object')
        student_data_school_df['school_id_tc'] = int(school_id)
        student_data_school_df['pull_datetime'] = pull_datetime
        student_data_school_df = student_data_school_df.reindex(
            columns= [
                'school_id_tc',
                'id',
                'pull_datetime',
                'first_name',
                'middle_name',
                'last_name',
                'birth_date',
                'gender',
                'ethnicity',
                'dominant_language',
                'household_income',
                'grade',
                'classroom_ids',
                'program',
                'hours_string',
                'student_id',
                'allergies',
                'parent_ids',
                'approved_adults_string',
                'emergency_contacts_string',
                'notes',
                'last_day',
                'exit_reason',
                'exit_survey_id',
                'exit_notes'
            ],
            fill_value=None
        )
        student_data_school_df.rename(
            columns= {
                'id': 'student_id_tc',
                'first_name': 'student_first_name_tc',
                'middle_name': 'student_middle_name_tc',
                'last_name': 'student_last_name_tc',
                'birth_date': 'student_birth_date_tc',
                'gender': 'student_gender_tc',
                'hours_string': 'student_hours_string_tc',
                'dominant_language': 'student_dominant_language_tc',
                'allergies': 'student_allergies_tc',
                'ethnicity': 'student_ethnicity_tc',
                'household_income': 'student_household_income_tc',
                'approved_adults_string': 'student_approved_adults_string_tc',
                'emergency_contacts_string': 'student_emergency_contacts_string_tc',
                'classroom_ids': 'student_classroom_ids_tc',
                'parent_ids': 'student_parent_ids_tc',
                'program': 'student_program_tc',
                'middle_name': 'student_middle_name_tc',
                'grade': 'student_grade_tc',
                'last_day': 'student_last_day_tc',
                'exit_reason': 'student_exit_reason_tc',
                'student_id': 'student_id_alt_tc',
                'notes': 'student_notes_tc',
                'exit_survey_id': 'student_exit_survey_id_tc',
                'exit_notes': 'student_exit_notes_tc'
            },
            inplace=True
        )
        student_data_school_df['student_birth_date_tc'] = student_data_school_df['student_birth_date_tc'].apply(to_date)
        student_data_school_df['student_last_day_tc'] = student_data_school_df['student_last_day_tc'].apply(to_date)
        student_data_school_df = student_data_school_df.astype({
            'school_id_tc': 'Int64',
            'student_id_tc': 'Int64',
            'student_first_name_tc': 'string',
            'student_middle_name_tc': 'string',
            'student_last_name_tc': 'string',
            'student_birth_date_tc': 'object',
            'student_gender_tc': 'string',
            'student_ethnicity_tc': 'object',
            'student_dominant_language_tc': 'string',
            'student_household_income_tc': 'string',
            'student_grade_tc': 'string',
            'student_classroom_ids_tc': 'object',
            'student_program_tc': 'string',
            'student_hours_string_tc': 'string',
            'student_id_alt_tc': 'string',
            'student_allergies_tc': 'string',
            'student_parent_ids_tc': 'object',
            'student_approved_adults_string_tc': 'string',
            'student_emergency_contacts_string_tc': 'string',
            'student_notes_tc': 'string',
            'student_last_day_tc': 'object',
            'student_exit_reason_tc': 'string',
            'student_exit_survey_id_tc': 'Int64',
            'student_exit_notes_tc': 'string'
        })
        student_data_school_df['student_ethnicity_tc'] = student_data_school_df['student_ethnicity_tc'].where(
            pd.notnull(student_data_school_df['student_ethnicity_tc']),
            None
        )
        student_data_school_df['student_classroom_ids_tc'] = student_data_school_df['student_classroom_ids_tc'].where(
            pd.notnull(student_data_school_df['student_classroom_ids_tc']),
            None
        )
        student_data_school_df['student_parent_ids_tc'] = student_data_school_df['student_parent_ids_tc'].where(
            pd.notnull(student_data_school_df['student_parent_ids_tc']),
            None
        )
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
                'id': 'school_id_tc',
                'name': 'school_name_tc',
                'address': 'school_address_tc',
                'phone': 'school_phone_tc',
                'time_zone': 'school_time_zone_tc'
            },
            inplace=True
        )
        school_data_df['school_id_tc'] = school_data_df['school_id_tc'].astype('Int64')
        school_data_df['school_name_tc'] = school_data_df['school_name_tc'].astype('string')
        school_data_df['school_address_tc'] = school_data_df['school_address_tc'].astype('string')
        school_data_df['school_phone_tc'] = school_data_df['school_phone_tc'].astype('string')
        school_data_df['school_time_zone_tc'] = school_data_df['school_time_zone_tc'].astype('string')
        school_data_df = school_data_df.reindex(columns=[
                'school_id_tc',
                'school_name_tc',
                'school_address_tc',
                'school_phone_tc',
                'school_time_zone_tc'
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
