import wf_core_data.utils
import requests
import pandas as pd
from collections import OrderedDict
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
            json_output = self.transparent_classroom_request(
                'authenticate.json',
                auth=(self.username, self.password)
            )
            self.api_token = json_output['api_token']

    def fetch_data(self, pull_datetime=None):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching all data from Transparent Classroom for all schools and sessions')
        school_data = self.fetch_school_data(pull_datetime=pull_datetime)
        school_ids = [school.get('school_id_tc') for school in school_data]
        logger.info('Fetched {} school IDs'.format(len(school_ids)))
        data = {
            'schools': school_data,
            'classrooms': list(),
            'users': list(),
            'teachers_default_classrooms': list(),
            'teachers_accessible_classrooms': list(),
            'sessions': list(),
            'students': list(),
            'students_classrooms': list(),
            'students_parents': list()
        }
        for school_id in school_ids:
            data_school= self.fetch_data_school(
                school_id=school_id,
                pull_datetime=pull_datetime
            )
            data['classrooms'].extend(data_school['classrooms'])
            data['users'].extend(data_school['users'])
            data['teachers_default_classrooms'].extend(data_school['teachers_default_classrooms'])
            data['teachers_accessible_classrooms'].extend(data_school['teachers_accessible_classrooms'])
            data['sessions'].extend(data_school['sessions'])
            data['students'].extend(data_school['students'])
            data['students_classrooms'].extend(data_school['students_classrooms'])
            data['students_parents'].extend(data_school['students_parents'])
        return data

    def fetch_data_school(self, school_id, pull_datetime=None):
        school_id = int(school_id)
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching all data from Transparent Classroom for school ID {} for all sessions'.format(
            school_id
        ))
        session_data_school = self.fetch_session_data_school(
            school_id=school_id,
            pull_datetime=pull_datetime
        )
        classroom_data_school = self.fetch_classroom_data_school(
            school_id=school_id,
            pull_datetime=pull_datetime
        )
        user_data_school = self.fetch_user_data_school(
            school_id=school_id,
            pull_datetime=pull_datetime
        )
        student_data_school, student_parent_data_school = self.fetch_student_data_school(
            school_id=school_id,
            pull_datetime=pull_datetime
        )
        teacher_default_classroom_data = list()
        teacher_accessible_classroom_data=list()
        for user_datum in user_data_school:
            if 'teacher' in user_datum.get('user_roles_tc', []):
                teacher_default_classroom_data_teacher, teacher_accessible_classroom_data_teacher = self.fetch_teacher_data(
                    school_id=school_id,
                    user_id=user_datum['user_id_tc'],
                    pull_datetime=pull_datetime
                )
                teacher_default_classroom_data.extend(teacher_default_classroom_data_teacher)
                teacher_accessible_classroom_data.extend(teacher_accessible_classroom_data_teacher)
        session_ids = [session.get('session_id_tc') for session in session_data_school]
        logger.info('Fetched {} session IDs'.format(len(session_ids)))
        logger.info('Fetching student classroom association data from Transparent Classroom for school ID {} for each session'.format(
            school_id
        ))
        student_classroom_data = list()
        for session_id in session_ids:
            student_classroom_session_data = self.fetch_student_classroom_session_data(
                school_id=school_id,
                session_id=session_id,
                pull_datetime=pull_datetime
            )
            student_classroom_data.extend(student_classroom_session_data)
        data_school = {
            'classrooms': classroom_data_school,
            'users': user_data_school,
            'teachers_default_classrooms': teacher_default_classroom_data,
            'teachers_accessible_classrooms': teacher_accessible_classroom_data,
            'sessions': session_data_school,
            'students': student_data_school,
            'students_classrooms': student_classroom_data,
            'students_parents': student_parent_data_school
        }
        return data_school

    def fetch_school_ids(self):
        school_data = self.fetch_school_data()
        school_ids = [school.get('school_id_tc') for school in school_data]
        return school_ids

    def fetch_school_data(self, pull_datetime=None):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching school data from Transparent Classroom for all schools')
        json_output = self.transparent_classroom_request('schools.json')
        if not isinstance(json_output, list):
            raise ValueError('Received unexpected response from Transparent Classroom: {}'.format(
                json_output
            ))
        school_data=list()
        for datum in json_output:
            if datum.get('type') == 'School':
                school_datum = OrderedDict([
                    ('school_id_tc', datum.get('id')),
                    ('pull_datetime', pull_datetime),
                    ('school_name_tc', datum.get('name')),
                    ('school_address_tc', datum.get('address')),
                    ('school_phone_tc', datum.get('phone')),
                    ('school_time_zone_tc', datum.get('time_zone'))
                ])
                school_data.append(school_datum)
        return school_data

    def fetch_classroom_data_school(self, school_id, pull_datetime=None):
        school_id = int(school_id)
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching classroom data from Transparent Classroom for school ID {}'.format(school_id))
        json_output = self.transparent_classroom_request(
            'classrooms.json',
                params={
                    'show_inactive': True
                },
            school_id=school_id
        )
        if not isinstance(json_output, list):
            raise ValueError('Received unexpected response from Transparent Classroom: {}'.format(
                json_output
            ))
        classroom_data_school=list()
        for datum in json_output:
            classroom_datum = OrderedDict([
                ('school_id_tc', school_id),
                ('classroom_id_tc', datum.get('id')),
                ('pull_datetime', pull_datetime),
                ('classroom_name_tc', datum.get('name')),
                ('classroom_lesson_set_id_tc', datum.get('lesson_set_id')),
                ('classroom_level_tc', datum.get('level')),
                ('classroom_active_tc', wf_core_data.utils.to_boolean(datum.get('active')))
            ])
            classroom_data_school.append(classroom_datum)
        return classroom_data_school

    def fetch_session_ids(self, school_id):
        session_data_school = self.fetch_session_data_school(
            school_id=school_id,
            pull_datetime=None
        )
        session_ids = [session.get('session_id_tc') for session in session_data_school]
        return session_ids

    def fetch_session_data_school(self, school_id, pull_datetime=None):
        school_id = int(school_id)
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching session data from Transparent Classroom for school ID {}'.format(school_id))
        json_output = self.transparent_classroom_request('sessions.json', school_id=school_id)
        if not isinstance(json_output, list):
            raise ValueError('Received unexpected response from Transparent Classroom: {}'.format(
                json_output
            ))
        session_data_school=list()
        for datum in json_output:
            session_datum = OrderedDict([
                ('school_id_tc', school_id),
                ('session_id_tc', datum.get('id')),
                ('pull_datetime', pull_datetime),
                ('session_name_tc', datum.get('name')),
                ('session_start_date_tc', wf_core_data.utils.to_date(datum.get('start_date'))),
                ('session_stop_date_tc', wf_core_data.utils.to_date(datum.get('stop_date'))),
                ('session_current_tc', wf_core_data.utils.to_boolean(datum.get('current'))),
                ('session_inactive_tc', wf_core_data.utils.to_boolean(datum.get('inactive'))),
                ('session_num_children_tc', int(datum.get('children')))
            ])
            session_data_school.append(session_datum)
        return session_data_school

    def fetch_user_data_school(self, school_id, pull_datetime=None):
        school_id = int(school_id)
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching user data from Transparent Classroom for school ID {}'.format(
            school_id
        ))
        json_output = self.transparent_classroom_request(
            'users.json',
            school_id=school_id
        )
        if not isinstance(json_output, list):
            raise ValueError('Received unexpected response from Transparent Classroom: {}'.format(
                json_output
            ))
        user_data_school = list()
        for datum in json_output:
            user_datum = OrderedDict([
                ('school_id_tc', school_id),
                ('user_id_tc', datum.get('id')),
                ('pull_datetime', pull_datetime),
                ('user_first_name_tc', datum.get('first_name')),
                ('user_last_name_tc', datum.get('last_name')),
                ('user_email_tc', datum.get('email')),
                ('user_roles_tc', datum.get('roles'))
            ])
            user_data_school.append(user_datum)
        return user_data_school

    def fetch_teacher_data(self, school_id, user_id, pull_datetime=None):
        school_id = int(school_id)
        user_id = int(user_id)
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching teacher data from Transparent Classroom for school ID {} and user id {}'.format(
            school_id,
            user_id
        ))
        teacher_datum = self.transparent_classroom_request(
            'users/{}.json'.format(user_id),
            school_id=school_id
        )
        if not isinstance(teacher_datum, dict):
            raise ValueError('Received unexpected response from Transparent Classroom: {}'.format(
                teacher_datum
            ))
        teacher_default_classroom_data = list()
        teacher_accessible_classroom_data = list()
        if teacher_datum.get('default_classroom_id') is not None:
            teacher_default_classroom_datum = OrderedDict([
                ('school_id_tc', school_id),
                ('user_id_tc', teacher_datum.get('id')),
                ('pull_datetime', pull_datetime),
                ('teacher_default_classroom_id_tc', teacher_datum.get('default_classroom_id'))
            ])
            teacher_default_classroom_data.append(teacher_default_classroom_datum)
        for accessible_classroom_id in teacher_datum.get('accessible_classroom_ids', []):
            teacher_accessible_classroom_datum = OrderedDict([
                ('school_id_tc', school_id),
                ('user_id_tc', teacher_datum.get('id')),
                ('pull_datetime', pull_datetime),
                ('teacher_accessible_classroom_id_tc', accessible_classroom_id)
            ])
            teacher_accessible_classroom_data.append(teacher_accessible_classroom_datum)
        return teacher_default_classroom_data, teacher_accessible_classroom_data

    def fetch_student_data_school(self, school_id, pull_datetime=None):
        school_id = int(school_id)
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching student data from Transparent Classroom for school ID {} for all sessions'.format(
            school_id
        ))
        json_output = self.transparent_classroom_request(
            'children.json',
            params={
                'session_id': 'all'
            },
            school_id=school_id
        )
        if not isinstance(json_output, list):
            raise ValueError('Received unexpected response from Transparent Classroom: {}'.format(
                json_output
            ))
        student_data_school = list()
        student_parent_data_school = list()
        for datum in json_output:
            student_datum = OrderedDict([
                ('school_id_tc', school_id),
                ('student_id_tc', datum.get('id')),
                ('pull_datetime', pull_datetime),
                ('student_first_name_tc', datum.get('first_name')),
                ('student_middle_name_tc', datum.get('middle_name')),
                ('student_last_name_tc', datum.get('last_name')),
                ('student_birth_date_tc', wf_core_data.utils.to_date(datum.get('birth_date'))),
                ('student_gender_tc', datum.get('gender')),
                ('student_hours_string_tc', datum.get('hours_string')),
                ('student_dominant_language_tc', datum.get('dominant_language')),
                ('student_allergies_tc', datum.get('allergies')),
                ('student_ethnicity_tc', datum.get('ethnicity')),
                ('student_household_income_tc', datum.get('household_income')),
                ('student_approved_adults_string_tc', datum.get('approved_adults_string')),
                ('student_emergency_contacts_string_tc', datum.get('emergency_contacts_string')),
                ('student_program_tc', datum.get('program')),
                ('student_grade_tc', datum.get('grade')),
                ('student_last_day_tc', wf_core_data.utils.to_date(datum.get('last_day'))),
                ('student_exit_reason_tc', datum.get('exit_reason')),
                ('student_id_alt_tc', datum.get('student_id')),
                ('student_notes_tc', datum.get('notes')),
                ('student_exit_survey_id_tc', datum.get('exit_survey_id')),
                ('student_exit_notes_tc', datum.get('exit_notes'))
            ])
            student_data_school.append(student_datum)
            for parent_id in datum.get('parent_ids', []):
                student_parent_datum = OrderedDict([
                    ('school_id_tc', school_id),
                    ('student_id_tc', datum.get('id')),
                    ('pull_datetime', pull_datetime),
                    ('parent_id_tc', parent_id)
                ])
                student_parent_data_school.append(student_parent_datum)
        return student_data_school, student_parent_data_school

    def fetch_student_classroom_session_data(self, school_id, session_id, pull_datetime=None):
        school_id = int(school_id)
        session_id = int(session_id)
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching student classroom association data from Transparent Classroom for school ID {} and session ID {}'.format(
            school_id,
            session_id
        ))
        json_output = self.transparent_classroom_request(
            'children.json',
            params={
                'session_id': session_id
            },
            school_id=school_id
        )
        if not isinstance(json_output, list):
            raise ValueError('Received unexpected response from Transparent Classroom: {}'.format(
                json_output
            ))
        student_classroom_session_data = list()
        for datum in json_output:
            for classroom_id in datum.get('classroom_ids', []):
                student_classroom_datum = OrderedDict([
                    ('school_id_tc', school_id),
                    ('student_id_tc', datum.get('id')),
                    ('pull_datetime', pull_datetime),
                    ('session_id_tc', session_id),
                    ('classroom_id_tc', classroom_id)
                ])
                student_classroom_session_data.append(student_classroom_datum)
        return student_classroom_session_data

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


def convert_school_data_to_df(school_data):
    school_data_df = pd.DataFrame(
        school_data,
        dtype='object'
    )
    school_data_df['pull_datetime'] = pd.to_datetime(school_data_df['pull_datetime'])
    school_data_df = school_data_df.astype({
        'school_id_tc': 'int',
        'school_name_tc': 'string',
        'school_address_tc': 'string',
        'school_phone_tc': 'string',
        'school_time_zone_tc': 'string'
    })
    return school_data_df

def convert_classroom_data_to_df(classroom_data):
    classroom_data_df = pd.DataFrame(
        classroom_data,
        dtype='object'
    )
    classroom_data_df['pull_datetime'] = pd.to_datetime(classroom_data_df['pull_datetime'])
    classroom_data_df = classroom_data_df.astype({
        'school_id_tc': 'int',
        'classroom_id_tc': 'int',
        'classroom_name_tc': 'string',
        'classroom_lesson_set_id_tc': 'Int64',
        'classroom_level_tc': 'string',
        'classroom_active_tc': 'bool'
    })
    return classroom_data_df

def convert_user_data_to_df(user_data):
    user_data_df = pd.DataFrame(
        user_data,
        dtype='object'
    )
    user_data_df['pull_datetime'] = pd.to_datetime(user_data_df['pull_datetime'])
    user_data_df = user_data_df.astype({
        'school_id_tc': 'int',
        'user_id_tc': 'int',
        'user_first_name_tc': 'string',
        'user_last_name_tc': 'string',
        'user_email_tc': 'string',
    })
    return user_data_df

def convert_teacher_default_classroom_data_to_df(teacher_default_classroom_data):
    teacher_default_classroom_data_df = pd.DataFrame(
        teacher_default_classroom_data,
        dtype='object'
    )
    teacher_default_classroom_data_df['pull_datetime'] = pd.to_datetime(teacher_default_classroom_data_df['pull_datetime'])
    teacher_default_classroom_data_df = teacher_default_classroom_data_df.astype({
        'school_id_tc': 'int',
        'user_id_tc': 'int',
        'teacher_default_classroom_id_tc': 'int'
    })
    return teacher_default_classroom_data_df

def convert_teacher_accessible_classroom_data_to_df(teacher_accessible_classroom_data):
    teacher_accessible_classroom_data_df = pd.DataFrame(
        teacher_accessible_classroom_data,
        dtype='object'
    )
    teacher_accessible_classroom_data_df['pull_datetime'] = pd.to_datetime(teacher_accessible_classroom_data_df['pull_datetime'])
    teacher_accessible_classroom_data_df = teacher_accessible_classroom_data_df.astype({
        'school_id_tc': 'int',
        'user_id_tc': 'int',
        'teacher_accessible_classroom_id_tc': 'int'
    })
    return teacher_accessible_classroom_data_df

def convert_session_data_to_df(session_data):
    session_data_df = pd.DataFrame(
        session_data,
        dtype='object'
    )
    session_data_df['pull_datetime'] = pd.to_datetime(session_data_df['pull_datetime'])
    session_data_df = session_data_df.astype({
        'school_id_tc': 'int',
        'session_id_tc': 'int',
        'session_name_tc': 'string',
        'session_current_tc': 'bool',
        'session_inactive_tc': 'bool',
        'session_num_children_tc': 'Int64'
    })
    return session_data_df

def convert_student_data_to_df(student_data):
    student_data_df = pd.DataFrame(
        student_data,
        dtype='object'
    )
    student_data_df['pull_datetime'] = pd.to_datetime(student_data_df['pull_datetime'])
    student_data_df = student_data_df.astype({
            'school_id_tc': 'int',
            'student_id_tc': 'int',
            'student_first_name_tc': 'string',
            'student_middle_name_tc': 'string',
            'student_last_name_tc': 'string',
            'student_birth_date_tc': 'object',
            'student_gender_tc': 'string',
            'student_ethnicity_tc': 'object',
            'student_dominant_language_tc': 'string',
            'student_household_income_tc': 'string',
            'student_grade_tc': 'string',
            'student_program_tc': 'string',
            'student_hours_string_tc': 'string',
            'student_id_alt_tc': 'string',
            'student_allergies_tc': 'string',
            'student_approved_adults_string_tc': 'string',
            'student_emergency_contacts_string_tc': 'string',
            'student_notes_tc': 'string',
            'student_last_day_tc': 'object',
            'student_exit_reason_tc': 'string',
            'student_exit_survey_id_tc': 'Int64',
            'student_exit_notes_tc': 'string'
    })
    return student_data_df

def convert_student_classroom_data_to_df(student_classroom_data):
    student_classroom_data_df = pd.DataFrame(
        student_classroom_data,
        dtype='object'
    )
    student_classroom_data_df['pull_datetime'] = pd.to_datetime(student_classroom_data_df['pull_datetime'])
    student_classroom_data_df = student_classroom_data_df.astype({
            'school_id_tc': 'int',
            'student_id_tc': 'int',
            'session_id_tc': 'int',
            'classroom_id_tc': 'int'
    })
    return student_classroom_data_df

def convert_student_parent_data_to_df(student_parent_data):
    student_parent_data_df = pd.DataFrame(
        student_parent_data,
        dtype='object'
    )
    student_parent_data_df['pull_datetime'] = pd.to_datetime(student_parent_data_df['pull_datetime'])
    student_parent_data_df = student_parent_data_df.astype({
            'school_id_tc': 'int',
            'student_id_tc': 'int',
            'parent_id_tc': 'int'
    })
    return student_parent_data_df
