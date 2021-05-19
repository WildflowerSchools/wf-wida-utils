import wf_core_data.utils
import requests
import pandas as pd
from collections import OrderedDict
import pickle
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

    def fetch_and_write_data_local(
        self,
        base_directory,
        pull_datetime=None,
        only_current=False,
        format='dataframe',
        output_directory_stem='transparent_classroom_snapshot',
        all_data_list_filename_stem ='data_tc_list_dict',
        school_data_filename_stem='school_data_tc',
        classroom_data_filename_stem='classroom_data_tc',
        user_data_filename_stem='user_data_tc',
        teacher_default_classroom_data_filename_stem='teacher_default_classroom_data_tc',
        teacher_accessible_classroom_data_filename_stem='teacher_accessible_classroom_data_tc',
        session_data_filename_stem='session_data_tc',
        student_data_filename_stem='student_data_tc',
        student_classroom_data_filename_stem='student_classroom_data_tc',
        student_parent_data_filename_stem='student_parent_data_tc'
    ):
        pull_datetime=wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        data = self.fetch_data(
            pull_datetime=pull_datetime,
            only_current=only_current,
            format=format
        )
        write_data_local(
            data=data,
            base_directory=base_directory,
            only_current=only_current,
            format=format,
            output_directory_stem=output_directory_stem,
            all_data_list_filename_stem=all_data_list_filename_stem,
            school_data_filename_stem=school_data_filename_stem,
            classroom_data_filename_stem=classroom_data_filename_stem,
            user_data_filename_stem=user_data_filename_stem,
            teacher_default_classroom_data_filename_stem=teacher_default_classroom_data_filename_stem,
            teacher_accessible_classroom_data_filename_stem=teacher_accessible_classroom_data_filename_stem,
            session_data_filename_stem=session_data_filename_stem,
            student_data_filename_stem=student_data_filename_stem,
            student_classroom_data_filename_stem=student_classroom_data_filename_stem,
            student_parent_data_filename_stem=student_parent_data_filename_stem
        )

    def fetch_data(
        self,
        pull_datetime=None,
        only_current=False,
        format='dataframe'
    ):
        pull_datetime=wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching all data from Transparent Classroom for all schools and sessions')
        school_data = self.fetch_school_data(
            pull_datetime=pull_datetime,
            format='list'
        )
        school_ids = [school.get('school_id_tc') for school in school_data]
        logger.info('Fetched {} school IDs'.format(len(school_ids)))
        data = {
            'pull_datetime': pull_datetime,
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
                pull_datetime=pull_datetime,
                only_current=only_current,
                format='list'
            )
            data['classrooms'].extend(data_school['classrooms'])
            data['users'].extend(data_school['users'])
            data['teachers_default_classrooms'].extend(data_school['teachers_default_classrooms'])
            data['teachers_accessible_classrooms'].extend(data_school['teachers_accessible_classrooms'])
            data['sessions'].extend(data_school['sessions'])
            data['students'].extend(data_school['students'])
            data['students_classrooms'].extend(data_school['students_classrooms'])
            data['students_parents'].extend(data_school['students_parents'])
        if format == 'dataframe':
            data['schools'] = convert_school_data_to_df(data['schools'])
            data['classrooms'] = convert_classroom_data_to_df(data['classrooms'])
            data['users'] = convert_user_data_to_df(data['users'])
            data['teachers_default_classrooms'] = convert_teacher_default_classroom_data_to_df(data['teachers_default_classrooms'])
            data['teachers_accessible_classrooms'] = convert_teacher_accessible_classroom_data_to_df(data['teachers_accessible_classrooms'])
            data['sessions'] = convert_session_data_to_df(data['sessions'])
            data['students'] = convert_student_data_to_df(data['students'])
            data['students_classrooms'] = convert_student_classroom_data_to_df(data['students_classrooms'])
            data['students_parents'] = convert_student_parent_data_to_df(data['students_parents'])
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return data

    def fetch_data_school(
        self,
        school_id,
        pull_datetime=None,
        only_current=False,
        format='dataframe'
    ):
        school_id = int(school_id)
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        if only_current:
            logger.info('Fetching only current data from Transparent Classroom for school ID {} for current session'.format(
                school_id
            ))
        else:
            logger.info('Fetching all data from Transparent Classroom for school ID {} for all sessions'.format(
                school_id
            ))
        session_data_school = self.fetch_session_data_school(
            school_id=school_id,
            pull_datetime=pull_datetime,
            format='list'
        )
        classroom_data_school = self.fetch_classroom_data_school(
            school_id=school_id,
            pull_datetime=pull_datetime,
            format='list'
        )
        user_data_school = self.fetch_user_data_school(
            school_id=school_id,
            pull_datetime=pull_datetime,
            format='list'
        )
        student_data_school, student_parent_data_school = self.fetch_student_data_school(
            school_id=school_id,
            pull_datetime=pull_datetime,
            only_current=only_current,
            format='list'
        )
        teacher_default_classroom_data = list()
        teacher_accessible_classroom_data=list()
        for user_datum in user_data_school:
            if 'teacher' in user_datum.get('user_roles_tc', []):
                teacher_default_classroom_data_teacher, teacher_accessible_classroom_data_teacher = self.fetch_teacher_classroom_data_teacher(
                    school_id=school_id,
                    user_id=user_datum['user_id_tc'],
                    pull_datetime=pull_datetime,
                    format='list'
                )
                teacher_default_classroom_data.extend(teacher_default_classroom_data_teacher)
                teacher_accessible_classroom_data.extend(teacher_accessible_classroom_data_teacher)
        if only_current:
            logger.info('Fetching student classroom association data from Transparent Classroom for school ID {} for current session'.format(
                school_id
            ))
        else:
            logger.info('Fetching student classroom association data from Transparent Classroom for school ID {} for each session'.format(
                school_id
            ))
        student_classroom_data = list()
        for session in session_data_school:
            if only_current and not session.get('session_current_tc'):
                continue
            student_classroom_data_session = self.fetch_student_classroom_data_session(
                school_id=school_id,
                session_id=session.get('session_id_tc'),
                pull_datetime=pull_datetime,
                format='list'
            )
            student_classroom_data.extend(student_classroom_data_session)
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
        if format == 'dataframe':
            data_school['classrooms'] = convert_classroom_data_to_df(data_school['classrooms'])
            data_school['users'] = convert_user_data_to_df(data_school['users'])
            data_school['teachers_default_classrooms'] = convert_teacher_default_classroom_data_to_df(data_school['teachers_default_classrooms'])
            data_school['teachers_accessible_classrooms'] = convert_teacher_accessible_classroom_data_to_df(data_school['teachers_accessible_classrooms'])
            data_school['sessions'] = convert_session_data_to_df(data_school['sessions'])
            data_school['students'] = convert_student_data_to_df(data_school['students'])
            data_school['students_classrooms'] = convert_student_classroom_data_to_df(data_school['students_classrooms'])
            data_school['students_parents'] = convert_student_parent_data_to_df(data_school['students_parents'])
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return data_school

    def fetch_student_data(
        self,
        school_ids=None,
        pull_datetime=None,
        only_current=False,
        format='dataframe'
    ):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        if school_ids is None:
            school_ids=self.fetch_school_ids()
        logger.info('Fetching student data from Transparent Classroom for {} schools'.format(len(school_ids)))
        student_data = list()
        student_parent_data = list()
        for school_id in school_ids:
            student_data_school, student_parent_data_school = self.fetch_student_data_school(
                school_id=school_id,
                pull_datetime=pull_datetime,
                only_current=only_current,
                format='list'
            )
            student_data.extend(student_data_school)
            student_parent_data.extend(student_parent_data_school)
        if format == 'dataframe':
            student_data = convert_student_data_to_df(student_data)
            student_parent_data = convert_student_parent_data_to_df(student_parent_data)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return student_data, student_parent_data

    def fetch_student_data_school(
        self,
        school_id,
        pull_datetime=None,
        only_current=False,
        format='dataframe'
    ):
        school_id = int(school_id)
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching student data from Transparent Classroom for school ID {} for all sessions'.format(
            school_id
        ))
        if only_current:
            params={
                'only_current': 'true'
            }
        else:
            params={
                'session_id': 'all'
            }
        json_output = self.transparent_classroom_request(
            'children.json',
            params=params,
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
        if format == 'dataframe':
            student_data_school = convert_student_data_to_df(student_data_school)
            student_parent_data_school = convert_student_parent_data_to_df(student_parent_data_school)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return student_data_school, student_parent_data_school

    def fetch_student_classroom_data(
        self,
        school_ids=None,
        session_data=None,
        pull_datetime=None,
        only_current=False,
        format='dataframe'
    ):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        if school_ids is not None and session_data is not None:
            raise ValueError('Cannot specify both session data and school IDs')
        student_classroom_data = list()
        if session_data is not None:
            if not isinstance(session_data, pd.DataFrame):
                session_data = convert_session_data_to_df(session_data)
            logger.info('Fetching student classroom association data from Transparent Classroom for {} schools'.format(
                len(session_data.index.get_level_values('school_id_tc').unique())
            ))
            for school_id, group_df in session_data.groupby('school_id_tc'):
                # print(group_df)
                session_data_list = group_df.reset_index().to_dict(orient='records')
                # print(session_data_list)
                student_classroom_data_school = self.fetch_student_classroom_data_school(
                    school_id=school_id,
                    session_data_list=session_data_list,
                    pull_datetime=pull_datetime,
                    only_current=only_current,
                    format='list'
                )
                student_classroom_data.extend(student_classroom_data_school)
        else:
            if school_ids is None:
                school_ids=self.fetch_school_ids()
            logger.info('Fetching student classroom association data from Transparent Classroom for {} schools'.format(
                len(school_ids)
            ))
            for school_id in school_ids:
                student_classroom_data_school = self.fetch_student_classroom_data_school(
                    school_id=school_id,
                    session_data_list=None,
                    pull_datetime=pull_datetime,
                    only_current=only_current,
                    format='list'
                )
                student_classroom_data.extend(student_classroom_data_school)
        if format == 'dataframe':
            student_classroom_data = convert_student_classroom_data_to_df(student_classroom_data)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return student_classroom_data

    def fetch_student_classroom_data_school(
        self,
        school_id,
        session_data_list=None,
        pull_datetime=None,
        only_current=False,
        format='dataframe'
    ):
        if school_id is not None:
            school_id = int(school_id)
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        if session_data_list is None:
            session_data_list = self.fetch_session_data_school(
                school_id=school_id,
                pull_datetime=pull_datetime,
                format='list'
            )
        logger.info('Fetching student classroom association data from Transparent Classroom for school ID {}'.format(
            school_id,
        ))
        student_classroom_data_school = list()
        for session in session_data_list:
            if only_current and not session.get('session_current_tc'):
                continue
            student_classroom_data_session = self.fetch_student_classroom_data_session(
                school_id=school_id,
                session_id=session.get('session_id_tc'),
                pull_datetime=pull_datetime,
                format='list'
            )
            student_classroom_data_school.extend(student_classroom_data_session)
        if format == 'dataframe':
            student_classroom_data_school = convert_student_classroom_data_to_df(student_classroom_data_school)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return student_classroom_data_school

    def fetch_student_classroom_data_session(
        self,
        school_id,
        session_id,
        pull_datetime=None,
        format='dataframe'
    ):
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
        student_classroom_data_session = list()
        for datum in json_output:
            for classroom_id in datum.get('classroom_ids', []):
                student_classroom_datum = OrderedDict([
                    ('school_id_tc', school_id),
                    ('student_id_tc', datum.get('id')),
                    ('pull_datetime', pull_datetime),
                    ('session_id_tc', session_id),
                    ('classroom_id_tc', classroom_id)
                ])
                student_classroom_data_session.append(student_classroom_datum)
        if format == 'dataframe':
            student_classroom_data_session = convert_student_classroom_data_to_df(student_classroom_data_session)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return student_classroom_data_session

    def fetch_session_ids_school(
        self,
        school_id
    ):
        session_data_school = self.fetch_session_data_school(
            school_id=school_id,
            pull_datetime=None,
            format='list'
        )
        session_ids_school = [session.get('session_id_tc') for session in session_data_school]
        return session_ids_school

    def fetch_session_data(
        self,
        school_ids=None,
        pull_datetime=None,
        format='dataframe'
    ):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        if school_ids is None:
            school_ids=self.fetch_school_ids()
        logger.info('Fetching session data from Transparent Classroom for {} schools'.format(len(school_ids)))
        session_data = list()
        for school_id in school_ids:
            session_data_school = self.fetch_session_data_school(
                school_id=school_id,
                pull_datetime=pull_datetime,
                format='list'
            )
            session_data.extend(session_data_school)
        if format == 'dataframe':
            session_data = convert_session_data_to_df(session_data)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return session_data

    def fetch_session_data_school(
        self,
        school_id,
        pull_datetime=None,
        format='dataframe'
    ):
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
        if format == 'dataframe':
            session_data_school = convert_session_data_to_df(session_data_school)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return session_data_school

    def fetch_teacher_classroom_data(
        self,
        school_ids=None,
        user_data=None,
        pull_datetime=None,
        format='dataframe'
    ):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        if school_ids is not None and user_data is not None:
            raise ValueError('Cannot specify both school IDs and user data')
        teacher_default_classroom_data = list()
        teacher_accessible_classroom_data = list()
        if user_data is not None:
            if not isinstance(user_data, pd.DataFrame):
                user_data = convert_user_data_to_df(user_data)
            teacher_data = user_data.loc[user_data['user_roles_tc'].apply(lambda x: 'teacher' in x)]
            logger.info('Fetching teacher classroom association data from Transparent Classroom for {} schools'.format(
                len(teacher_data.index.get_level_values('school_id_tc').unique())
            ))
            for school_id, group_df in teacher_data.groupby('school_id_tc'):
                user_ids = group_df.index.get_level_values('user_id_tc').unique().tolist()
                teacher_default_classroom_data_school, teacher_accessible_classroom_data_school = self.fetch_teacher_classroom_data_school(
                    school_id=school_id,
                    user_ids=user_ids,
                    pull_datetime=pull_datetime,
                    format='list'
                )
                teacher_default_classroom_data.extend(teacher_default_classroom_data_school)
                teacher_accessible_classroom_data.extend(teacher_accessible_classroom_data_school)
        else:
            if school_ids is None:
                school_ids = self.fetch_school_ids()
            logger.info('Fetching teacher classroom association data from Transparent Classroom for {} schools'.format(
                len(school_ids)
            ))
            for school_id in school_ids:
                teacher_default_classroom_data_school, teacher_accessible_classroom_data_school = self.fetch_teacher_classroom_data_school(
                    school_id=school_id,
                    user_ids=None,
                    pull_datetime=pull_datetime,
                    format='list'
                )
                teacher_default_classroom_data.extend(teacher_default_classroom_data_school)
                teacher_accessible_classroom_data.extend(teacher_accessible_classroom_data_school)
        if format == 'dataframe':
            teacher_default_classroom_data = convert_teacher_default_classroom_data_to_df(teacher_default_classroom_data)
            teacher_accessible_classroom_data = convert_teacher_accessible_classroom_data_to_df(teacher_accessible_classroom_data)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return teacher_default_classroom_data, teacher_accessible_classroom_data

    def fetch_teacher_classroom_data_school(
        self,
        school_id,
        user_ids=None,
        pull_datetime=None,
        format='dataframe'
    ):
        school_id = int(school_id)
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        if user_ids is None:
            user_ids = self.fetch_teacher_user_ids_school(school_id=school_id)
        logger.info('Fetching teacher classroom association data from Transparent Classroom for school ID {} for {} teachers'.format(
            school_id,
            len(user_ids)
        ))
        teacher_default_classroom_data_school = list()
        teacher_accessible_classroom_data_school = list()
        for user_id in user_ids:
            teacher_default_classroom_data_teacher, teacher_accessible_classroom_data_teacher = self.fetch_teacher_classroom_data_teacher(
                school_id=school_id,
                user_id=user_id,
                pull_datetime=pull_datetime,
                format='list'
            )
            teacher_default_classroom_data_school.extend(teacher_default_classroom_data_teacher)
            teacher_accessible_classroom_data_school.extend(teacher_accessible_classroom_data_teacher)
        if format == 'dataframe':
            teacher_default_classroom_data_school = convert_teacher_default_classroom_data_to_df(teacher_default_classroom_data_school)
            teacher_accessible_classroom_data_school = convert_teacher_accessible_classroom_data_to_df(teacher_accessible_classroom_data_school)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return teacher_default_classroom_data_school, teacher_accessible_classroom_data_school

    def fetch_teacher_classroom_data_teacher(
        self,
        school_id,
        user_id,
        pull_datetime=None,
        format='dataframe'
    ):
        school_id = int(school_id)
        user_id = int(user_id)
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching teacher classroom association data from Transparent Classroom for school ID {} and user id {}'.format(
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
        teacher_default_classroom_data_teacher = list()
        teacher_accessible_classroom_data_teacher = list()
        if teacher_datum.get('default_classroom_id') is not None:
            teacher_default_classroom_datum = OrderedDict([
                ('school_id_tc', school_id),
                ('user_id_tc', teacher_datum.get('id')),
                ('pull_datetime', pull_datetime),
                ('teacher_default_classroom_id_tc', teacher_datum.get('default_classroom_id'))
            ])
            teacher_default_classroom_data_teacher.append(teacher_default_classroom_datum)
        for accessible_classroom_id in teacher_datum.get('accessible_classroom_ids', []):
            teacher_accessible_classroom_datum = OrderedDict([
                ('school_id_tc', school_id),
                ('user_id_tc', teacher_datum.get('id')),
                ('pull_datetime', pull_datetime),
                ('teacher_accessible_classroom_id_tc', accessible_classroom_id)
            ])
            teacher_accessible_classroom_data_teacher.append(teacher_accessible_classroom_datum)
        if format == 'dataframe':
            teacher_default_classroom_data_teacher = convert_teacher_default_classroom_data_to_df(teacher_default_classroom_data_teacher)
            teacher_accessible_classroom_data_teacher = convert_teacher_accessible_classroom_data_to_df(teacher_accessible_classroom_data_teacher)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return teacher_default_classroom_data_teacher, teacher_accessible_classroom_data_teacher

    def fetch_teacher_user_ids_school(
        self,
        school_id
    ):
        teachers_school = self.fetch_teacher_data_school(
            school_id=school_id,
            pull_datetime=None,
            format='list'
        )
        teacher_user_ids_school = [teacher.get('user_id_tc') for teacher in teachers_school]
        return teacher_user_ids_school

    def fetch_teacher_data(
        self,
        school_ids=None,
        pull_datetime=None,
        format='dataframe'
    ):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        if school_ids is None:
            school_ids=self.fetch_school_ids()
        logger.info('Fetching teacher data from Transparent Classroom for {} schools'.format(len(school_ids)))
        user_data = self.fetch_user_data(
            school_ids=school_ids,
            pull_datetime=pull_datetime,
            format='list'
        )
        teacher_data = list(filter(lambda user: 'teacher' in user.get('user_roles_tc'), user_data))
        if format == 'dataframe':
            teacher_data = convert_user_data_to_df(teacher_data)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return teacher_data

    def fetch_teacher_data_school(
        self,
        school_id,
        pull_datetime=None,
        format='dataframe'
    ):
        school_id = int(school_id)
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching teacher data from Transparent Classroom for school ID {}'.format(
            school_id
        ))
        user_data_school = self.fetch_user_data_school(
            school_id=school_id,
            pull_datetime=pull_datetime,
            format='list'
        )
        teacher_data_school = list(filter(lambda user: 'teacher' in user.get('user_roles_tc'), user_data_school))
        if format == 'dataframe':
            teacher_data_school = convert_user_data_to_df(teacher_data_school)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return teacher_data_school

    def fetch_parent_data(
        self,
        school_ids=None,
        pull_datetime=None,
        format='dataframe'
    ):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        if school_ids is None:
            school_ids=self.fetch_school_ids()
        logger.info('Fetching parent data from Transparent Classroom for {} schools'.format(len(school_ids)))
        user_data = self.fetch_user_data(
            school_ids=school_ids,
            pull_datetime=pull_datetime,
            format='list'
        )
        parent_data = list(filter(lambda user: 'parent' in user.get('user_roles_tc'), user_data))
        if format == 'dataframe':
            parent_data = convert_user_data_to_df(parent_data)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return parent_data

    def fetch_parent_data_school(
        self,
        school_id,
        pull_datetime=None,
        format='dataframe'
    ):
        school_id = int(school_id)
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info('Fetching parent data from Transparent Classroom for school ID {}'.format(
            school_id
        ))
        user_data_school = self.fetch_user_data_school(
            school_id=school_id,
            pull_datetime=pull_datetime,
            format='list'
        )
        parent_data_school = list(filter(lambda user: 'parent' in user.get('user_roles_tc'), user_data_school))
        if format == 'dataframe':
            parent_data_school = convert_user_data_to_df(parent_data_school)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return parent_data_school

    def fetch_user_data(
        self,
        school_ids=None,
        pull_datetime=None,
        format='dataframe'
    ):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        if school_ids is None:
            school_ids=self.fetch_school_ids()
        logger.info('Fetching user data from Transparent Classroom for {} schools'.format(len(school_ids)))
        user_data = list()
        for school_id in school_ids:
            user_data_school = self.fetch_user_data_school(
                school_id=school_id,
                pull_datetime=pull_datetime,
                format='list'
            )
            user_data.extend(user_data_school)
        if format == 'dataframe':
            user_data = convert_user_data_to_df(user_data)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return user_data

    def fetch_user_data_school(
        self,
        school_id,
        pull_datetime=None,
        format='dataframe'
    ):
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
        if format == 'dataframe':
            user_data_school = convert_user_data_to_df(user_data_school)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return user_data_school

    def fetch_classroom_data(
        self,
        school_ids=None,
        pull_datetime=None,
        format='dataframe'
    ):
        pull_datetime = wf_core_data.utils.to_datetime(pull_datetime)
        if pull_datetime is None:
            pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        if school_ids is None:
            school_ids=self.fetch_school_ids()
        logger.info('Fetching classroom data from Transparent Classroom for {} schools'.format(len(school_ids)))
        classroom_data = list()
        for school_id in school_ids:
            classroom_data_school = self.fetch_classroom_data_school(
                school_id=school_id,
                pull_datetime=pull_datetime,
                format='list'
            )
            classroom_data.extend(classroom_data_school)
        if format == 'dataframe':
            classroom_data = convert_classroom_data_to_df(classroom_data)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return classroom_data

    def fetch_classroom_data_school(
        self,
        school_id,
        pull_datetime=None,
        format='dataframe'
    ):
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
        if format == 'dataframe':
            classroom_data_school = convert_classroom_data_to_df(classroom_data_school)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return classroom_data_school

    def fetch_school_ids(self):
        logger.info('Fetching school IDs for all schools')
        school_data = self.fetch_school_data(
            pull_datetime=None,
            format='list'
        )
        school_ids = [school.get('school_id_tc') for school in school_data]
        return school_ids

    def fetch_school_data(
        self,
        pull_datetime=None,
        format='dataframe'
    ):
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
        if format == 'dataframe':
            school_data = convert_school_data_to_df(school_data)
        elif format == 'list':
            pass
        else:
            raise ValueError('Data format \'{}\' not recognized'.format(format))
        return school_data

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
            error_message = 'Transparent Classroom request returned status code {}'.format(r.status_code)
            try:
                if r.json().get('errors') is not None:
                    error_message += '\n{}'.format(json.dumps(r.json().get('errors'), indent=2))
            except:
                pass
            raise Exception(error_message)
        return r.json()

def write_data_local(
    data,
    base_directory,
    only_current=False,
    format='dataframe',
    output_directory_stem='transparent_classroom_snapshot',
    all_data_list_filename_stem ='data_tc_list_dict',
    school_data_filename_stem='school_data_tc',
    classroom_data_filename_stem='classroom_data_tc',
    user_data_filename_stem='user_data_tc',
    teacher_default_classroom_data_filename_stem='teacher_default_classroom_data_tc',
    teacher_accessible_classroom_data_filename_stem='teacher_accessible_classroom_data_tc',
    session_data_filename_stem='session_data_tc',
    student_data_filename_stem='student_data_tc',
    student_classroom_data_filename_stem='student_classroom_data_tc',
    student_parent_data_filename_stem='student_parent_data_tc'
):
    if only_current:
        output_directory_stem = output_directory_stem + '_current'
    # Create local directory
    pull_datetime = data['pull_datetime']
    timestamp_filename_suffix = pull_datetime.strftime('%Y%m%d_%H%M%S')
    output_directory = os.path.join(
        base_directory,
        '{}_{}'.format(
            output_directory_stem,
            timestamp_filename_suffix
        )
    )
    logger.info('Creating directory \'{}\''.format(
        output_directory
    ))
    os.makedirs(output_directory, exist_ok=True)
    if format == 'list':
        with open(
            os.path.join(
                output_directory,
                '{}_{}.pkl'.format(
                    all_data_list_filename_stem,
                    timestamp_filename_suffix
            ))
            ,'wb'
        ) as fp:
            pickle.dump(data, fp)
    elif format == 'dataframe':
        # Write school data in dataframe form to disk
        data['schools'].to_csv(
            os.path.join(
                output_directory,
                '{}_{}.csv'.format(
                    school_data_filename_stem,
                    timestamp_filename_suffix
                )
            ),
            index=False
        )
        data['schools'].to_pickle(os.path.join(
            output_directory,
            '{}_{}.pkl'.format(
                school_data_filename_stem,
                timestamp_filename_suffix
            )
        ))
        # Write classroom data in dataframe form to disk
        data['classrooms'].to_csv(
            os.path.join(
                output_directory,
                '{}_{}.csv'.format(
                    classroom_data_filename_stem,
                    timestamp_filename_suffix
                )
            ),
            index=False
        )
        data['classrooms'].to_pickle(os.path.join(
            output_directory,
            '{}_{}.pkl'.format(
                classroom_data_filename_stem,
                timestamp_filename_suffix
            )
        ))
        # Write user data in dataframe form to disk
        data['users'].to_csv(
            os.path.join(
                output_directory,
                '{}_{}.csv'.format(
                    user_data_filename_stem,
                    timestamp_filename_suffix
                )
            ),
            index=False
        )
        data['users'].to_pickle(os.path.join(
            output_directory,
            '{}_{}.pkl'.format(
                user_data_filename_stem,
                timestamp_filename_suffix
            )
        ))
        # Write teacher default classroom association data in dataframe form to disk
        data['teachers_default_classrooms'].to_csv(
            os.path.join(
                output_directory,
                '{}_{}.csv'.format(
                    teacher_default_classroom_data_filename_stem,
                    timestamp_filename_suffix
                )
            ),
            index=False
        )
        data['teachers_default_classrooms'].to_pickle(os.path.join(
            output_directory,
            '{}_{}.pkl'.format(
                teacher_default_classroom_data_filename_stem,
                timestamp_filename_suffix
            )
        ))
        # Write teacher accessible classroom association data in dataframe form to disk
        data['teachers_accessible_classrooms'].to_csv(
            os.path.join(
                output_directory,
                '{}_{}.csv'.format(
                    teacher_accessible_classroom_data_filename_stem,
                    timestamp_filename_suffix
                )
            ),
            index=False
        )
        data['teachers_accessible_classrooms'].to_pickle(os.path.join(
            output_directory,
            '{}_{}.pkl'.format(
                teacher_accessible_classroom_data_filename_stem,
                timestamp_filename_suffix
            )
        ))
        # Write session data in dataframe form to disk
        data['sessions'].to_csv(
            os.path.join(
                output_directory,
                '{}_{}.csv'.format(
                    session_data_filename_stem,
                    timestamp_filename_suffix
                )
            ),
            index=False
        )
        data['sessions'].to_pickle(os.path.join(
            output_directory,
            '{}_{}.pkl'.format(
                session_data_filename_stem,
                timestamp_filename_suffix
            )
        ))
        # Write student data in dataframe form to disk
        data['students'].to_csv(
            os.path.join(
                output_directory,
                '{}_{}.csv'.format(
                    student_data_filename_stem,
                    timestamp_filename_suffix
                )
            ),
            index=False
        )
        data['students'].to_pickle(os.path.join(
            output_directory,
            '{}_{}.pkl'.format(
                student_data_filename_stem,
                timestamp_filename_suffix
            )
        ))
        # Write student classroom association data in dataframe form to disk
        data['students_classrooms'].to_csv(
            os.path.join(
                output_directory,
                '{}_{}.csv'.format(
                    student_classroom_data_filename_stem,
                    timestamp_filename_suffix
                )
            ),
            index=False
        )
        data['students_classrooms'].to_pickle(os.path.join(
            output_directory,
            '{}_{}.pkl'.format(
                student_classroom_data_filename_stem,
                timestamp_filename_suffix
            )
        ))
        # Write student parent association data in dataframe form to disk
        data['students_parents'].to_csv(
            os.path.join(
                output_directory,
                '{}_{}.csv'.format(
                    student_parent_data_filename_stem,
                    timestamp_filename_suffix
                )
            ),
            index=False
        )
        data['students_parents'].to_pickle(os.path.join(
            output_directory,
            '{}_{}.pkl'.format(
                student_parent_data_filename_stem,
                timestamp_filename_suffix
            )
        ))
    else:
        raise ValueError('Data format \'{}\' not recognized'.format(format))

def convert_school_data_to_df(school_data):
    if len(school_data) == 0:
        return pd.DataFrame()
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
    school_data_df.set_index('school_id_tc', inplace=True)
    return school_data_df

def convert_classroom_data_to_df(classroom_data):
    if len(classroom_data) == 0:
        return pd.DataFrame()
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
    classroom_data_df.set_index(['school_id_tc', 'classroom_id_tc'], inplace=True)
    return classroom_data_df

def convert_user_data_to_df(user_data):
    if len(user_data) == 0:
        return pd.DataFrame()
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
    user_data_df.set_index(['school_id_tc', 'user_id_tc'], inplace=True)
    return user_data_df

def convert_teacher_default_classroom_data_to_df(teacher_default_classroom_data):
    if len(teacher_default_classroom_data) == 0:
        return pd.DataFrame()
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
    teacher_default_classroom_data_df.set_index(['school_id_tc', 'user_id_tc'], inplace=True)
    return teacher_default_classroom_data_df

def convert_teacher_accessible_classroom_data_to_df(teacher_accessible_classroom_data):
    if len(teacher_accessible_classroom_data) == 0:
        return pd.DataFrame()
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
    teacher_accessible_classroom_data_df.set_index(['school_id_tc', 'user_id_tc', 'teacher_accessible_classroom_id_tc'], inplace=True)
    return teacher_accessible_classroom_data_df

def convert_session_data_to_df(session_data):
    if len(session_data) == 0:
        return pd.DataFrame()
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
    session_data_df.set_index(['school_id_tc', 'session_id_tc'], inplace=True)
    return session_data_df

def convert_student_data_to_df(student_data):
    if len(student_data) == 0:
        return pd.DataFrame()
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
    student_data_df.set_index(['school_id_tc', 'student_id_tc'], inplace=True)
    return student_data_df

def convert_student_classroom_data_to_df(student_classroom_data):
    if len(student_classroom_data) == 0:
        return pd.DataFrame()
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
    student_classroom_data_df.set_index(['school_id_tc', 'student_id_tc', 'session_id_tc', 'classroom_id_tc'], inplace=True)
    return student_classroom_data_df

def convert_student_parent_data_to_df(student_parent_data):
    if len(student_parent_data) == 0:
        return pd.DataFrame()
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
    student_parent_data_df.set_index(['school_id_tc', 'student_id_tc', 'parent_id_tc'], inplace=True)
    return student_parent_data_df
