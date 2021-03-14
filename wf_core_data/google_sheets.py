from wf_core_data.utils import to_date
from gspread_pandas import Spread, Client
import pandas as pd
import json
import datetime
import time
import logging

logger = logging.getLogger(__name__)

def ingest_student_data_google_sheets(sheet_metadata, delay=None):
    df_list = list()
    logger.info('Ingesting data from each sheet')
    for sheet_metadatum in sheet_metadata:
        pull_date = sheet_metadatum['pull_date']
        sheet_id = sheet_metadatum['sheet_id']
        df_sheet = ingest_student_data_google_sheet(sheet_id, pull_date)
        df_list.append(df_sheet)
        if delay is not None:
            logger.info('Waiting {} seconds'.format(delay))
            time.sleep(delay)
    logger.info('Ingested data from {} sheets. Concatenating.'.format(len(df_list)))
    df = pd.concat(df_list, ignore_index=True)
    df.sort_values(['school_id_tc', 'student_id_tc', 'pull_datetime'], inplace=True, ignore_index=True)
    return df

def ingest_student_data_google_sheet(sheet_id, pull_date):
    logger.info('Ingesting data from sheet with pull date {} and ID {}'.format(pull_date, sheet_id))
    spread = Spread(sheet_id)
    df = spread.sheet_to_df(index=None)
    df['school_id_tc'] = pd.to_numeric(df['school_id']).astype('Int64')
    df['child_raw_dict'] = df['child_raw'].apply(lambda x: json.loads(x))
    df['student_id_tc'] = pd.to_numeric(df['child_raw_dict'].apply(lambda x: int(x.get('id')))).astype('Int64')
    df['pull_datetime'] = pd.to_datetime(pull_date, utc=True)
    df['student_first_name_tc'] = df['child_raw_dict'].apply(lambda x: x.get('first_name')).astype('string')
    df['student_middle_name_tc'] = df['child_raw_dict'].apply(lambda x: x.get('middle_name')).astype('string')
    df['student_last_name_tc'] = df['child_raw_dict'].apply(lambda x: x.get('last_name')).astype('string')
    df['student_birth_date_tc'] = df['child_raw_dict'].apply(lambda x: to_date(x.get('birth_date')))
    df['student_gender_tc'] = df['child_raw_dict'].apply(lambda x: x.get('gender')).astype('string')
    df['student_ethnicity_tc'] = df['child_raw_dict'].apply(lambda x: x.get('ethnicity'))
    df['student_dominant_language_tc'] = df['child_raw_dict'].apply(lambda x: x.get('dominant_language')).astype('string')
    df['student_household_income_tc'] = df['child_raw_dict'].apply(lambda x: x.get('household_income')).astype('string')
    df['student_grade_tc'] = df['child_raw_dict'].apply(lambda x: x.get('grade')).astype('string')
    df['student_classroom_ids_tc'] = df['child_raw_dict'].apply(lambda x: x.get('classroom_ids'))
    df['student_program_tc'] = df['child_raw_dict'].apply(lambda x: x.get('program')).astype('string')
    df['student_hours_string_tc'] = df['child_raw_dict'].apply(lambda x: x.get('hours_string')).astype('string')
    df['student_id_alt_tc'] = df['child_raw_dict'].apply(lambda x: x.get('student_id')).astype('string')
    df['student_allergies_tc'] = df['child_raw_dict'].apply(lambda x: x.get('allergies')).astype('string')
    df['student_parent_ids_tc'] = df['child_raw_dict'].apply(lambda x: x.get('parent_ids'))
    df['student_approved_adults_string_tc'] = df['child_raw_dict'].apply(lambda x: x.get('approved_adults_string')).astype('string')
    df['student_emergency_contacts_string_tc'] = df['child_raw_dict'].apply(lambda x: x.get('emergency_contacts_string')).astype('string')
    df['student_notes_tc'] = df['child_raw_dict'].apply(lambda x: x.get('notes')).astype('string')
    df['student_last_day_tc'] = df['child_raw_dict'].apply(lambda x: to_date(x.get('last_day')))
    df['student_exit_reason_tc'] = df['child_raw_dict'].apply(lambda x: x.get('exit_reason')).astype('string')
    df['student_exit_survey_id_tc'] = pd.to_numeric(df['child_raw_dict'].apply(lambda x: x.get('exit_survey_id'))).astype('Int64')
    df['student_exit_notes_tc'] = df['child_raw_dict'].apply(lambda x: x.get('exit_notes')).astype('string')
    df = df.reindex(columns=[
        'school_id_tc',
        'student_id_tc',
        'pull_datetime',
        'student_first_name_tc',
        'student_middle_name_tc',
        'student_last_name_tc',
        'student_birth_date_tc',
        'student_gender_tc',
        'student_ethnicity_tc',
        'student_dominant_language_tc',
        'student_household_income_tc',
        'student_grade_tc',
        'student_classroom_ids_tc',
        'student_program_tc',
        'student_hours_string_tc',
        'student_id_alt_tc',
        'student_allergies_tc',
        'student_parent_ids_tc',
        'student_approved_adults_string_tc',
        'student_emergency_contacts_string_tc',
        'student_notes_tc',
        'student_last_day_tc',
        'student_exit_reason_tc',
        'student_exit_survey_id_tc',
        'student_exit_notes_tc'
    ])
    if df.duplicated(['school_id_tc', 'student_id_tc']).any():
        raise ValueError('Ingested data contains duplicate Transparent Classroom school ID/student id combinations')
    return df
