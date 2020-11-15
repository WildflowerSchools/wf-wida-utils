from gspread_pandas import Spread, Client
import pandas as pd
import json
import logging

logger = logging.getLogger(__name__)

def ingest_student_data_google_sheets(sheet_metadata):
    df_list = list()
    logger.info('Ingesting data from each sheet')
    for sheet_metadatum in sheet_metadata:
        pull_date = sheet_metadatum['pull_date']
        sheet_id = sheet_metadatum['sheet_id']
        df_sheet = ingest_student_data_google_sheet(sheet_id, pull_date)
        df_list.append(df_sheet)
    logger.info('Ingested data from {} sheets. Concatenating.'.format(len(df_list)))
    df = pd.concat(df_list, ignore_index=True)
    df.sort_values(['tc_school_id', 'tc_student_id', 'pull_date'], inplace=True, ignore_index=True)
    return df

def ingest_student_data_google_sheet(sheet_id, pull_date):
    logger.info('Ingesting data from sheet with pull date {} and ID {}'.format(pull_date, sheet_id))
    spread = Spread(sheet_id)
    df = spread.sheet_to_df(index=None)
    df['tc_school_id'] = df['school_id'].astype(int)
    df['child_raw_dict'] = df['child_raw'].apply(lambda x: json.loads(x))
    df['tc_student_id'] = df['child_raw_dict'].apply(lambda x: int(x.get('id')))
    df['pull_date'] = pull_date
    df['student_first_name'] = df['child_raw_dict'].apply(lambda x: x.get('first_name'))
    df['student_last_name'] = df['child_raw_dict'].apply(lambda x: x.get('last_name'))
    df['student_birth_date'] = df['child_raw_dict'].apply(lambda x: pd.to_datetime(x.get('birth_date')).date())
    df['student_gender'] = df['child_raw_dict'].apply(lambda x: x.get('gender'))
    df.replace({'Male': 'M', 'Female': 'F'}, inplace=True)
    df['student_ethnicity_list'] = df['child_raw_dict'].apply(lambda x: x.get('ethnicity'))
    df['student_dominant_language'] = df['child_raw_dict'].apply(lambda x: x.get('dominant_language'))
    df = df.reindex(columns=[
        'tc_school_id',
        'tc_student_id',
        'pull_date',
        'student_first_name',
        'student_last_name',
        'student_birth_date',
        'student_gender',
        'student_ethnicity_list',
        'student_dominant_language'

    ])
    if df.duplicated(['tc_school_id', 'tc_student_id']).any():
        raise ValueError('Ingested data contains duplicate Transparent Classroom school ID/student id combinations')
    return df
