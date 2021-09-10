import wf_core_data.rosters.shared_constants
import pandas as pd
import uuid
import os
import logging

logger = logging.getLogger(__name__)

MEFS_TARGET_COLUMN_NAMES = [
    'FirstName',
    'LastName',
    'ChildID',
    'BirthMonthYear',
    'Gender',
    'SpecialEducationServices',
    'Ethnicity',
    'SecondLanguageLearner',
    'PostalCode',
    'CountryCode',
    'Notes',
    'Class'
]

MEFS_GENDER_MAP = {
    'M': 'Male',
    'F': 'Female',
    'unmatched_value': 'Other',
    'na_value': 'Other'
}

MEFS_ETHNICITY_MAP = {
    'african_american': 'Black / African',
    'asian_american': 'Asian',
    'hispanic': 'Hispanic / Latino',
    'middle_eastern': None,
    'native_american': 'Native / Aboriginal',
    'other': None,
    'pacific_islander': 'Hawaiian / Pacific Islander',
    'white': 'White / Caucasian',
    'unmatched_value': None,
    'na_value': None
}

MEFS_GRADE_NAME_MAP = {
    'EC': 'EC',
    'PK': 'PK',
    'PK_3': 'PK',
    'PK_4': 'PK',
    'K': 'K',
    '1': '1',
    '2': '2',
    '3': '3',
    '4': '4',
    '5': '5',
    '6': '6',
    '7': '7',
    '8': '8',
    '9': '9',
    '10': '10',
    '11': '11',
    '12': '12',
    'unmatched_value': None,
    'na_value': None
}

MEFS_TESTABLE_GRADES = [
    'K',
    '1',
    '2',
    '3',
    '4',
    '5',
    '6',
    '7',
    '8',
    '9',
    '10',
    '11',
    '12'
]

def create_mefs_roster_and_write_locally(
    base_directory,
    filename_suffix,
    master_roster_subdirectory='master_rosters',
    master_roster_filename_stem='master_roster',
    mefs_roster_subdirectory='mefs_rosters',
    mefs_roster_filename_stem='mefs_roster',
    mefs_ids_filename_suffix=None,
    mefs_ids_subdirectory='mefs_ids',
    mefs_ids_filename_stem='mefs_ids'
):
    if mefs_ids_filename_suffix is None:
        mefs_ids_filename_suffix=filename_suffix
    master_roster_filename = os.path.join(
        base_directory,
        master_roster_subdirectory,
        '{}_{}'.format(
            master_roster_filename_stem,
            filename_suffix
        ),
        '{}_{}.pkl'.format(
            master_roster_filename_stem,
            filename_suffix
        )
    )
    master_roster_data = pd.read_pickle(master_roster_filename)
    mefs_ids_filename = os.path.join(
        base_directory,
        mefs_ids_subdirectory,
        '{}_{}'.format(
            mefs_ids_filename_stem,
            mefs_ids_filename_suffix
        ),
        '{}_{}.pkl'.format(
            mefs_ids_filename_stem,
            mefs_ids_filename_suffix
        )
    )
    old_mefs_ids = pd.read_pickle(mefs_ids_filename)
    mefs_roster_data, new_mefs_ids = wf_core_data.create_mefs_roster(
        master_roster_data=master_roster_data,
        mefs_ids=old_mefs_ids
    )
    write_mefs_rosters_local(
        mefs_roster_data=mefs_roster_data,
        base_directory=base_directory,
        subdirectory=mefs_roster_subdirectory,
        filename_stem=mefs_roster_filename_stem,
        filename_suffix=filename_suffix
    )
    write_mefs_ids_local(
        mefs_ids=new_mefs_ids,
        base_directory=base_directory,
        subdirectory=mefs_ids_subdirectory,
        filename_stem=mefs_ids_filename_stem,
        filename_suffix=filename_suffix
    )


def create_mefs_roster(
    master_roster_data,
    mefs_ids
):
    # Rename fields
    logger.info('Renaming fields')
    mefs_roster_data = (
        master_roster_data
        .rename(columns = {
            'student_first_name_tc': 'FirstName',
            'student_last_name_tc': 'LastName',
            'school_zip_code_tc': 'PostalCode'
        })
    )
    # Create new fields
    ## Child ID
    logger.info('Creating child ID field')
    mefs_id_map = (
        mefs_ids
        .copy()
        .reset_index()
        .dropna(subset=['school_id_tc', 'student_id_tc'])
        .set_index(['school_id_tc', 'student_id_tc'])
    )
    mefs_roster_data = (
        mefs_roster_data
        .join(
            mefs_id_map,
            how='left'
        )
        .rename(columns={'student_id_mefs_wf': 'ChildID'})
    )
    num_new_mefs_ids = mefs_roster_data['ChildID'].isna().sum()
    logger.info('There appear to be {} records without MEFS child IDs. Generating.'.format(
        num_new_mefs_ids
    ))
    mefs_roster_data['ChildID'] = mefs_roster_data['ChildID'].apply(
        lambda x: x if pd.notna(x) else str(uuid.uuid4())[-10:]
    )
    mefs_ids_new = (
        mefs_roster_data
        .loc[:, ['ChildID']]
        .rename(columns={'ChildID': 'student_id_mefs_wf'})
        .reset_index()
        .set_index('student_id_mefs_wf')
    )
    mefs_ids_combined = (
        pd.concat((
            mefs_ids,
            mefs_ids_new
        ))
        .reset_index()
        .drop_duplicates()
        .set_index('student_id_mefs_wf')
    )
    if len(mefs_ids_combined) - len(mefs_ids) != num_new_mefs_ids:
        raise ValueError('{} MEFS child IDs were provided and {} new IDs were generated but new table contains {} IDs'.format(
            len(mefs_ids),
            num_new_mefs_ids,
            len(mefs_ids_combined)
        ))
    logger.info('Provided MEFS child ID table contained {} IDs. {} new IDs were generated. New MEFS child ID table contains {} IDs'.format(
        len(mefs_ids),
        num_new_mefs_ids,
        len(mefs_ids_combined)
    ))
    ## Student birth date
    logger.info('Creating birth month and year field')
    mefs_roster_data['BirthMonthYear'] = mefs_roster_data['student_birth_date_tc'].apply(
        lambda x: x.strftime('%Y-%m-%d')
    )
    ## Student gender
    logger.info('Creating gender field')
    mefs_roster_data['Gender'] = mefs_roster_data['student_gender_wf'].apply(
        lambda x: MEFS_GENDER_MAP.get(x, MEFS_GENDER_MAP.get('unmatched_value')) if pd.notna(x) else MEFS_GENDER_MAP.get('na_value')
    )
    ## Special education services
    logger.info('Creating special education services field')
    mefs_roster_data['SpecialEducationServices'] = 'Unknown'
    ## Student ethnicity
    logger.info('Creating ethnicity field')
    def student_ethnicity_mefs(ethnicity_list):
        if not isinstance(ethnicity_list, list):
            return ''
        ethnicity_list_mefs = list()
        for ethnicity in ethnicity_list:
            ethnicity_mefs = MEFS_ETHNICITY_MAP.get(ethnicity)
            if ethnicity_mefs is not None:
                ethnicity_list_mefs.append(ethnicity_mefs)
        ethnicity_string_mefs = '|'.join(sorted(list(set(ethnicity_list_mefs))))
        return ethnicity_string_mefs
    mefs_roster_data['Ethnicity'] = mefs_roster_data['student_ethnicity_wf'].apply(student_ethnicity_mefs)
    ## Second language learner
    logger.info('Creating second language learner field')
    mefs_roster_data['SecondLanguageLearner'] = 'Unknown'
    ## Country code
    logger.info('Creating country code field')
    mefs_roster_data['CountryCode'] = 'US'
    ## Arrange columns and rows
    logger.info('Rearranging columns and rows')
    mefs_roster_data = (
        mefs_roster_data
        .reindex(columns=(
            wf_core_data.rosters.shared_constants.GROUPING_COLUMN_NAMES +
            MEFS_TARGET_COLUMN_NAMES
        ))
        .sort_values(
            wf_core_data.rosters.shared_constants.GROUPING_COLUMN_NAMES +
            ['FirstName', 'LastName']
        )
    )
    # Create output
    mefs_roster_data = (
        mefs_roster_data
        .reset_index(drop=True)
        .astype('object')
    )
    return mefs_roster_data, mefs_ids_combined

def write_mefs_rosters_local(
    mefs_roster_data,
    base_directory,
    subdirectory='mefs_rosters',
    filename_stem='mefs_roster',
    filename_suffix=None

):
    if filename_suffix is None:
        filename_suffix = datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y%m%d')
    logger.info('Filename suffix is {}'.format(filename_suffix))
    output_directory_base = os.path.join(
        base_directory,
        subdirectory,
        '{}_{}'.format(
            filename_stem,
            filename_suffix
        )
    )
    output_directory_csv = os.path.join(
        output_directory_base,
        'csv'
    )
    output_directory_pickle = os.path.join(
        output_directory_base,
        'pickle'
    )
    output_directory_excel = os.path.join(
        output_directory_base,
        'excel'
    )
    os.makedirs(output_directory_csv, exist_ok=True)
    os.makedirs(output_directory_pickle, exist_ok=True)
    os.makedirs(output_directory_excel, exist_ok=True)
    output = (
        mefs_roster_data
        .drop(columns=wf_core_data.rosters.shared_constants.GROUPING_COLUMN_NAMES)
    )
    filename = '{}_{}'.format(
        filename_stem,
        filename_suffix
    )
    output.to_csv(
        os.path.join(
            output_directory_csv,
            '{}.csv'.format(
                filename
            )
        ),
        index = False
    )
    output.to_pickle(
        os.path.join(
            output_directory_pickle,
            '{}.pkl'.format(
                filename
            )
        )
    )
    output.to_excel(
        os.path.join(
            output_directory_excel,
            '{}.xlsx'.format(
                filename
            )
        )
    )

    for legal_entity_short_name, roster_df_group in mefs_roster_data.groupby('legal_entity_short_name_wf'):
        output = (
            roster_df_group
            .drop(columns=wf_core_data.rosters.shared_constants.GROUPING_COLUMN_NAMES)
        )
        filename = '{}_{}_{}'.format(
            filename_stem,
            legal_entity_short_name,
            filename_suffix
        )
        output.to_csv(
            os.path.join(
                output_directory_csv,
                '{}.csv'.format(
                    filename
                )
            ),
            index = False
        )
        output.to_pickle(
            os.path.join(
                output_directory_pickle,
                '{}.pkl'.format(
                    filename
                )
            )
        )
        output.to_excel(
            os.path.join(
                output_directory_excel,
                '{}.xlsx'.format(
                    filename
                )
            )
        )

    for school_short_name, roster_df_group in mefs_roster_data.groupby('school_short_name_wf'):
        output = (
            roster_df_group
            .drop(columns=wf_core_data.rosters.shared_constants.GROUPING_COLUMN_NAMES)
        )
        filename = '{}_{}_{}'.format(
            filename_stem,
            school_short_name,
            filename_suffix
        )
        output.to_csv(
            os.path.join(
                output_directory_csv,
                '{}.csv'.format(
                    filename
                )
            ),
            index = False
        )
        output.to_pickle(
            os.path.join(
                output_directory_pickle,
                '{}.pkl'.format(
                    filename
                )
            )
        )
        output.to_excel(
            os.path.join(
                output_directory_excel,
                '{}.xlsx'.format(
                    filename
                )
            ),
            index=False
        )

    for classroom_short_name, roster_df_group in mefs_roster_data.groupby('classroom_short_name_wf'):
        output = (
            roster_df_group
            .drop(columns=wf_core_data.rosters.shared_constants.GROUPING_COLUMN_NAMES)
        )
        filename = '{}_{}_{}'.format(
            filename_stem,
            classroom_short_name,
            filename_suffix
        )
        output.to_csv(
            os.path.join(
                output_directory_csv,
                '{}.csv'.format(
                    filename
                )
            ),
            index = False
        )
        output.to_pickle(
            os.path.join(
                output_directory_pickle,
                '{}.pkl'.format(
                    filename
                )
            )
        )
        output.to_excel(
            os.path.join(
                output_directory_excel,
                '{}.xlsx'.format(
                    filename
                )
            ),
            index=False
        )

def write_mefs_ids_local(
    mefs_ids,
    base_directory,
    subdirectory='mefs_ids',
    filename_stem='mefs_ids',
    filename_suffix=None

):
    if filename_suffix is None:
        filename_suffix = datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y%m%d')
    logger.info('Filename suffix is {}'.format(filename_suffix))
    output_directory = os.path.join(
        base_directory,
        subdirectory,
        '{}_{}'.format(
            filename_stem,
            filename_suffix
        )
    )
    os.makedirs(output_directory, exist_ok=True)
    filename = '{}_{}'.format(
        filename_stem,
        filename_suffix
    )
    mefs_ids.to_csv(
        os.path.join(
            output_directory,
            '{}.csv'.format(
                filename
            )
        )
    )
    mefs_ids.to_pickle(
        os.path.join(
            output_directory,
            '{}.pkl'.format(
                filename
            )
        )
    )
