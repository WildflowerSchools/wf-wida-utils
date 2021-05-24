import wf_core_data.rosters.shared_constants
import pandas as pd
import uuid
import logging

logger = logging.getLogger(__name__)

MEFS_TARGET_COLUMN_NAMES = [
    'First Name',
    'Last Name',
    'Child ID',
    'Birth Month Year',
    'Gender',
    'Special Education Services',
    'Ethnicity',
    'Second Language Learner',
    'Postal Code',
    'Country Code',
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

def create_mefs_roster(
    master_roster,
    mefs_ids
):
    # Rename fields
    logger.info('Renaming fields')
    mefs_roster = (
        master_roster
        .rename(columns = {
            'student_first_name_tc': 'First Name',
            'student_last_name_tc': 'Last Name',
            'school_zip_code_tc': 'Postal Code'
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
    mefs_roster = (
        mefs_roster
        .join(
            mefs_id_map,
            how='left'
        )
        .rename(columns={'student_id_mefs_wf': 'Child ID'})
    )
    num_new_mefs_ids = mefs_roster['Child ID'].isna().sum()
    logger.info('There appear to be {} records without MEFS child IDs. Generating.'.format(
        num_new_mefs_ids
    ))
    mefs_roster['Child ID'] = mefs_roster['Child ID'].apply(
        lambda x: x if pd.notna(x) else str(uuid.uuid4())[-10:]
    )
    mefs_ids_new = (
        mefs_roster
        .loc[:, ['Child ID']]
        .rename(columns={'Child ID': 'student_id_mefs_wf'})
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
    mefs_roster['Birth Month Year'] = mefs_roster['student_birth_date_tc'].apply(
        lambda x: x.strftime('%Y-%m-%d')
    )
    ## Student gender
    logger.info('Creating gender field')
    mefs_roster['Gender'] = mefs_roster['student_gender_wf'].apply(
        lambda x: MEFS_GENDER_MAP.get(x, MEFS_GENDER_MAP.get('unmatched_value')) if pd.notna(x) else MEFS_GENDER_MAP.get('na_value')
    )
    ## Special education services
    logger.info('Creating special education services field')
    mefs_roster['Special Education Services'] = 'Unknown'
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
    mefs_roster['Ethnicity'] = mefs_roster['student_ethnicity_wf'].apply(student_ethnicity_mefs)
    ## Second language learner
    logger.info('Creating second language learner field')
    mefs_roster['Second Language Learner'] = 'Unknown'
    ## Country code
    logger.info('Creating country code field')
    mefs_roster['Country Code'] = 'US'
    ## Arrange columns and rows
    logger.info('Rearranging columns and rows')
    mefs_roster = (
        mefs_roster
        .reindex(columns=(
            wf_core_data.rosters.shared_constants.GROUPING_COLUMN_NAMES +
            MEFS_TARGET_COLUMN_NAMES
        ))
        .sort_values(
            wf_core_data.rosters.shared_constants.GROUPING_COLUMN_NAMES +
            ['First Name', 'Last Name']
        )
    )
    # Create output
    mefs_roster = (
        mefs_roster
        .reset_index(drop=True)
        .astype('object')
    )
    return mefs_roster, mefs_ids_combined
