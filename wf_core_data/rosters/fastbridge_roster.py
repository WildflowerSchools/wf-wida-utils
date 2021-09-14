import wf_core_data.rosters.shared_constants
import wf_core_data.rosters.shared_functions
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

FASTBRIDGE_TARGET_COLUMN_NAMES = [
    'State',
    'SchoolDistrict',
    'School',
    'Grade',
    'Course',
    'Section',
    'StudentID',
    'StudentStateID',
    'StudentFirstName',
    'StudentLastName',
    'TeacherID',
    'TeacherFirstName',
    'TeacherLastName',
    'TeacherEmail',
    'StudentGender',
    'StudentBirthDate',
    'StudentRace',
    'MealStatus',
    'EnglishProficiency',
    'NativeLanguage',
    'ServiceCode',
    'PrimaryDisabilityType',
    'IEPReading',
    'IEPMath',
    'IEPBehavior',
    'GiftedAndTalented',
    'Section504',
    'Mobility'
]

FASTBRIDGE_GENDER_MAP = {
    'M': 'M',
    'F': 'F',
    'unmatched_value': None,
    'na_value': None
}

FASTBRIDGE_ETHNICITY_MAP = {
    'african_american': 'AA',
    'asian_american': 'AS',
    'hispanic': 'HI',
    'middle_eastern': 'OT',
    'native_american': 'AI',
    'other': 'OT',
    'pacific_islander': 'NH',
    'white': 'WH',
    'unmatched_value': 'OT',
    'na_value': None,
    'multiple_values': 'MT'
}

FASTBRIDGE_GRADE_NAME_MAP = {
    'EC': 'EC',
    'PK': 'PK',
    'PK_3': 'PK',
    'PK_4': 'PK',
    'K': 'KG',
    '1': '01',
    '2': '02',
    '3': '03',
    '4': '04',
    '5': '05',
    '6': '06',
    '7': '07',
    '8': '08',
    '9': '09',
    '10': '10',
    '11': '11',
    '12': '12',
    'unmatched_value': None,
    'na_value': None
}

FASTBRIDGE_TESTABLE_GRADES = [
    'PK',
    'KG',
    '01',
    '02',
    '03',
    '04',
    '05',
    '06',
    '07',
    '08',
    '09',
    '10',
    '11',
    '12'
]

def create_fastbridge_roster_and_write_locally(
    base_directory,
    filename_suffix,
    master_roster_subdirectory='master_rosters',
    master_roster_filename_stem='master_roster',
    fastbridge_roster_subdirectory='fastbridge_rosters',
    fastbridge_roster_filename_stem='fastbridge_roster'
):
    filename = os.path.join(
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
    master_roster_data = pd.read_pickle(filename)
    fastbridge_roster_data = wf_core_data.create_fastbridge_roster(
        master_roster_data=master_roster_data
    )
    wf_core_data.write_fastbridge_rosters_local(
        fastbridge_roster_data=fastbridge_roster_data,
        base_directory=base_directory,
        subdirectory=fastbridge_roster_subdirectory,
        filename_stem=fastbridge_roster_filename_stem,
        filename_suffix=filename_suffix
    )

def create_fastbridge_roster(
    master_roster_data
):
    ## Rename fields
    logger.info('Renaming fields')
    fastbridge_roster_data = (
        master_roster_data
        .rename(columns = {
            'school_state': 'State',
            'legal_entity_name_wf': 'SchoolDistrict',
            'school_name_tc': 'School',
            'classroom_name_tc': 'Course',
            'student_id_alt_normalized_tc': 'StudentStateID',
            'student_first_name_tc': 'StudentFirstName',
            'student_last_name_tc': 'StudentLastName',
            'teacher_id_tc': 'TeacherID',
            'teacher_first_name_tc': 'TeacherFirstName',
            'teacher_last_name_tc': 'TeacherLastName',
            'teacher_email_tc':  'TeacherEmail'
        })
    )
    ## Create new fields
    ### Section
    fastbridge_roster_data['Section'] = 'S1'
    ### Student ID
    logger.info('Creating student ID field')
    fastbridge_roster_data['StudentID'] = fastbridge_roster_data.index.get_level_values('student_id_tc')
    ### Student birth date
    logger.info('Creating birth date field')
    fastbridge_roster_data['StudentBirthDate'] = fastbridge_roster_data['student_birth_date_tc'].apply(
        lambda x: x.strftime('%m/%d/%Y')
    )
    ### Student gender
    logger.info('Creating gender field')
    fastbridge_roster_data['StudentGender'] = fastbridge_roster_data['student_gender_wf'].apply(
        lambda x: FASTBRIDGE_GENDER_MAP.get(x, FASTBRIDGE_GENDER_MAP.get('unmatched_value')) if pd.notna(x) else FASTBRIDGE_GENDER_MAP.get('na_value')
    )
    ### Grade
    logger.info('Creating grade field')
    fastbridge_roster_data['Grade'] = fastbridge_roster_data['student_grade_wf'].apply(
        lambda x: FASTBRIDGE_GRADE_NAME_MAP.get(x, FASTBRIDGE_GRADE_NAME_MAP.get('unmatched_value')) if pd.notna(x) else FASTBRIDGE_GRADE_NAME_MAP.get('na_value')
    )
    ### Student ethnicity
    logger.info('Creating ethnicity field')
    def student_race_fastbridge(ethnicity_list):
        if not isinstance(ethnicity_list, list):
            return FASTBRIDGE_ETHNICITY_MAP.get('na_value')
        if len(ethnicity_list) > 1:
            return FASTBRIDGE_ETHNICITY_MAP.get('multiple_values')
        return FASTBRIDGE_ETHNICITY_MAP.get(ethnicity_list[0], FASTBRIDGE_ETHNICITY_MAP.get('unmatched_value'))
    fastbridge_roster_data['StudentRace'] = fastbridge_roster_data['student_ethnicity_wf'].apply(student_race_fastbridge)
    ## Arrange columns and rows
    logger.info('Rearranging columns and rows')
    fastbridge_roster_data = (
        fastbridge_roster_data
        .reindex(columns=(
            wf_core_data.rosters.shared_constants.GROUPING_COLUMN_NAMES +
            FASTBRIDGE_TARGET_COLUMN_NAMES
        ))
        .sort_values(
            wf_core_data.rosters.shared_constants.GROUPING_COLUMN_NAMES +
            ['Grade', 'StudentFirstName', 'StudentLastName']
        )
    )
    ## Create output
    logger.info('Restriction to testable grades. {} student records before restricting'.format(
        len(fastbridge_roster_data)
    ))
    fastbridge_roster_data = (
        fastbridge_roster_data
        .loc[fastbridge_roster_data['Grade'].isin(FASTBRIDGE_TESTABLE_GRADES)]
        .copy()
        .reset_index(drop=True)
        .astype('object')
    )
    logger.info('Restricted to testable grades. {} student records after restricting'.format(
        len(fastbridge_roster_data)
    ))
    return fastbridge_roster_data

def write_fastbridge_rosters_local(
    fastbridge_roster_data,
    base_directory,
    subdirectory='fastbridge_rosters',
    filename_stem='fastbridge_roster',
    filename_suffix=None

):
    wf_core_data.rosters.shared_functions.write_rosters_local(
        roster_data=fastbridge_roster_data,
        base_directory=base_directory,
        subdirectory=subdirectory,
        filename_stem=filename_stem,
        filename_suffix=filename_suffix

    )
