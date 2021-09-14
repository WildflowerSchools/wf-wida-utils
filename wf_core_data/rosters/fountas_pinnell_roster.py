import wf_core_data.rosters.shared_constants
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

FOUNTAS_PINNELL_TARGET_COLUMN_NAMES = [
    '## First Name',
    'Last Name',
    'Student ID',
    'DOB',
    'Grade',
    'Gender',
    'Race/Ethnicity',
    'Language',
    'School Lunch Program',
    'Eng Lang Learner',
    'SpEd',
    'Addl Reading Serv',
    'Other Services Programs',
    'Other Services Description',
    'Custom 1',
    'Custom 2'
]

FOUNTAS_PINNELL_GENDER_MAP = {
    'M': 'M',
    'F': 'F',
    'unmatched_value': 'F',
    'na_value': 'F'
}

FOUNTAS_PINNELL_ETHNICITY_MAP = {
    'african_american': 'Black',
    'asian_american': 'Asian/Pacific Islander',
    'hispanic': 'Hispanic',
    'middle_eastern': 'Other',
    'native_american': 'Native American/Alaskan',
    'other': 'Other',
    'pacific_islander': 'Asian/Pacific Islander',
    'white': 'White',
    'unmatched_value': 'Other',
    'na_value': None,
    'multiple_values': 'Multiracial'
}

FOUNTAS_PINNELL_GRADE_NAME_MAP = {
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

FOUNTAS_PINNELL_TESTABLE_GRADES = [
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

def create_fountas_pinnell_roster_and_write_locally(
    base_directory,
    filename_suffix,
    master_roster_subdirectory='master_rosters',
    master_roster_filename_stem='master_roster',
    fountas_pinnell_roster_subdirectory='fountas_pinnell_rosters',
    fountas_pinnell_roster_filename_stem='fountas_pinnell_roster'
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
    fountas_pinnell_roster_data = wf_core_data.create_fountas_pinnell_roster(
        master_roster_data=master_roster_data
    )
    wf_core_data.write_fountas_pinnell_rosters_local(
        fountas_pinnell_roster_data=fountas_pinnell_roster_data,
        base_directory=base_directory,
        subdirectory=fountas_pinnell_roster_subdirectory,
        filename_stem=fountas_pinnell_roster_filename_stem,
        filename_suffix=filename_suffix
    )

def create_fountas_pinnell_roster(
    master_roster_data
):
    ## Rename fields
    logger.info('Renaming fields')
    fountas_pinnell_roster_data = (
        master_roster_data
        .rename(columns = {
            'student_first_name_tc': '## First Name',
            'student_last_name_tc': 'Last Name'
        })
    )
    ## Create new fields
    ### Student ID
    logger.info('Creating student ID field')
    fountas_pinnell_roster_data['Student ID'] = fountas_pinnell_roster_data.apply(
        lambda row: '{}-{}'.format(
            row.name[0],
            row.name[1]
        ),
        axis=1
    )
    ### Student birth date
    logger.info('Creating birth date field')
    fountas_pinnell_roster_data['DOB'] = fountas_pinnell_roster_data['student_birth_date_tc'].apply(
        lambda x: x.strftime('%m/%d/%Y')
    )
    ### Student gender
    logger.info('Creating gender field')
    fountas_pinnell_roster_data['Gender'] = fountas_pinnell_roster_data['student_gender_wf'].apply(
        lambda x: FOUNTAS_PINNELL_GENDER_MAP.get(x, FOUNTAS_PINNELL_GENDER_MAP.get('unmatched_value')) if pd.notna(x) else FOUNTAS_PINNELL_GENDER_MAP.get('na_value')
    )
    ### Grade
    logger.info('Creating grade field')
    fountas_pinnell_roster_data['Grade'] = fountas_pinnell_roster_data['student_grade_wf'].apply(
        lambda x: FOUNTAS_PINNELL_GRADE_NAME_MAP.get(x, FOUNTAS_PINNELL_GRADE_NAME_MAP.get('unmatched_value')) if pd.notna(x) else FOUNTAS_PINNELL_GRADE_NAME_MAP.get('na_value')
    )
    ### Student ethnicity
    logger.info('Creating ethnicity field')
    def student_race_fountas_pinnell(ethnicity_list):
        if not isinstance(ethnicity_list, list):
            return FOUNTAS_PINNELL_ETHNICITY_MAP.get('na_value')
        if len(ethnicity_list) > 1:
            return FOUNTAS_PINNELL_ETHNICITY_MAP.get('multiple_values')
        return FOUNTAS_PINNELL_ETHNICITY_MAP.get(ethnicity_list[0], FOUNTAS_PINNELL_ETHNICITY_MAP.get('unmatched_value'))
    fountas_pinnell_roster_data['Race/Ethnicity'] = fountas_pinnell_roster_data['student_ethnicity_wf'].apply(student_race_fountas_pinnell)
    ### Language
    fountas_pinnell_roster_data['Language'] = 'English'
    ## Arrange columns and rows
    logger.info('Rearranging columns and rows')
    fountas_pinnell_roster_data = (
        fountas_pinnell_roster_data
        .reindex(columns=(
            wf_core_data.rosters.shared_constants.GROUPING_COLUMN_NAMES +
            FOUNTAS_PINNELL_TARGET_COLUMN_NAMES
        ))
        .sort_values(
            wf_core_data.rosters.shared_constants.GROUPING_COLUMN_NAMES +
            ['Grade', '## First Name', 'Last Name']
        )
    )
    ## Create output
    logger.info('Restriction to testable grades. {} student records before restricting'.format(
        len(fountas_pinnell_roster_data)
    ))
    fountas_pinnell_roster_data = (
        fountas_pinnell_roster_data
        .loc[fountas_pinnell_roster_data['Grade'].isin(FOUNTAS_PINNELL_TESTABLE_GRADES)]
        .copy()
        .reset_index(drop=True)
        .astype('object')
    )
    logger.info('Restricted to testable grades. {} student records after restricting'.format(
        len(fountas_pinnell_roster_data)
    ))
    return fountas_pinnell_roster_data

def write_fountas_pinnell_rosters_local(
    fountas_pinnell_roster_data,
    base_directory,
    subdirectory='fountas_pinnell_rosters',
    filename_stem='fountas_pinnell_roster',
    filename_suffix=None

):
    wf_core_data.rosters.shared_functions.write_rosters_local(
        roster_data=fountas_pinnell_roster_data,
        base_directory=base_directory,
        subdirectory=subdirectory,
        filename_stem=filename_stem,
        filename_suffix=filename_suffix

    )
