import wf_core_data.rosters.shared_constants
import wf_core_data.rosters.shared_functions
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

WIDA_TARGET_COLUMN_NAMES = [
    'State Name Abbreviation',
    'District Name',
    'District Number',
    'School Name',
    'School Number',
    'Student Last Name',
    'Student First Name',
    'Student Middle Name',
    'Birth Date',
    'Gender',
    'State Student ID',
    'District Student ID',
    'Grade',
    'Ethnicity - Hispanic/Latino',
    'Race - American Indian/Alaskan Native',
    'Race - Asian',
    'Race - Black/African American',
    'Race - Pacific Islander/Hawaiian',
    'Race - White',
    'Native Language',
    'Date First Enrolled US School',
    'Length of Time in LEP/ELL Program',
    'Title III Status',
    'Migrant',
    'IEP Status',
    '504 Plan',
    'Primary Disability',
    'Secondary Disability',
    'LIEP Classification',
    'LIEP - Parental Refusal',
    'LIEP - Optional Data',
    'Accommodation - MC',
    'Accommodation - RA',
    'Accommodation - ES',
    'Accommodation - LP',
    'Accommodation - BR',
    'Accommodation - SD',
    'Accommodation - IR',
    'Accommodation - RP',
    'Accommodation - SR',
    'Accommodation - WD',
    'Accommodation - RD',
    'Accommodation - NS',
    'Accommodation - EM',
    'State Defined Optional Data',
    'District Defined Optional Data',
    'Mode of Administration',
    'Paper Tier',
    'Alternate ACCESS for ELLs Tester',
    'Student Type',
    'Additional field to be used by a state if needed'
]

WIDA_GENDER_MAP = {
    'M': 'M',
    'F': 'F',
    'unmatched_value': None,
    'na_value': None
}

WIDA_GRADE_NAME_MAP = {
    'EC': 'EC',
    'PK': 'PK',
    'PK_3': 'PK',
    'PK_4': 'PK',
    'K': '00',
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

WIDA_TESTABLE_GRADES = [
    '00'
]

def create_wida_roster_and_write_locally(
    base_directory,
    filename_suffix,
    master_roster_subdirectory='master_rosters',
    master_roster_filename_stem='master_roster',
    wida_roster_subdirectory='wida_rosters',
    wida_roster_filename_stem='wida_roster'
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
    wida_roster_data = wf_core_data.create_wida_roster(
        master_roster_data=master_roster_data
    )
    wf_core_data.write_wida_rosters_local(
        wida_roster_data=wida_roster_data,
        base_directory=base_directory,
        subdirectory=wida_roster_subdirectory,
        filename_stem=wida_roster_filename_stem,
        filename_suffix=filename_suffix
    )

def create_wida_roster(
    master_roster_data
):
    ## Rename fields
    logger.info('Renaming fields')
    wida_roster_data = (
        master_roster_data
        .rename(columns = {
            'school_state': 'State Name Abbreviation',
            'district_name_wida': 'District Name',
            'district_id_wida': 'District Number',
            'school_name_wida': 'School Name',
            'school_id_wida': 'School Number',
            'student_last_name_tc': 'Student Last Name',
            'student_first_name_tc': 'Student First Name',
            'student_middle_name_tc': 'Student Middle Name',
            'student_id_alt_normalized_tc': 'State Student ID'
        })
    )
    ## Create new fields
    ### Student ID
    logger.info('Creating district student ID field')
    wida_roster_data['District Student ID'] = wida_roster_data.apply(
        lambda row: '{}-{}'.format(
            row.name[0],
            row.name[1]
        ),
        axis=1
    )
    ### Student birth date
    logger.info('Creating birth date field')
    wida_roster_data['Birth Date'] = wida_roster_data['student_birth_date_tc'].apply(
        lambda x: x.strftime('%m/%d/%Y')
    )
    ### Student gender
    logger.info('Creating gender field')
    wida_roster_data['Gender'] = wida_roster_data['student_gender_wf'].apply(
        lambda x: WIDA_GENDER_MAP.get(x, WIDA_GENDER_MAP.get('unmatched_value')) if pd.notna(x) else WIDA_GENDER_MAP.get('na_value')
    )
    ### Grade
    logger.info('Creating grade field')
    wida_roster_data['Grade'] = wida_roster_data['student_grade_wf'].apply(
        lambda x: WIDA_GRADE_NAME_MAP.get(x, WIDA_GRADE_NAME_MAP.get('unmatched_value')) if pd.notna(x) else WIDA_GRADE_NAME_MAP.get('na_value')
    )
    ### Student ethnicity
    logger.info('Creating ethnicity fields')
    wida_roster_data['Ethnicity - Hispanic/Latino'] = wida_roster_data['student_ethnicity_wf'].apply(lambda ethnicity_list:
        'Y' if isinstance(ethnicity_list, list) and 'hispanic' in ethnicity_list else None
    )
    wida_roster_data['Race - American Indian/Alaskan Native'] = wida_roster_data['student_ethnicity_wf'].apply(lambda ethnicity_list:
        'Y' if isinstance(ethnicity_list, list) and 'native_american' in ethnicity_list else None
    )
    wida_roster_data['Race - Asian'] = wida_roster_data['student_ethnicity_wf'].apply(lambda ethnicity_list:
        'Y' if isinstance(ethnicity_list, list) and 'asian_american' in ethnicity_list else None
    )
    wida_roster_data['Race - Black/African American'] = wida_roster_data['student_ethnicity_wf'].apply(lambda ethnicity_list:
        'Y' if isinstance(ethnicity_list, list) and 'african_american' in ethnicity_list else None
    )
    wida_roster_data['Race - Pacific Islander/Hawaiian'] = wida_roster_data['student_ethnicity_wf'].apply(lambda ethnicity_list:
        'Y' if isinstance(ethnicity_list, list) and 'pacific_islander' in ethnicity_list else None
    )
    wida_roster_data['Race - White'] = wida_roster_data['student_ethnicity_wf'].apply(lambda ethnicity_list:
        'Y' if isinstance(ethnicity_list, list) and 'white' in ethnicity_list else None
    )
    ## Arrange columns and rows
    logger.info('Rearranging columns and rows')
    wida_roster_data = (
        wida_roster_data
        .reindex(columns=(
            wf_core_data.rosters.shared_constants.GROUPING_COLUMN_NAMES +
            WIDA_TARGET_COLUMN_NAMES
        ))
        .sort_values(
            wf_core_data.rosters.shared_constants.GROUPING_COLUMN_NAMES +
            ['Grade', 'Student First Name', 'Student Last Name']
        )
    )
    ## Create output
    logger.info('Restriction to testable grades. {} student records before restricting'.format(
        len(wida_roster_data)
    ))
    wida_roster_data = (
        wida_roster_data
        .loc[wida_roster_data['Grade'].isin(WIDA_TESTABLE_GRADES)]
        .copy()
        .reset_index(drop=True)
        .astype('object')
    )
    logger.info('Restricted to testable grades. {} student records after restricting'.format(
        len(wida_roster_data)
    ))
    return wida_roster_data

def write_wida_rosters_local(
    wida_roster_data,
    base_directory,
    subdirectory='wida_rosters',
    filename_stem='wida_roster',
    filename_suffix=None

):
    wf_core_data.rosters.shared_functions.write_rosters_local(
        roster_data=wida_roster_data,
        base_directory=base_directory,
        subdirectory=subdirectory,
        filename_stem=filename_stem,
        filename_suffix=filename_suffix

    )
