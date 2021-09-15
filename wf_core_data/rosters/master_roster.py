import wf_core_data
import pandas as pd
import slugify
import datetime
import re
import os
import logging

logger = logging.getLogger(__name__)

def fetch_master_roster_data_and_write_local(
    base_directory,
    transparent_classroom_client=None,
    transparent_classroom_username=None,
    transparent_classroom_password=None,
    transparent_classroom_api_token=None,
    transparent_classroom_url_base='https://www.transparentclassroom.com/api/v1/',
    hub_info_path_stem = 'hub_info',
    legal_entity_info_path_stem = 'legal_entity_info',
    school_info_path_stem = 'school_info',
    classroom_info_path_stem = 'classroom_info',
    teacher_info_path_stem = 'teacher_info',
    ethnicity_info_path_stem = 'ethnicity_info',
    gender_map_path_stem = 'gender_map',
    ethnicity_map_path_stem = 'ethnicity_map',
    grade_map_path_stem = 'grade_map',
    subdirectory='master_rosters',
    filename_stem='master_roster',
    filename_suffix=None
):
    master_roster_data = fetch_master_roster_data(
        base_directory=base_directory,
        transparent_classroom_client=transparent_classroom_client,
        transparent_classroom_username=transparent_classroom_username,
        transparent_classroom_password=transparent_classroom_password,
        transparent_classroom_api_token=transparent_classroom_api_token,
        transparent_classroom_url_base=transparent_classroom_url_base,
        hub_info_path_stem=hub_info_path_stem,
        legal_entity_info_path_stem=legal_entity_info_path_stem,
        school_info_path_stem=school_info_path_stem,
        classroom_info_path_stem=classroom_info_path_stem,
        teacher_info_path_stem=teacher_info_path_stem,
        ethnicity_info_path_stem=ethnicity_info_path_stem,
        gender_map_path_stem=gender_map_path_stem,
        ethnicity_map_path_stem=ethnicity_map_path_stem,
        grade_map_path_stem=grade_map_path_stem
    )
    write_master_roster_data_local(
        master_roster_data=master_roster_data,
        base_directory=base_directory,
        subdirectory=subdirectory,
        filename_stem=filename_stem,
        filename_suffix=filename_suffix
    )

def fetch_master_roster_data(
    base_directory,
    transparent_classroom_client=None,
    transparent_classroom_username=None,
    transparent_classroom_password=None,
    transparent_classroom_api_token=None,
    transparent_classroom_url_base='https://www.transparentclassroom.com/api/v1/',
    hub_info_path_stem='hub_info',
    legal_entity_info_path_stem='legal_entity_info',
    school_info_path_stem='school_info',
    classroom_info_path_stem='classroom_info',
    teacher_info_path_stem='teacher_info',
    ethnicity_info_path_stem='ethnicity_info',
    gender_map_path_stem='gender_map',
    ethnicity_map_path_stem='ethnicity_map',
    grade_map_path_stem='grade_map'
):
    if transparent_classroom_client is None:
        transparent_classroom_client = wf_core_data.transparent_classroom.TransparentClassroomClient(
            username=transparent_classroom_username,
            password=transparent_classroom_password,
            api_token=transparent_classroom_api_token,
            url_base=transparent_classroom_url_base
        )
    pull_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
    ## Fetch target entity info
    ### Hubs
    logger.info('Fetching target hub info')
    hubs = pd.read_csv(
        os.path.join(
            base_directory,
            '.'.join([
                hub_info_path_stem,
                'csv'
            ])
        ),
        index_col='hub_short_name_wf'
    )
    logger.info('Fetched info for {} target hubs'.format(len(hubs)))
    ### Legal entities
    logger.info('Fetching target legal entity info')
    legal_entities = pd.read_csv(
        os.path.join(
            base_directory,
            '.'.join([
                legal_entity_info_path_stem,
                'csv'
            ])
        ),
        index_col='legal_entity_short_name_wf'
    )
    logger.info('Fetched info for {} target legal entities'.format(len(legal_entities)))
    ### Schools
    logger.info('Fetching target school info')
    schools = pd.read_csv(
        os.path.join(
            base_directory,
            '.'.join([
                school_info_path_stem,
                'csv'
            ])
        ),
        index_col='school_id_tc',
        dtype='object'
    )
    logger.info('Fetched info for {} target schools'.format(len(schools)))
    ### Classrooms
    logger.info('Fetching target classroom info')
    classrooms = pd.read_csv(
        os.path.join(
            base_directory,
            '.'.join([
                classroom_info_path_stem,
                'csv'
            ])
        ),
        index_col=['school_id_tc', 'classroom_id_tc']
    )
    logger.info('Fetched info for {} target classrooms'.format(len(classrooms)))
    ### Teachers
    logger.info('Fetching target teacher info')
    teachers = pd.read_csv(
        os.path.join(
            base_directory,
            '.'.join([
                teacher_info_path_stem,
                'csv'
            ])
        ),
        index_col=['school_id_tc', 'teacher_id_tc']
    )
    logger.info('Fetched info for {} target teachers'.format(len(teachers)))
    ### Ethnicities
    logger.info('Fetching target ethnicity info')
    ethnicities = pd.read_csv(
        os.path.join(
            base_directory,
            '.'.join([
                ethnicity_info_path_stem,
                'csv'
            ])
        ),
        index_col='ethnicity_short_name_wf'
    )
    logger.info('Fetched info for {} target ethnicities'.format(len(ethnicities)))
    ## Fetch mappings
    ### Ethnicity map
    logger.info('Fetching ethnicity mapping info')
    ethnicity_map = pd.read_csv(
        os.path.join(
            base_directory,
            '.'.join([
                ethnicity_map_path_stem,
                'csv'
            ])
        ),
        index_col='ethnicity_tc'
    )
    logger.info('Fetched mapping info for {} ethnicity values'.format(len(ethnicity_map)))
    ### Gender map
    logger.info('Fetching gender mapping info')
    gender_map = pd.read_csv(
        os.path.join(
            base_directory,
            '.'.join([
                gender_map_path_stem,
                'csv'
            ])
        ),
        index_col='gender_tc'
    )
    logger.info('Fetched mapping info for {} gender values'.format(len(gender_map)))
    ### Grade map
    logger.info('Fetching grade mapping info')
    grade_map = pd.read_csv(
        os.path.join(
            base_directory,
            '.'.join([
                grade_map_path_stem,
                'csv'
            ])
        ),
        index_col='grade_tc_slugified'
    )
    logger.info('Fetched mapping info for {} grade values'.format(len(grade_map)))
    ## Fetch data from Transparent Classroom
    ### School data
    logger.info('Fetching school data from Transparent Classroom')
    school_data  = (
        transparent_classroom_client.fetch_school_data(
            pull_datetime=pull_datetime,
            format='dataframe'
        )
        .join(
            schools,
            how='inner'
        )
    )
    logger.info('Fetched data for {} target schools'.format(len(school_data)))
    ### Classroom data
    logger.info('Fetching classroom data from Transparent Classroom')
    classroom_data = (
        transparent_classroom_client.fetch_classroom_data(
            school_ids=schools.index,
            pull_datetime=pull_datetime,
            format='dataframe'
        )
        .join(
            classrooms,
            how='inner'
        )
    )
    logger.info('Fetched data for {} target classrooms'.format(len(classroom_data)))
    ### Student classroom data
    logger.info('Fetching student classroom association data from Transparent Classroom')
    student_classroom_data = (
        transparent_classroom_client.fetch_student_classroom_data(
            school_ids=schools.index,
            session_data=None,
            pull_datetime=pull_datetime,
            only_current=True,
            format='dataframe'
        )
        .reset_index(level='session_id_tc', drop=True)
        .reset_index(level='classroom_id_tc')
    )
    logger.info('Fetched {} student classroom associations'.format(len(student_classroom_data)))
    ### Teacher data
    logger.info('Fetching teacher data from Transparent Classroom')
    teacher_data = (
        transparent_classroom_client.fetch_teacher_data(
            school_ids=schools.index,
            pull_datetime=pull_datetime,
            format='dataframe'
        )
        .rename_axis(index={'user_id_tc': 'teacher_id_tc'})
        .join(
            teachers,
            how='inner'
        )
        .rename(columns={
            'user_first_name_tc': 'teacher_first_name_tc',
            'user_last_name_tc': 'teacher_last_name_tc',
            'user_email_tc': 'teacher_email_tc'
        })
    )
    logger.info('Fetched data for {} target teachers'.format(len(teacher_data)))
    ### Student data
    logger.info('Fetching student data from Transparent Classroom')
    student_data, student_parent_data = transparent_classroom_client.fetch_student_data(
        school_ids=schools.index,
        pull_datetime=pull_datetime,
        only_current=True,
        format='dataframe'
    )
    logger.info('Fetched data for {} students'.format(len(student_data)))
    ## Join data tables
    logger.info('Joining student data with other data')
    master_roster_data = (
        student_data
        .join(
            student_classroom_data.drop(columns='pull_datetime'),
            how='left'
        )
        .join(
            classroom_data.drop(columns='pull_datetime'),
            how='inner',
            on=['school_id_tc', 'classroom_id_tc']
        )
        .join(
            school_data.drop(columns='pull_datetime'),
            how='left',
            on='school_id_tc'
        )
        .join(
            legal_entities,
            how='left',
            on='legal_entity_short_name_wf'
        )
        .join(
            hubs,
            how='left',
            on='hub_short_name_wf'
        )
        .join(
            teacher_data.drop(columns='pull_datetime'),
            how='left',
            on=['school_id_tc', 'teacher_id_tc']
        )
    )
    logger.info('Roster includes {} students after joining with other data'.format(len(master_roster_data)))
    ## Construct new fields
    ### School ZIP code
    logger.info('Constructing school ZIP code field')
    zip_re = re.compile(r' (?P<zip>[0-9]{5})')
    master_roster_data['school_zip_code_tc'] = master_roster_data['school_address_tc'].apply(
        lambda x: zip_re.search(x).group('zip') if zip_re.search(x) is not None else None
    )
    ### Normalized alternative student ID (typically state ID)
    logger.info('Constructing normalized alternative student ID field')
    master_roster_data['student_id_alt_normalized_tc'] = master_roster_data['student_id_alt_tc'].apply(
        wf_core_data.utils.extract_alphanumeric
    )
    ### Normalized gender
    logger.info('Constructing normalized gender field')
    gender_map_dict = gender_map['gender_wf'].to_dict()
    master_roster_data['student_gender_wf'] = master_roster_data['student_gender_tc'].apply(
        lambda x: gender_map_dict.get(x, None) if not pd.isna(x) else None
    )
    ### Normalized ethnicity
    logger.info('Constructing normalized ethnicity field')
    student_ethnicity_wf = (
        master_roster_data['student_ethnicity_tc']
        .dropna()
        .explode()
        .to_frame()
        .join(
            ethnicity_map,
            how='left',
            on='student_ethnicity_tc'
        )
        .dropna(subset=['ethnicity_short_name_wf'])
        .groupby(['school_id_tc', 'student_id_tc'])
        .aggregate(student_ethnicity_wf = ('ethnicity_short_name_wf', lambda x: sorted(list(set(x.to_list())))))
    )
    master_roster_data = (
        master_roster_data
        .join(
            student_ethnicity_wf,
            how='left'
        )
    )
    ### Normalized grade
    logger.info('Constructing normalized grade field')
    grade_map_dict = grade_map['grade_short_name_wf'].to_dict()
    def extract_grade_name(row):
        if not pd.isna(row['student_grade_tc']) and slugify.slugify(row['student_grade_tc'], separator='_') in grade_map_dict.keys():
            return grade_map_dict.get(slugify.slugify(row['student_grade_tc'], separator='_'))
        if not pd.isna(row['student_program_tc']) and slugify.slugify(row['student_program_tc'], separator='_') in grade_map_dict.keys():
            return grade_map_dict.get(slugify.slugify(row['student_program_tc'], separator='_'))
        if not pd.isna(row['classroom_name_tc']) and slugify.slugify(row['classroom_name_tc'], separator='_') in grade_map_dict.keys():
            return grade_map_dict.get(slugify.slugify(row['classroom_name_tc'], separator='_'))
        return None
    master_roster_data['student_grade_wf'] = master_roster_data.apply(extract_grade_name, axis=1)
    ## Rearrange rows and columns
    logger.info('Rearranging rows and columns')
    master_roster_data = (
        master_roster_data
        .reindex(columns=[
            'hub_short_name_wf',
            'hub_name_wf',
            'legal_entity_short_name_wf',
            'legal_entity_name_wf',
            'school_short_name_wf',
            'school_name_tc',
            'school_zip_code_tc',
            'school_state',
            'district_id_wida',
            'district_name_wida',
            'school_id_wida',
            'school_name_wida',
            'classroom_id_tc',
            'classroom_short_name_wf',
            'classroom_name_tc',
            'teacher_id_tc',
            'teacher_short_name_wf',
            'teacher_first_name_tc',
            'teacher_last_name_tc',
            'teacher_email_tc',
            'student_first_name_tc',
            'student_last_name_tc',
            'student_middle_name_tc',
            'student_birth_date_tc',
            'student_gender_wf',
            'student_gender_tc',
            'student_ethnicity_wf',
            'student_ethnicity_tc',
            'student_dominant_language_tc',
            'student_household_income_tc',
            'student_grade_wf',
            'student_grade_tc',
            'student_program_tc',
            'student_id_alt_tc',
            'student_id_alt_normalized_tc'
        ])
        .sort_values([
            'hub_short_name_wf',
            'legal_entity_short_name_wf',
            'school_short_name_wf',
            'classroom_short_name_wf',
            'student_last_name_tc',
            'student_first_name_tc',
        ])
    )
    return master_roster_data

def write_master_roster_data_local(
    master_roster_data,
    base_directory,
    subdirectory='master_rosters',
    filename_stem='master_roster',
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
    logger.info('Writing pickle file')
    master_roster_data.to_pickle(
        os.path.join(
            output_directory,
            '{}_{}.pkl'.format(
                filename_stem,
                filename_suffix
            )
        )
    )
    logger.info('Writing CSV file')
    master_roster_data.to_csv(
        os.path.join(
            output_directory,
            '{}_{}.csv'.format(
                filename_stem,
                filename_suffix
            )
        )
    )
