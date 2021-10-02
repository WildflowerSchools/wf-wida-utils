import wf_core_data.utils
import pandas as pd
import numpy as np
import inflection
import collections
import itertools
import os
import logging

logger = logging.getLogger(__name__)

TERMS_NWEA = (
    'Fall',
    'Winter',
    'Spring'
)

# ASSESSMENTS = collections.OrderedDict((
#     ('Early Reading English', (
#         'Early Reading English',
#         'Concepts of Print',
#         'Decodable Words',
#         'Letter Names',
#         'Letter Sounds',
#         'Nonsense Words',
#         'Onset Sounds',
#         'Oral Repetition',
#         'Word Rhyming',
#         'Sentence Reading',
#         'Sight Words',
#         'Word Blending',
#         'Word Segmenting',
#         'CBMR-English'
#     )),
#     ('Early Reading Spanish', (
#         'Early Reading Spanish',
#         'Concepts of Print Spanish',
#         'Decodable Words Spanish',
#         'Letter Names Spanish',
#         'Letter Sounds Spanish',
#         'Onset Sounds Spanish',
#         'Oral Repetition Spanish',
#         'Word Rhyming Spanish',
#         'Sentence Reading Spanish',
#         'Sight Word Spanishs',
#         'Syllable Reading Spanish',
#         'Word Blending Spanish',
#         'Word Segmenting Spanish',
#         'CBMR-Spanish'
#     )),
#     ('Early Math', (
#         'Early Math',
#         'Composing',
#         'Counting Objects',
#         'Decomposing ONE',
#         'Decomposing KG',
#         'Equal Partitioning',
#         'Match Quantity',
#         'Number Sequence ONE',
#         'Number Sequence KG',
#         'Numeral Identification ONE',
#         'Numeral Identification KG',
#         'Place Value',
#         'QuantityDiscrimination Least',
#         'Quantity Discrimination Most',
#         'Subitizing',
#         'Verbal Addition',
#         'Verbal Subtraction',
#         'Story Problems'
#     ))
# ))

# METRICS = (
#     'Risk Level',
#     'Percentile at Nation',
#     'Final Date'
# )
#
# RISK_LEVELS=(
#     'highRisk',
#     'someRisk',
#     'lowRisk'
# )

# FIELD_NAMES_LIST = list()
# for test, subtests in ASSESSMENTS.items():
#     for term in TERMS:
#         for subtest in subtests:
#             for metric in METRICS:
#                 FIELD_NAMES_LIST.append({
#                     'test': test,
#                     'term': term,
#                     'subtest': subtest,
#                     'metric': metric,
#                     'field_name': ' '.join([
#                         term,
#                         subtest,
#                         metric
#                     ])
#                 })
# FIELD_NAMES = pd.DataFrame(FIELD_NAMES_LIST)
# FIELD_NAMES.set_index(['test', 'field_name'], inplace=True)
#
# TEMP = FIELD_NAMES.reset_index()
# TEST_DATE_FIELD_NAMES = (
#     TEMP
#     .loc[TEMP['metric'] == 'Final Date', 'field_name']
#     .tolist()
# )
#

def fetch_results_local_directory_nwea(
    path,
    file_extensions=['.csv', '.CSV']
):
    if not os.path.exists(path):
        raise ValueError('Path \'{}\' not found'.format(path))
    if not os.path.isdir(path):
        raise ValueError('Object at \'{}\' is not a directory'.format(path))
    paths = list()
    for directory_entry in os.listdir(path):
        file_path = os.path.join(
            path,
            directory_entry
        )
        if not os.path.isfile(file_path):
            continue
        file_extension = os.path.splitext(os.path.normpath(file_path))[1]
        if file_extension not in file_extensions:
            continue
        paths.append(file_path)
    if len(paths) == 0:
        raise ValueError('No files of type {} found in directory'.format(file_extensions))
    results = fetch_results_local_files_nwea(paths)
    return results

def fetch_results_local_files_nwea(
    paths
):
    results_list = list()
    for path in paths:
        results_file = fetch_results_local_file_nwea(
            path=path
        )
        results_list.append(results_file)
    results = pd.concat(results_list)
    return results

def fetch_results_local_file_nwea(
    path
):
    if not os.path.exists(path):
        raise ValueError('File \'{}\' not found'.format(path))
    if not os.path.isfile(path):
        raise ValueError('Object at \'{}\' is not a file'.format(path))
    results = pd.read_csv(
        path,
        dtype='object'
    )
    return results

def parse_results_nwea(results):
    test_events = extract_test_events_nwea(results)
    student_info, student_info_changes = extract_student_info_nwea(results)
    student_assignments = extract_student_assignments_nwea(results)
    return test_events, student_info, student_info_changes, student_assignments

def extract_test_events_nwea(
    results
):
    test_events = (
        results
        .rename(columns={
            'TermTested': 'term_school_year',
            'DistrictName': 'legal_entity',
            'Subject': 'subject',
            'Course': 'course',
            'StudentID': 'student_id_nwea',
            'TestDate': 'test_date',
            'StartRIT': 'rit_score',
            'StartRITSEM': 'rit_score_sem',
            'StartPercentile': 'percentile',
            'StartPercentileSE': 'percentile_se'
        })
    )
    test_events['term'] = test_events['term_school_year'].apply(lambda x: x.split(' ')[0])
    test_events['school_year'] = test_events['term_school_year'].apply(lambda x: x.split(' ')[1])
    test_events['term'] = pd.Categorical(
        test_events['term'],
        categories=TERMS_NWEA,
        ordered=True
    )
    test_events['test_date'] = test_events['test_date'].apply(wf_core_data.utils.to_date)
    test_events['rit_score'] = pd.to_numeric(test_events['rit_score']).astype('float')
    test_events['rit_score_sem'] = pd.to_numeric(test_events['rit_score_sem']).astype('float')
    test_events['percentile'] = pd.to_numeric(test_events['percentile']).astype('float')
    test_events['percentile_se'] = pd.to_numeric(test_events['percentile_se'].replace('<1', 0.5)).astype('float')
    test_events = test_events.reindex(columns=[
        'school_year',
        'term',
        'subject',
        'course',
        'legal_entity',
        'student_id_nwea',
        'test_date',
        'rit_score',
        'rit_score_sem',
        'percentile',
        'percentile_se'
    ])
    test_events.set_index(
        [
            'school_year',
            'term',
            'subject',
            'course',
            'legal_entity',
            'student_id_nwea'
        ],
        inplace=True
    )
    test_events.sort_index(inplace=True)
    return test_events

def extract_student_info_nwea(
    results
):
    student_info = (
        results
        .rename(columns= {
            'TermTested': 'term_school_year',
            'DistrictName': 'legal_entity',
            'StudentID': 'student_id_nwea',
            'StudentLastName': 'last_name',
            'StudentFirstName': 'first_name'
        })
    )
    student_info['term'] = student_info['term_school_year'].apply(lambda x: x.split(' ')[0])
    student_info['school_year'] = student_info['term_school_year'].apply(lambda x: x.split(' ')[1])
    student_info = (
        student_info
        .reindex(columns=[
            'legal_entity',
            'student_id_nwea',
            'school_year',
            'term',
            'first_name',
            'last_name'
        ])
        .drop_duplicates()
    )
    student_info_changes = (
        student_info
        .groupby(['legal_entity', 'student_id_nwea'])
        .filter(lambda group: len(group.drop_duplicates(subset=['first_name', 'last_name'])) > 1)
    )
    student_info = (
        student_info
        .sort_values(['school_year', 'term'])
        .drop(columns=['school_year', 'term'])
        .groupby((['legal_entity', 'student_id_nwea']))
        .tail(1)
        .set_index(['legal_entity', 'student_id_nwea'])
        .sort_index()
    )
    return student_info, student_info_changes

def extract_student_assignments_nwea(
    results
):
    student_assignments = (
        results
        .rename(columns= {
            'TermTested': 'term_school_year',
            'DistrictName': 'legal_entity',
            'StudentID': 'student_id_nwea',
            'SchoolName': 'school',
            'Teacher': 'teacher_last_first',
            'ClassName': 'classroom',
            'StudentGrade': 'grade'
        })
    )
    student_assignments['term'] = student_assignments['term_school_year'].apply(lambda x: x.split(' ')[0])
    student_assignments['school_year'] = student_assignments['term_school_year'].apply(lambda x: x.split(' ')[1])
    student_assignments = (
        student_assignments
        .reindex(columns=[
            'legal_entity',
            'student_id_nwea',
            'school_year',
            'term',
            'school',
            'teacher_last_first',
            'classroom',
            'grade'
        ])
        .drop_duplicates()
        .set_index(['legal_entity', 'student_id_nwea', 'school_year', 'term'])
        .sort_index()
    )
    return student_assignments

def summarize_by_student_nwea(
    test_events,
    student_info,
    min_growth_days=60,
    unstack_variables=['term'],
    filter_dict=None,
    select_dict=None
):
    new_index_variables = list(test_events.index.names)
    for unstack_variable in unstack_variables:
        new_index_variables.remove(unstack_variable)
    students = (
        test_events
        .unstack(unstack_variables)
    )
    students.columns = ['_'.join([inflection.underscore(variable_name) for variable_name in x]) for x in students.columns]
    underlying_data_columns = list(students.columns)
    rit_scores = (
        test_events
        .dropna(subset=['rit_score'])
        .sort_values('test_date')
        .groupby(new_index_variables)
        .agg(
            rit_score_starting_date=('test_date', lambda x: x.iloc[0]),
            rit_score_ending_date=('test_date', lambda x: x.iloc[-1]),
            starting_rit_score=('rit_score', lambda x: x.iloc[0]),
            ending_rit_score=('rit_score', lambda x: x.iloc[-1]),
        )
    )
    percentiles = (
        test_events
        .dropna(subset=['percentile'])
        .sort_values('test_date')
        .groupby(new_index_variables)
        .agg(
            percentile_starting_date=('test_date', lambda x: x.dropna().iloc[0]),
            percentile_ending_date=('test_date', lambda x: x.dropna().iloc[-1]),
            starting_percentile=('percentile', lambda x: x.dropna().iloc[0]),
            ending_percentile=('percentile', lambda x: x.dropna().iloc[-1]),
        )
    )
    students = (
    students
        .join(
            rit_scores,
            how='left'
        )
        .join(
            percentiles,
            how='left'
        )
    )
    students['rit_score_num_days'] = (
        np.subtract(
            students['rit_score_ending_date'],
            students['rit_score_starting_date']
        )
        .apply(lambda x: x.days)
    )
    students['rit_score_growth'] = np.subtract(
        students['ending_rit_score'],
        students['starting_rit_score']
    )
    students.loc[students['rit_score_num_days'] < min_growth_days, 'rit_score_growth'] = np.nan
    students['percentile_num_days'] = (
        np.subtract(
            students['percentile_ending_date'],
            students['percentile_starting_date']
        )
        .apply(lambda x: x.days)
    )
    students['percentile_growth'] = np.subtract(
        students['ending_percentile'],
        students['starting_percentile']
    )
    students.loc[students['percentile_num_days'] < min_growth_days, 'percentile_growth'] = np.nan
    students = students.join(
        student_info,
        how='left',
        on=['legal_entity', 'student_id_nwea']
    )
    students = students.reindex(columns=
        [
            'first_name',
            'last_name'
        ] +
        underlying_data_columns +
        [
            'rit_score_growth',
            'percentile_growth'
        ]
    )
    if filter_dict is not None:
        students = wf_core_data.utils.filter_dataframe(
            dataframe=students,
            filter_dict=filter_dict
        )
    if select_dict is not None:
        students = wf_core_data.utils.select_from_dataframe(
            dataframe=students,
            select_dict=select_dict
        )
    return students

# def summarize_by_group(
#     students,
#     grouping_variables=[
#         'school_year',
#         'school',
#         'test',
#         'subtest'
#     ],
#     filter_dict=None,
#     select_dict=None
# ):
#     groups = (
#         students
#         .reset_index()
#         .groupby(grouping_variables)
#         .agg(
#             num_valid_test_results=('student_id_nwea', 'count'),
#             num_valid_goal_info=('met_goal', 'count'),
#             num_met_growth_goal=('met_growth_goal', 'sum'),
#             num_met_attainment_goal=('met_attainment_goal', 'sum'),
#             num_met_goal=('met_goal', 'sum'),
#             num_valid_percentile_growth=('percentile_growth', 'count'),
#             mean_percentile_growth=('percentile_growth', 'mean'),
#             num_valid_percentile_growth_per_year=('percentile_growth_per_year', 'count'),
#             mean_percentile_growth_per_year=('percentile_growth_per_year', 'mean')
#         )
#         .dropna(how='all')
#     )
#     groups = groups.loc[groups['num_valid_test_results'] > 0].copy()
#     groups['frac_met_growth_goal'] = groups['num_met_growth_goal'].astype('float')/groups['num_valid_goal_info'].astype('float')
#     groups['frac_met_attainment_goal'] = groups['num_met_attainment_goal'].astype('float')/groups['num_valid_goal_info'].astype('float')
#     groups['frac_met_goal'] = groups['num_met_goal'].astype('float')/groups['num_valid_goal_info'].astype('float')
#     groups = groups.reindex(columns=[
#         'num_valid_test_results',
#         'num_valid_goal_info',
#         'frac_met_growth_goal',
#         'frac_met_attainment_goal',
#         'frac_met_goal',
#         'num_valid_percentile_growth',
#         'mean_percentile_growth',
#         'num_valid_percentile_growth_per_year',
#         'mean_percentile_growth_per_year'
#     ])
#     if filter_dict is not None:
#         groups = wf_core_data.utils.filter_dataframe(
#             dataframe=groups,
#             filter_dict=filter_dict
#         )
#     if select_dict is not None:
#         groups = wf_core_data.utils.select_from_dataframe(
#             dataframe=groups,
#             select_dict=select_dict
#         )
#     return groups
#
# def infer_school_year_from_results(
#     results,
#     rollover_month=DEFAULT_ROLLOVER_MONTH,
#     rollover_day=DEFAULT_ROLLOVER_DAY
# ):
#     school_years = list()
#     for field_name in TEST_DATE_FIELD_NAMES:
#         if field_name not in results.columns:
#             continue
#         school_years_subtest = (
#             results[field_name]
#             .apply(lambda x: wf_core_data.utils.infer_school_year(
#                 wf_core_data.utils.to_date(x),
#                 rollover_month=rollover_month,
#                 rollover_day=rollover_day
#             ))
#             .dropna()
#             .unique()
#             .tolist()
#         )
#         # print(school_years_subtest)
#         school_years.extend(school_years_subtest)
#     # print(school_years)
#     school_years = np.unique(school_years).tolist()
#     # print(school_years)
#     if len(school_years) == 0:
#         raise ValueError('No parseable test dates found')
#     if len(school_years) > 1:
#         raise ValueError('Data contains multiple school years')
#     school_year = school_years[0]
#     # print(school_year)
#     return school_year
