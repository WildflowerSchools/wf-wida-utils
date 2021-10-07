import wf_core_data.utils
import pandas as pd
import numpy as np
import inflection
import collections
import itertools
import copy
import os
import logging

logger = logging.getLogger(__name__)

TIME_FRAME_ID_VARIABLES = [
    'school_year',
    'term'
]

STUDENT_ID_VARIABLES = [
    'fast_id'
]

STUDENT_INFO_VARIABLES = [
    'local_id',
    'state_id',
    'first_name',
    'last_name',
    'gender',
    'birth_date',
    'race'
]

STUDENT_ASSIGNMENT_VARIABLES = [
    'school',
    'grade'
]

ASSESSMENT_ID_VARIABLES = [
    'test',
    'subtest'
]

RESULTS_VARIABLES = [
    'test_date',
    'percentile',
    'risk_level'
]

TERMS = (
    'Fall',
    'Winter',
    'Spring'
)

ASSESSMENTS = collections.OrderedDict((
    ('Early Reading English', (
        'Early Reading English',
        'Concepts of Print',
        'Decodable Words',
        'Letter Names',
        'Letter Sounds',
        'Nonsense Words',
        'Onset Sounds',
        'Oral Repetition',
        'Word Rhyming',
        'Sentence Reading',
        'Sight Words',
        'Word Blending',
        'Word Segmenting',
        'CBMR-English'
    )),
    ('Early Reading Spanish', (
        'Early Reading Spanish',
        'Concepts of Print Spanish',
        'Decodable Words Spanish',
        'Letter Names Spanish',
        'Letter Sounds Spanish',
        'Onset Sounds Spanish',
        'Oral Repetition Spanish',
        'Word Rhyming Spanish',
        'Sentence Reading Spanish',
        'Sight Word Spanishs',
        'Syllable Reading Spanish',
        'Word Blending Spanish',
        'Word Segmenting Spanish',
        'CBMR-Spanish'
    )),
    ('Early Math', (
        'Early Math',
        'Composing',
        'Counting Objects',
        'Decomposing ONE',
        'Decomposing KG',
        'Equal Partitioning',
        'Match Quantity',
        'Number Sequence ONE',
        'Number Sequence KG',
        'Numeral Identification ONE',
        'Numeral Identification KG',
        'Place Value',
        'QuantityDiscrimination Least',
        'Quantity Discrimination Most',
        'Subitizing',
        'Verbal Addition',
        'Verbal Subtraction',
        'Story Problems'
    ))
))

METRICS = (
    'Risk Level',
    'Percentile at Nation',
    'Final Date'
)

RISK_LEVELS=(
    'highRisk',
    'someRisk',
    'lowRisk'
)

FIELD_NAMES_LIST = list()
for test, subtests in ASSESSMENTS.items():
    for term in TERMS:
        for subtest in subtests:
            for metric in METRICS:
                FIELD_NAMES_LIST.append({
                    'test': test,
                    'term': term,
                    'subtest': subtest,
                    'metric': metric,
                    'field_name': ' '.join([
                        term,
                        subtest,
                        metric
                    ])
                })
FIELD_NAMES = pd.DataFrame(FIELD_NAMES_LIST)
FIELD_NAMES.set_index(['test', 'field_name'], inplace=True)

TEMP = FIELD_NAMES.reset_index()
TEST_DATE_FIELD_NAMES = (
    TEMP
    .loc[TEMP['metric'] == 'Final Date', 'field_name']
    .tolist()
)

TESTS = list(ASSESSMENTS.keys())

SUBTESTS = list(itertools.chain(*ASSESSMENTS.values()))

DEFAULT_MIN_GROWTH_DAYS = 60

DEFAULT_SCHOOL_YEAR_DURATION_MONTHS = 9

DEFAULT_ROLLOVER_MONTH = 7
DEFAULT_ROLLOVER_DAY = 31

def fetch_fastbridge_results_local_directory(
    dir_path,
    rollover_month=DEFAULT_ROLLOVER_MONTH,
    rollover_day=DEFAULT_ROLLOVER_DAY
):
    if not os.path.exists(dir_path):
        raise ValueError('Path \'{}\' not found'.format(dir_path))
    if not os.path.isdir(dir_path):
        raise ValueError('Object at \'{}\' is not a directory'.format(dir_path))
    paths = list()
    for directory_entry in os.listdir(dir_path):
        file_path = os.path.join(
            dir_path,
            directory_entry
        )
        if not os.path.isfile(file_path):
            continue
        file_extension = os.path.splitext(os.path.normpath(file_path))[1]
        if file_extension not in ['.csv', '.CSV']:
            continue
        paths.append(file_path)
    if len(paths) == 0:
        raise ValueError('No CSV files found in directory')
    results = fetch_fastbridge_results_local_files(
        paths=paths,
        rollover_month=rollover_month,
        rollover_day=rollover_day
    )
    return results

def fetch_fastbridge_results_local_files(
    paths,
    rollover_month=DEFAULT_ROLLOVER_MONTH,
    rollover_day=DEFAULT_ROLLOVER_DAY
):
    results_list=list()
    for path in paths:
        results_file = fetch_fastbridge_results_local_file(
            path=path,
            school_year=None,
            rollover_month=rollover_month,
            rollover_day=rollover_day
        )
        results_list.append(results_file)
    results = pd.concat(
        results_list,
        ignore_index=True
    )
    return results

def fetch_fastbridge_results_local_file(
    path,
    school_year=None,
    rollover_month=DEFAULT_ROLLOVER_MONTH,
    rollover_day=DEFAULT_ROLLOVER_DAY
):
    if not os.path.exists(path):
        raise ValueError('File \'{}\' not found'.format(path))
    if not os.path.isfile(path):
        raise ValueError('Object at \'{}\' is not a file'.format(path))
    results = pd.read_csv(
        path,
        dtype='object'
    )
    if school_year is None:
        school_year=infer_school_year_from_results(
            results,
            rollover_month=rollover_month,
            rollover_day=rollover_day
        )
    results.insert(0, 'school_year', school_year)
    return results

def parse_fastbridge_results(
    results
):
    test_events = extract_test_events(results)
    student_info, student_info_changes = extract_student_info(results)
    student_assignments = extract_student_assignments(results)
    return test_events, student_info, student_info_changes, student_assignments

def extract_test_events(
    results
):
    test_events = (
        pd.melt(
            results,
            id_vars=['school_year', 'Assessment', 'FAST ID'],
            value_vars=set(FIELD_NAMES.index.get_level_values('field_name')).intersection(results.columns),
            var_name='field_name',
            value_name='value'
        )
        .rename(columns={
            'Assessment': 'test',
            'FAST ID': 'fast_id'
        })
        .dropna(subset=['value'])
        .join(
            FIELD_NAMES,
            how='left',
            on=['test', 'field_name']
        )
        .reindex(columns=[
            'school_year',
            'term',
            'test',
            'subtest',
            'fast_id',
            'metric',
            'value'
        ])
        .pivot(
            index=[
                'school_year',
                'term',
                'test',
                'subtest',
                'fast_id'
            ],
            columns='metric',
            values='value'
        )
        .reindex(columns=[
            'Final Date',
            'Percentile at Nation',
            'Risk Level'
        ])
        .rename(columns={
            'Final Date': 'test_date',
            'Percentile at Nation': 'percentile',
            'Risk Level': 'risk_level'
        })
        .reset_index()
    )
    test_events.columns.name = None
    test_events['term'] = pd.Categorical(
        test_events['term'],
        categories=TERMS,
        ordered=True
    )
    test_events['test'] = pd.Categorical(
        test_events['test'],
        categories=ASSESSMENTS.keys(),
        ordered=True
    )
    test_events['subtest'] = pd.Categorical(
        test_events['subtest'],
        categories=itertools.chain(*ASSESSMENTS.values()),
        ordered=True
    )
    test_events['test_date'] = test_events['test_date'].apply(wf_core_data.utils.to_date)
    test_events['percentile'] = pd.to_numeric(test_events['percentile']).astype('float')
    test_events['risk_level'] = pd.Categorical(
        test_events['risk_level'],
        categories=RISK_LEVELS,
        ordered=True
    )
    test_events = test_events.reindex(columns=[
        'school_year',
        'term',
        'test',
        'subtest',
        'fast_id',
        'test_date',
        'percentile',
        'risk_level'
    ])
    test_events.sort_values(
        [
            'school_year',
            'term',
            'test',
            'subtest',
            'test_date'
        ],
        inplace=True,
        ignore_index=True
    )
    test_events.set_index(
        [
                'school_year',
                'term',
                'test',
                'subtest',
                'fast_id'
        ],
        inplace=True
    )
    return test_events

def extract_student_info(
    results
):
    student_info = (
        results
        .rename(columns= {
            'FAST ID': 'fast_id',
            'Local ID': 'local_id',
            'State ID': 'state_id',
            'First Name': 'first_name',
            'Last Name': 'last_name',
            'Gender': 'gender',
            'DOB': 'birth_date',
            'Race': 'race'
        })
    )
    student_info['birth_date'] = student_info['birth_date'].apply(wf_core_data.utils.to_date)
    student_info = (
        student_info
        .reindex(columns=list(itertools.chain(
            STUDENT_ID_VARIABLES,
            ['school_year'],
            STUDENT_INFO_VARIABLES
        )))
        .drop_duplicates()
    )
    student_info_changes = (
        student_info
        .groupby(STUDENT_ID_VARIABLES)
        .filter(lambda group: len(group.drop_duplicates(subset=STUDENT_INFO_VARIABLES)) > 1)
    )
    student_info = (
        student_info
        .sort_values('school_year')
        .drop(columns='school_year')
        .groupby(STUDENT_ID_VARIABLES)
        .tail(1)
        .set_index(STUDENT_ID_VARIABLES)
        .sort_index()
    )
    return student_info, student_info_changes

def extract_student_assignments(
    results
):
    student_assignments = (
        results
        .rename(columns= {
            'FAST ID': 'fast_id',
            'School': 'school',
            'Grade': 'grade'
        })
    )
    student_assignments = (
        student_assignments
        .reindex(columns=list(itertools.chain(
            STUDENT_ID_VARIABLES,
            ['school_year'],
            STUDENT_ASSIGNMENT_VARIABLES
        )))
        .drop_duplicates()
        .set_index(list(itertools.chain(
            STUDENT_ID_VARIABLES,
            ['school_year']
        )))
        .sort_index()
    )
    return student_assignments

def summarize_by_test(
    test_events,
    student_assignments,
    grouping_variables = [
        'school_year',
        'school',
        'test',
        'subtest',
        'term'
    ],
    filter_dict=None,
    select_dict=None
):
    tests = (
        test_events
        .join(
            student_assignments,
            how='left',
            on=[
                'fast_id',
                'school_year'
            ]
        )
        .groupby(grouping_variables)
        .agg(
            num_test_events=('test_date', 'count'),
            num_valid_risk_level=('risk_level', 'count'),
            num_valid_percentile=('percentile', 'count')
        )
    )
    tests = tests.loc[tests['num_test_events'] > 0].copy()
    if filter_dict is not None:
        tests = wf_core_data.utils.filter_dataframe(
            dataframe=tests,
            filter_dict=filter_dict
        )
    if select_dict is not None:
        tests = wf_core_data.utils.select_from_dataframe(
            dataframe=tests,
            select_dict=select_dict
        )
    return tests

def summarize_by_student(
    test_events,
    student_info,
    student_assignments,
    new_time_index=['school_year'],
    min_growth_days=DEFAULT_MIN_GROWTH_DAYS,
    school_year_duration_months=DEFAULT_SCHOOL_YEAR_DURATION_MONTHS,
    filter_dict=None,
    select_dict=None
):
    new_index_variables = list(itertools.chain(
        new_time_index,
        ASSESSMENT_ID_VARIABLES,
        STUDENT_ID_VARIABLES
    ))
    unstack_variables = copy.deepcopy(TIME_FRAME_ID_VARIABLES)
    for new_time_index_variable in new_time_index:
        unstack_variables.remove(new_time_index_variable)
    students = (
        test_events
        .unstack(unstack_variables)
    )
    students.columns = ['_'.join([inflection.underscore(variable_name) for variable_name in x]) for x in students.columns]
    underlying_data_columns = list(students.columns)
    goals = (
        test_events
        .dropna(subset=['risk_level'])
        .sort_values('test_date')
        .groupby(new_index_variables)
        .agg(
            risk_level_starting_date=('test_date', lambda x: x.dropna().iloc[0]),
            risk_level_ending_date=('test_date', lambda x: x.dropna().iloc[-1]),
            starting_risk_level=('risk_level', lambda x: x.dropna().iloc[0]),
            ending_risk_level=('risk_level', lambda x: x.dropna().iloc[-1]),
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
            goals,
            how='left'
        )
        .join(
            percentiles,
            how='left'
        )
    )
    students['met_attainment_goal'] = (students['ending_risk_level'] == 'lowRisk')
    students['met_growth_goal'] = (students['starting_risk_level'] == 'highRisk') & (students['ending_risk_level'] != 'highRisk')
    students['met_goal'] = students['met_attainment_goal'] | students['met_growth_goal']
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
    students['percentile_growth_per_school_year'] = 365.25*(school_year_duration_months/12)*students['percentile_growth']/students['percentile_num_days']
    students = students.join(
        student_info,
        how='left',
        on=STUDENT_ID_VARIABLES
    )
    if 'school_year' in new_time_index:
        student_assignment_time_index = ['school_year']
    else:
        student_assignment_time_index = []
    latest_student_assignments = (
        student_assignments
        .reset_index()
        .sort_values(['school_year'])
        .groupby(list(itertools.chain(
            STUDENT_ID_VARIABLES,
            student_assignment_time_index
        )))
        .tail(1)
        .set_index(list(itertools.chain(
            STUDENT_ID_VARIABLES,
            student_assignment_time_index
        )))
    )
    students = students.join(
        latest_student_assignments,
        how='left',
        on=list(latest_student_assignments.index.names)
    )
    students = students.reindex(columns=list(itertools.chain(
        STUDENT_INFO_VARIABLES,
        STUDENT_ASSIGNMENT_VARIABLES,
        underlying_data_columns,
        [
            'risk_level_starting_date',
            'risk_level_ending_date',
            'risk_level_num_days',
            'starting_risk_level',
            'ending_risk_level',
            'met_growth_goal',
            'met_attainment_goal',
            'met_goal',
            'percentile_starting_date',
            'percentile_ending_date',
            'percentile_num_days',
            'starting_percentile',
            'ending_percentile',
            'percentile_growth',
            'percentile_growth_per_school_year',
        ]
    )))
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

def summarize_by_group(
    students,
    grouping_variables=[
        'school_year',
        'school',
        'test',
        'subtest'
    ],
    filter_dict=None,
    select_dict=None
):
    groups = (
        students
        .reset_index()
        .groupby(grouping_variables)
        .agg(
            num_valid_test_results=('fast_id', 'count'),
            num_valid_goal_info=('met_goal', 'count'),
            num_met_growth_goal=('met_growth_goal', 'sum'),
            num_met_attainment_goal=('met_attainment_goal', 'sum'),
            num_met_goal=('met_goal', 'sum'),
            num_valid_percentile_growth=('percentile_growth', 'count'),
            mean_percentile_growth=('percentile_growth', 'mean'),
            mean_percentile_growth_per_school_year=('percentile_growth_per_school_year', 'mean')
        )
        .dropna(how='all')
    )
    groups = groups.loc[groups['num_valid_test_results'] > 0].copy()
    groups['frac_met_growth_goal'] = groups['num_met_growth_goal'].astype('float')/groups['num_valid_goal_info'].astype('float')
    groups['frac_met_attainment_goal'] = groups['num_met_attainment_goal'].astype('float')/groups['num_valid_goal_info'].astype('float')
    groups['frac_met_goal'] = groups['num_met_goal'].astype('float')/groups['num_valid_goal_info'].astype('float')
    groups = groups.reindex(columns=[
        'num_valid_test_results',
        'num_valid_goal_info',
        'frac_met_growth_goal',
        'frac_met_attainment_goal',
        'frac_met_goal',
        'num_valid_percentile_growth',
        'mean_percentile_growth',
        'mean_percentile_growth_per_school_year'
    ])
    if filter_dict is not None:
        groups = wf_core_data.utils.filter_dataframe(
            dataframe=groups,
            filter_dict=filter_dict
        )
    if select_dict is not None:
        groups = wf_core_data.utils.select_from_dataframe(
            dataframe=groups,
            select_dict=select_dict
        )
    return groups

def infer_school_year_from_results(
    results,
    rollover_month=DEFAULT_ROLLOVER_MONTH,
    rollover_day=DEFAULT_ROLLOVER_DAY
):
    school_years = list()
    for field_name in TEST_DATE_FIELD_NAMES:
        if field_name not in results.columns:
            continue
        school_years_subtest = (
            results[field_name]
            .apply(lambda x: wf_core_data.utils.infer_school_year(
                wf_core_data.utils.to_date(x),
                rollover_month=rollover_month,
                rollover_day=rollover_day
            ))
            .dropna()
            .unique()
            .tolist()
        )
        school_years.extend(school_years_subtest)
    school_years = np.unique(school_years).tolist()
    if len(school_years) == 0:
        raise ValueError('No parseable test dates found')
    if len(school_years) > 1:
        raise ValueError('Data contains multiple school years')
    school_year = school_years[0]
    return school_year
