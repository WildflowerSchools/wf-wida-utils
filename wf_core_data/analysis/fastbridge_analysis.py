import wf_core_data.utils
import pandas as pd
import numpy as np
import collections
import itertools
import os
import logging

logger = logging.getLogger(__name__)

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

DEFAULT_ROLLOVER_MONTH = 7
DEFAULT_ROLLOVER_DAY = 31

def fetch_and_parse_fastbridge_results_local_directory(
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
    test_events, student_info = fetch_and_parse_fastbridge_results_local_files(
        paths=paths,
        rollover_month=rollover_month,
        rollover_day=rollover_day
    )
    return test_events, student_info

def fetch_and_parse_fastbridge_results_local_files(
    paths,
    rollover_month=DEFAULT_ROLLOVER_MONTH,
    rollover_day=DEFAULT_ROLLOVER_DAY
):
    test_events_list=list()
    student_info_list=list()
    for path in paths:
        results = fetch_fastbridge_results_local_file(
            path=path,
            school_year=None,
            rollover_month=rollover_month,
            rollover_day=rollover_day
        )
        test_events_file = extract_test_events(
            results=results
        )
        student_info_file=extract_student_info(
            results=results
        )
        test_events_list.append(test_events_file)
        student_info_list.append(student_info_file)
    test_events = pd.concat(
        test_events_list
    )
    student_info = pd.concat(
        student_info_list
    )
    test_events.sort_index(inplace=True)
    student_info = (
        student_info
        .reset_index()
        .drop_duplicates()
        .set_index(['fast_id', 'school_year'])
        .sort_index()
    )
    if student_info.index.duplicated().any():
        raise ValueError('Files contain conflicting info for the same school year and FAST ID')
    return test_events, student_info

def fetch_fastbridge_results_local_file_and_extract_test_events(
    path,
    school_year=None,
    rollover_month=DEFAULT_ROLLOVER_MONTH,
    rollover_day=DEFAULT_ROLLOVER_DAY
):
    results = fetch_fastbridge_results_local_file(
        path=path,
        school_year=school_year,
        rollover_month=rollover_month,
        rollover_day=rollover_day
    )
    test_events = extract_test_events(
        results=results
    )
    student_info = extract_student_info(
        results=results
    )
    test_events.sort_index(inplace=True)
    student_info = (
        student_info
        .reset_index()
        .drop_duplicates()
        .set_index('fast_id', 'school_year')
        .sort_index()
    )
    if student_info.index.duplicated().any():
        raise ValueError('Files contain conflicting info for the same school year and FAST ID')
    return test_events, student_info

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
            'Race': 'race',
            'School': 'school',
            'Grade': 'grade'
        })
    )
    student_info['birth_date'] = student_info['birth_date'].apply(wf_core_data.utils.to_date)
    student_info.set_index(
        ['fast_id', 'school_year'],
        inplace=True
    )
    student_info = student_info.reindex(columns=[
        'local_id',
        'state_id',
        'first_name',
        'last_name',
        'gender',
        'birth_date',
        'race',
        'school',
        'grade'
    ])
    return student_info

def summarize_by_student_test_school_year(
    test_events,
    student_info
):
    students_tests = (
        test_events
        .reset_index()
        .pivot(
            index=['school_year', 'test', 'subtest', 'fast_id'],
            columns = 'term',
            values=['test_date', 'risk_level', 'percentile']
        )
    )
    students_tests.columns = ['{}_{}'.format(x[0], x[1].lower()) for x in students_tests.columns]
    goals = (
        test_events
        .dropna(subset=['risk_level'])
        .reorder_levels(['school_year', 'test', 'subtest', 'fast_id', 'term'])
        .sort_index()
        .groupby(['school_year', 'test', 'subtest', 'fast_id'])
        .agg(
            starting_risk_level=('risk_level', lambda x: x.dropna().iloc[0]),
            ending_risk_level=('risk_level', lambda x: x.dropna().iloc[-1]),
        )
    )
    percentiles = (
        test_events
        .dropna(subset=['percentile'])
        .reorder_levels(['school_year', 'test', 'subtest', 'fast_id', 'term'])
        .sort_index()
        .groupby(['school_year', 'test', 'subtest', 'fast_id'])
        .agg(
            starting_date=('test_date', lambda x: x.dropna().iloc[0]),
            ending_date=('test_date', lambda x: x.dropna().iloc[-1]),
            starting_percentile=('percentile', lambda x: x.dropna().iloc[0]),
            ending_percentile=('percentile', lambda x: x.dropna().iloc[-1]),
        )
    )
    students_tests = (
    students_tests
        .join(
            goals,
            how='left'
        )
        .join(
            percentiles,
            how='left'
        )
    )
    students_tests['met_attainment_goal'] = (students_tests['ending_risk_level'] == 'lowRisk')
    students_tests['met_growth_goal'] = (students_tests['starting_risk_level'] == 'highRisk') & (students_tests['ending_risk_level'] != 'highRisk')
    students_tests['met_goal'] = students_tests['met_attainment_goal'] | students_tests['met_growth_goal']
    students_tests['num_days'] = (
        np.subtract(
            students_tests['ending_date'],
            students_tests['starting_date']
        )
        .apply(lambda x: x.days)
    )
    students_tests['percentile_growth'] = np.subtract(
        students_tests['ending_percentile'],
        students_tests['starting_percentile']
    )
    students_tests.loc[students_tests['num_days'] == 0, 'percentile_growth'] = np.nan
    students_tests['percentile_growth_per_year'] = (
        students_tests
        .apply(
            lambda row: row['percentile_growth']/(row['num_days']/365.25) if not pd.isna(row['percentile_growth']) and row['num_days'] > 0 else np.nan,
            axis=1
        )
    )
    students_tests = students_tests.join(
        student_info,
        how='left',
        on=['fast_id', 'school_year']
    )
    students_tests = students_tests.reindex(columns=[
        'local_id',
        'state_id',
        'first_name',
        'last_name',
        'gender',
        'birth_date',
        'race',
        'school',
        'grade',
        'test_date_fall',
        'test_date_winter',
        'test_date_spring',
        'risk_level_fall',
        'risk_level_winter',
        'risk_level_spring',
        'met_growth_goal',
        'met_attainment_goal',
        'met_goal',
        'percentile_fall',
        'percentile_winter',
        'percentile_spring',
        'percentile_growth',
        'percentile_growth_per_year'
    ])
    return students_tests

def summarize_by_student_group(
    students_tests,
    grouping_variables=[
        'school_year',
        'school',
        'test',
        'subtest'
    ]
):
    student_groups = (
        students_tests
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
            num_valid_percentile_growth_per_year=('percentile_growth_per_year', 'count'),
            mean_percentile_growth_per_year=('percentile_growth_per_year', 'mean')
        )
        .dropna(how='all')
    )
    student_groups = student_groups.loc[student_groups['num_valid_test_results'] > 0].copy()
    student_groups['frac_met_growth_goal'] = student_groups['num_met_growth_goal'].astype('float')/student_groups['num_valid_goal_info'].astype('float')
    student_groups['frac_met_attainment_goal'] = student_groups['num_met_attainment_goal'].astype('float')/student_groups['num_valid_goal_info'].astype('float')
    student_groups['frac_met_goal'] = student_groups['num_met_goal'].astype('float')/student_groups['num_valid_goal_info'].astype('float')
    student_groups = student_groups.reindex(columns=[
        'num_valid_test_results',
        'num_valid_goal_info',
        'frac_met_growth_goal',
        'frac_met_attainment_goal',
        'frac_met_goal',
        'num_valid_percentile_growth',
        'mean_percentile_growth',
        'num_valid_percentile_growth_per_year',
        'mean_percentile_growth_per_year'
    ])
    return student_groups

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
        # print(school_years_subtest)
        school_years.extend(school_years_subtest)
    # print(school_years)
    school_years = np.unique(school_years).tolist()
    # print(school_years)
    if len(school_years) == 0:
        raise ValueError('No parseable test dates found')
    if len(school_years) > 1:
        raise ValueError('Data contains multiple school years')
    school_year = school_years[0]
    # print(school_year)
    return school_year
