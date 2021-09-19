import wf_core_data.utils
import pandas as pd
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

def fetch_fastbridge_results_local_directory_and_extract_test_events(
    dir_path,
    rollover_month=7,
    rollover_day=31
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
    test_events = fetch_fastbridge_results_local_files_and_extract_test_events(
        paths=paths,
        rollover_month=rollover_month,
        rollover_day=rollover_day
    )
    return test_events

def fetch_fastbridge_results_local_files_and_extract_test_events(
    paths,
    rollover_month=7,
    rollover_day=31
):
    parsed_results_list=list()
    for path in paths:
        results = fetch_fastbridge_results_local_file(
            path=path
        )
        test_events = extract_test_events(
            results=results,
            rollover_month=rollover_month,
            rollover_day=rollover_day
        )
        parsed_results_list.append(test_events)
    test_events = pd.concat(
        parsed_results_list,
        ignore_index=True
    )
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
    return test_events

def fetch_fastbridge_results_local_file_and_extract_test_events(
    path,
    rollover_month=7,
    rollover_day=31
):
    results = fetch_fastbridge_results_local_file(
        path=path
    )
    test_events = extract_test_events(
        results=results,
        rollover_month=rollover_month,
        rollover_day=rollover_day
    )
    return test_events

def fetch_fastbridge_results_local_file(
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

def extract_test_events(
    results,
    rollover_month=7,
    rollover_day=31
):
    test_events = (
        pd.melt(
            results,
            id_vars=['Assessment', 'FAST ID'],
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
            'term',
            'test',
            'subtest',
            'fast_id',
            'metric',
            'value'
        ])
        .pivot(
            index=[
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
    test_events['school_year'] = test_events['test_date'].apply(wf_core_data.utils.infer_school_year)
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
    return test_events
