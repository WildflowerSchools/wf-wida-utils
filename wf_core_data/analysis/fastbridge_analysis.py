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

def fetch_and_parse_fastbridge_results_local_directory(
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
    parsed_results = fetch_and_parse_fastbridge_results_local_files(
        paths=paths,
        rollover_month=rollover_month,
        rollover_day=rollover_day
    )
    return parsed_results

def fetch_and_parse_fastbridge_results_local_files(
    paths,
    rollover_month=7,
    rollover_day=31
):
    parsed_results_list=list()
    for path in paths:
        results = fetch_fastbridge_results_local_file(
            path=path
        )
        parsed_results = parse_fastbridge_results(
            results=results,
            rollover_month=rollover_month,
            rollover_day=rollover_day
        )
        parsed_results_list.append(parsed_results)
    parsed_results = pd.concat(
        parsed_results_list,
        ignore_index=True
    )
    parsed_results.sort_values(
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
    return parsed_results

def fetch_and_parse_fastbridge_results_local_file(
    path,
    rollover_month=7,
    rollover_day=31
):
    results = fetch_fastbridge_results_local_file(
        path=path
    )
    parsed_results = parse_fastbridge_results(
        results=results,
        rollover_month=rollover_month,
        rollover_day=rollover_day
    )
    return parsed_results

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

def parse_fastbridge_results(
    results,
    rollover_month=7,
    rollover_day=31
):
    parsed_results = (
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
    parsed_results.columns.name = None
    parsed_results['term'] = pd.Categorical(
        parsed_results['term'],
        categories=TERMS,
        ordered=True
    )
    parsed_results['test'] = pd.Categorical(
        parsed_results['test'],
        categories=ASSESSMENTS.keys(),
        ordered=True
    )
    parsed_results['subtest'] = pd.Categorical(
        parsed_results['subtest'],
        categories=itertools.chain(*ASSESSMENTS.values()),
        ordered=True
    )
    parsed_results['test_date'] = parsed_results['test_date'].apply(wf_core_data.utils.to_date)
    parsed_results['percentile'] = pd.to_numeric(parsed_results['percentile']).astype('float')
    parsed_results['risk_level'] = pd.Categorical(
        parsed_results['risk_level'],
        categories=RISK_LEVELS,
        ordered=True
    )
    parsed_results['school_year'] = parsed_results['test_date'].apply(wf_core_data.utils.infer_school_year)
    parsed_results = parsed_results.reindex(columns=[
        'school_year',
        'term',
        'test',
        'subtest',
        'fast_id',
        'test_date',
        'percentile',
        'risk_level'
    ])
    parsed_results.sort_values(
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
    return parsed_results
