import wf_core_data.rosters.shared_constants
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

def write_rosters_local(
    roster_data,
    base_directory,
    subdirectory,
    filename_stem,
    filename_suffix=None

):
    if filename_suffix is None:
        filename_suffix = datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y%m%d')
    logger.info('Filename suffix is {}'.format(filename_suffix))
    output_directory_base = os.path.join(
        base_directory,
        subdirectory,
        '{}_{}'.format(
            filename_stem,
            filename_suffix
        )
    )
    output_directory_csv = os.path.join(
        output_directory_base,
        'csv'
    )
    output_directory_pickle = os.path.join(
        output_directory_base,
        'pickle'
    )
    output_directory_excel = os.path.join(
        output_directory_base,
        'excel'
    )
    os.makedirs(output_directory_csv, exist_ok=True)
    os.makedirs(output_directory_pickle, exist_ok=True)
    os.makedirs(output_directory_excel, exist_ok=True)
    output = (
        roster_data
        .drop(columns=wf_core_data.rosters.shared_constants.GROUPING_COLUMN_NAMES)
    )
    filename = '{}_{}'.format(
        filename_stem,
        filename_suffix
    )
    output.to_csv(
        os.path.join(
            output_directory_csv,
            '{}.csv'.format(
                filename
            )
        ),
        index = False
    )
    output.to_pickle(
        os.path.join(
            output_directory_pickle,
            '{}.pkl'.format(
                filename
            )
        )
    )
    output.to_excel(
        os.path.join(
            output_directory_excel,
            '{}.xlsx'.format(
                filename
            )
        )
    )
    for legal_entity_short_name, roster_df_group in roster_data.groupby('legal_entity_short_name_wf'):
        output = (
            roster_df_group
            .drop(columns=wf_core_data.rosters.shared_constants.GROUPING_COLUMN_NAMES)
        )
        filename = '{}_{}_{}'.format(
            filename_stem,
            legal_entity_short_name,
            filename_suffix
        )
        output.to_csv(
            os.path.join(
                output_directory_csv,
                '{}.csv'.format(
                    filename
                )
            ),
            index = False
        )
        output.to_pickle(
            os.path.join(
                output_directory_pickle,
                '{}.pkl'.format(
                    filename
                )
            )
        )
        output.to_excel(
            os.path.join(
                output_directory_excel,
                '{}.xlsx'.format(
                    filename
                )
            )
        )
    for school_short_name, roster_df_group in roster_data.groupby('school_short_name_wf'):
        output = (
            roster_df_group
            .drop(columns=wf_core_data.rosters.shared_constants.GROUPING_COLUMN_NAMES)
        )
        filename = '{}_{}_{}'.format(
            filename_stem,
            school_short_name,
            filename_suffix
        )
        output.to_csv(
            os.path.join(
                output_directory_csv,
                '{}.csv'.format(
                    filename
                )
            ),
            index = False
        )
        output.to_pickle(
            os.path.join(
                output_directory_pickle,
                '{}.pkl'.format(
                    filename
                )
            )
        )
        output.to_excel(
            os.path.join(
                output_directory_excel,
                '{}.xlsx'.format(
                    filename
                )
            ),
            index=False
        )
    for classroom_short_name, roster_df_group in roster_data.groupby('classroom_short_name_wf'):
        output = (
            roster_df_group
            .drop(columns=wf_core_data.rosters.shared_constants.GROUPING_COLUMN_NAMES)
        )
        filename = '{}_{}_{}'.format(
            filename_stem,
            classroom_short_name,
            filename_suffix
        )
        output.to_csv(
            os.path.join(
                output_directory_csv,
                '{}.csv'.format(
                    filename
                )
            ),
            index = False
        )
        output.to_pickle(
            os.path.join(
                output_directory_pickle,
                '{}.pkl'.format(
                    filename
                )
            )
        )
        output.to_excel(
            os.path.join(
                output_directory_excel,
                '{}.xlsx'.format(
                    filename
                )
            ),
            index=False
        )
