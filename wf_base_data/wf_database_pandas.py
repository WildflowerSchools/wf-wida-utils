from .database_pandas import DatabasePandas
from .google_sheets import ingest_student_data_google_sheet
import pandas as pd
import uuid
import logging

logger = logging.getLogger(__name__)

SCHEMA = {
    'student_ids': {
        'key_column_names': [
            'tc_school_id',
            'tc_student_id'
        ],
        'value_column_names': [
            'student_id'
        ]
    },
    'transparent_classroom_student_data': {
            'key_column_names': [
                'tc_school_id',
                'tc_student_id',
                'pull_datetime'
            ],
            'value_column_names': [
                'student_first_name',
                'student_last_name',
                'student_birth_date',
                'student_gender',
                'student_ethnicity_list',
                'student_dominant_language'
            ]
    }
}

class WildflowerDatabasePandas(DatabasePandas):
    def __init__(self):
        super().__init__(schema=SCHEMA)

    def pull_transparent_classroom_student_records_google_sheets(
        self,
        sheet_metadata
    ):
        for sheet_metadatum in sheet_metadata:
            self.pull_transparent_classroom_student_records_google_sheet(
                sheet_metadatum['sheet_id'],
                sheet_metadatum['pull_date']
            )

    def pull_transparent_classroom_student_records_google_sheet(
        self,
        sheet_id,
        pull_date
    ):
        records = ingest_student_data_google_sheet(sheet_id, pull_date)
        num_records = len(records)
        logger.info('Adding {} records from Google Sheet'.format(num_records))
        self.add_transparent_classroom_student_records(records)

    def add_transparent_classroom_student_records(self, records):
        self.data_tables['transparent_classroom_student_data'].create_records(records)
        self.add_new_student_ids()

    def add_new_student_ids(self):
        logger.info('Adding Wildflower student IDs for new Transparent Classroom students')
        tc_student_ids = (
            self
            .data_tables['transparent_classroom_student_data']
            .index()
            .droplevel('pull_datetime')
            .drop_duplicates()
        )
        new_tc_student_ids = tc_student_ids.difference(self.data_tables['student_ids'].index())
        num_new_tc_student_ids = len(new_tc_student_ids)
        if num_new_tc_student_ids == 0:
            logger.info('No new Transparent Classroom students found.')
            return
        logger.info('{} new Transparent Classroom students found. Generating Wildflower student IDs for them'.format(num_new_tc_student_ids))
        while True:
            student_id_records = pd.DataFrame(
                {'student_id': [self.generate_student_id() for _ in new_tc_student_ids]},
                index=new_tc_student_ids
            )
            if (
                not student_id_records['student_id'].duplicated().any() and
                len(set(student_id_records['student_id']).intersection(set(self.data_tables['student_ids'].dataframe()['student_id']))) == 0
            ):
                break
            logger.info('Duplicates found among generated Wildflower student IDs. Regenerating.')
        logger.info('Adding new Wildflower student IDs to database.')
        self.data_tables['student_ids'].create_records(student_id_records)

    def generate_student_id(self):
        uuid_object = uuid.uuid4()
        student_id = uuid_object.int & int('FFFFFFFF', 16)
        return student_id
