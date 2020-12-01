from wf_rdbms.database_pandas import DatabasePandas, TablePandas, FieldPandas
from wf_core_data.google_sheets import ingest_student_data_google_sheet
from wf_core_data.transparent_classroom import TransparentClassroomClient
import pandas as pd
from collections import OrderedDict
import datetime
import uuid
import logging

logger = logging.getLogger(__name__)

# DATABASE_SCHEMA = {
#     'students': OrderedDict([
#         ('student_id_wf', {'key': True, 'type': 'integer'})
#     ]),
#     'transparent_classroom_students': OrderedDict([
#         ('school_id_tc', {'key': True, 'type': 'integer'}),
#         ('student_id_tc', {'key': True, 'type': 'integer'}),
#         ('student_id_wf', {'type': 'integer'})
#     ]),
#     'transparent_classroom_student_data_history': OrderedDict([
#         ('school_id_tc', {'key': True, 'type': 'integer'}),
#         ('student_id_tc', {'key': True, 'type': 'integer'}),
#         ('pull_datetime', {'key': True, 'type': 'datetime'}),
#         ('student_first_name', {'type': 'string'}),
#         ('student_middle_name', {'type': 'string'}),
#         ('student_last_name', {'type': 'string'}),
#         ('student_birth_date', {'type': 'date'}),
#         ('student_gender', {'type': 'string'}),
#         ('student_ethnicity', {'type': 'list'}),
#         ('student_dominant_language', {'type': 'string'}),
#         ('student_household_income', {'type': 'string'}),
#         ('student_grade', {'type': 'string'}),
#         ('student_classroom_ids', {'type': 'list'}),
#         ('student_program', {'type': 'string'}),
#         ('student_hours_string', {'type': 'string'}),
#         ('student_id_tc_alt', {'type': 'string'}),
#         ('student_allergies', {'type': 'string'}),
#         ('student_parent_ids', {'type': 'list'}),
#         ('student_approved_adults_string', {'type': 'string'}),
#         ('student_emergency_contacts_string', {'type': 'string'}),
#         ('student_notes', {'type': 'string'}),
#         ('student_last_day', {'type': 'date'}),
#         ('student_exit_reason', {'type': 'string'}),
#         ('student_exit_survey_id', {'type': 'integer'}),
#         ('student_exit_notes', {'type': 'string'})
#     ])
# }

class WildflowerDatabasePandas(DatabasePandas):
    def __init__(self):
        super().__init__(
            name = 'wf_core_data',
            tables = [
                TablePandas(
                    name='students',
                    fields= [
                        FieldPandas(name='student_id_wf', type='integer')
                    ],
                    primary_key=['student_id_wf']
                ),
                TablePandas(
                    name='transparent_classroom_students',
                    fields= [
                        FieldPandas(name='school_id_tc',type='integer'),
                        FieldPandas(name='student_id_tc', type='integer'),
                        FieldPandas(name='student_id_wf', type='integer')
                    ],
                    primary_key=['school_id_tc', 'student_id_tc']
                ),
                TablePandas(
                    name='transparent_classroom_student_data_history',
                    fields= [
                        FieldPandas(name='school_id_tc', type='integer'),
                        FieldPandas(name='student_id_tc', type='integer'),
                        FieldPandas(name='pull_datetime', type='datetime'),
                        FieldPandas(name='student_first_name', type='string'),
                        FieldPandas(name='student_middle_name', type='string'),
                        FieldPandas(name='student_last_name', type='string'),
                        FieldPandas(name='student_birth_date', type='date'),
                        FieldPandas(name='student_gender', type='string'),
                        FieldPandas(name='student_ethnicity', type='list'),
                        FieldPandas(name='student_dominant_language', type='string'),
                        FieldPandas(name='student_household_income', type='string'),
                        FieldPandas(name='student_grade', type='string'),
                        FieldPandas(name='student_classroom_ids', type='list'),
                        FieldPandas(name='student_program', type='string'),
                        FieldPandas(name='student_hours_string', type='string'),
                        FieldPandas(name='student_id_tc_alt', type='string'),
                        FieldPandas(name='student_allergies', type='string'),
                        FieldPandas(name='student_parent_ids', type='list'),
                        FieldPandas(name='student_approved_adults_string', type='string'),
                        FieldPandas(name='student_emergency_contacts_string', type='string'),
                        FieldPandas(name='student_notes', type='string'),
                        FieldPandas(name='student_last_day', type='date'),
                        FieldPandas(name='student_exit_reason', type='string'),
                        FieldPandas(name='student_exit_survey_id', type='integer'),
                        FieldPandas(name='student_exit_notes', type='string')
                    ],
                    primary_key=['school_id_tc', 'student_id_tc', 'pull_datetime']
                )
            ]
        )

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

    def pull_transparent_classroom_student_records_transparent_classroom(self, client=None):
        if client is None:
            client = client = TransparentClassroomClient()
        pull_datetime = datetime.datetime.now()
        records = client.fetch_student_data()
        records['pull_datetime'] = pull_datetime
        records['pull_datetime'] = pd.to_datetime(records['pull_datetime'])
        num_records = len(records)
        logger.info('Adding {} records from Transparent Classroom'.format(num_records))
        self.add_transparent_classroom_student_records(records)

    def add_transparent_classroom_student_records(self, records):
        self.tables['transparent_classroom_student_data_history'].create_records(records)
        self.add_new_student_ids()
        self.check_structure()

    def add_new_student_ids(self):
        logger.info('Adding Wildflower student IDs for new Transparent Classroom students')
        tc_student_ids = (
            self
            .tables['transparent_classroom_student_data_history']
            .index()
            .droplevel('pull_datetime')
            .drop_duplicates()
        )
        new_tc_student_ids = tc_student_ids.difference(self.tables['transparent_classroom_students'].index())
        num_new_tc_student_ids = len(new_tc_student_ids)
        if num_new_tc_student_ids == 0:
            logger.info('No new Transparent Classroom students found.')
            return
        logger.info('{} new Transparent Classroom students found. Generating Wildflower student IDs for them'.format(num_new_tc_student_ids))
        while True:
            student_id_records = pd.DataFrame(
                {'student_id_wf': [self.generate_student_id() for _ in new_tc_student_ids]},
                index=new_tc_student_ids
            )
            if (
                not student_id_records['student_id_wf'].duplicated().any() and
                len(set(student_id_records['student_id_wf']).intersection(set(self.tables['transparent_classroom_students'].dataframe()['student_id_wf']))) == 0
            ):
                break
            logger.info('Duplicates found among generated Wildflower student IDs. Regenerating.')
        logger.info('Adding new Wildflower student IDs to database.')
        self.tables['students'].create_records(student_id_records.reset_index(drop=True))
        self.tables['transparent_classroom_students'].create_records(student_id_records)

    def generate_student_id(self):
        uuid_object = uuid.uuid4()
        student_id = uuid_object.int & int('FFFFFFFF', 16)
        return student_id

    def check_structure(self):
        logger.info('Checking database structure')
        student_ids_wf_students_table = self.tables['students'].keys()
        student_ids_wf_tc_table = set(self.tables['transparent_classroom_students'].dataframe()['student_id_wf'])
        student_ids_wf_students_table_but_not_tc_table = student_ids_wf_students_table.difference(student_ids_wf_tc_table)
        if len(student_ids_wf_students_table_but_not_tc_table) > 0:
            raise ValueError('The following Wildflower student IDs are in the \'students\' table but not the \'transparent_classroom_students\' table: {}'.format(
                student_ids_wf_students_table_but_not_tc_table
            ))
        student_ids_wf_tc_table_but_not_students_table = student_ids_wf_tc_table.difference(student_ids_wf_students_table)
        if len(student_ids_wf_tc_table_but_not_students_table) > 0:
            raise ValueError('The following Wildflower student IDs are in the \'transparent_classroom_students\' but not the \'students\' table: {}'.format(
                student_ids_wf_tc_table_but_not_students_table
            ))
        student_ids_tc_students_table = self.tables['transparent_classroom_students'].keys()
        student_ids_tc_history_table = set(self.tables['transparent_classroom_student_data_history'].index().droplevel('pull_datetime'))
        student_ids_tc_students_table_but_not_history_table = student_ids_tc_students_table.difference(student_ids_tc_history_table)
        if len(student_ids_tc_students_table_but_not_history_table) > 0:
            raise ValueError('The following Transparent Classroom student IDs are in the \'transparent_classroom_students\' table but not the \'transparent_classroom_student_data_history\' table: {}'.format(
                student_ids_tc_students_table_but_not_history_table
            ))
        student_ids_tc_history_table_but_not_students_table = student_ids_tc_history_table.difference(student_ids_tc_students_table)
        if len(student_ids_tc_history_table_but_not_students_table) > 0:
            raise ValueError('The following Transparent Classroom student IDs are in the \'transparent_classroom_student_data_history\' table but not the \'transparent_classroom_students\' table: {}'.format(
                student_ids_tc_history_table_but_not_students_table
            ))
