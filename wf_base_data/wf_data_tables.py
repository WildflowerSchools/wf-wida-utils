from .pandas_data_table import PandasDataTable

class StudentIDDataTablePandas(PandasDataTable):
    def __init__(self):
        super().__init__(
            key_column_names = [
                'tc_school_id',
                'tc_student_id'
            ],
            value_column_names = [
                'student_id'
            ]
        )

class TransparentClassroomStudentDataTablePandas(PandasDataTable):
    def __init__(self):
        super().__init__(
            key_column_names = [
                'tc_school_id',
                'tc_student_id',
                'pull_datetime'
            ],
            value_column_names = [
                'student_first_name',
                'student_last_name',
                'student_birth_date',
                'student_gender',
                'student_ethnicity_list',
                'student_dominant_language'
            ]
        )
