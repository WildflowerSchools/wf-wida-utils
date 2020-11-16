
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

# class StudentIDDataTablePandas(DataTablePandas):
#     def __init__(self):
#         super().__init__(
#             key_column_names = [
#                 'tc_school_id',
#                 'tc_student_id'
#             ],
#             value_column_names = [
#                 'student_id'
#             ]
#         )
#
# class TransparentClassroomStudentDataTablePandas(DataTablePandas):
#     def __init__(self):
#         super().__init__(
#             key_column_names = [
#                 'tc_school_id',
#                 'tc_student_id',
#                 'pull_datetime'
#             ],
#             value_column_names = [
#                 'student_first_name',
#                 'student_last_name',
#                 'student_birth_date',
#                 'student_gender',
#                 'student_ethnicity_list',
#                 'student_dominant_language'
#             ]
#         )
