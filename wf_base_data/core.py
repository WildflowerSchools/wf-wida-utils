import uuid

def generate_student_id():
    uuid_object = uuid.uuid4()
    student_id = uuid_object.int & int('FFFFFFFF', 16)
    return student_id
