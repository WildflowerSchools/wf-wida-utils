import pandas as pd

def to_datetime(object):
    try:
        datetime = pd.to_datetime(object, utc=True).to_pydatetime()
        if pd.isnull(datetime):
            date = None
    except:
        datetime = None
    return datetime

def to_date(object):
    try:
        date = pd.to_datetime(object).date()
        if pd.isnull(date):
            date = None
    except:
        date = None
    return date

def to_singleton(object):
    try:
        num_elements = len(object)
        if num_elements > 1:
            raise ValueError('More than one element in object. Conversion to singleton failed')
        if num_elements == 0:
            return None
        return object[0]
    except:
        return object

def to_boolean(object):
    if isinstance(object, bool):
        return object
    if isinstance(object, str):
        if object in ['True', 'TRUE', 'T']:
            return True
        if object in ['False', 'FALSE', 'F']:
            return False
        return None
    if isinstance(object, int):
        if object == 1:
            return True
        if object == 0:
            return False
        return None
    return None

def extract_alphanumeric(object):
    if pd.isna(object):
        return None
    try:
        object_string = str(object)
    except:
        return None
    alphanumeric_string = ''.join(ch for ch in object_string if ch.isalnum())
    return alphanumeric_string
