import pandas as pd
import logging

logger = logging.getLogger(__name__)

class Database:
    """
    Class to define a generic database for Wildflower base data
    """
    def __init__(
        self,
        schema
    ):
        """
        Contructor for Database

        The schema input should be a dictionary with data table names as keys
        and column names as values in the format {'DATA_TABLE_NAME':
        {'key_column_names': [...], 'value_column_names': [...]}, ...}

        Parameters:
            schema (dict of dict): Names and column names for data tables
        """
        for data_table_name, data_table_column_names in schema.items():
            logger.info('Initializing data table {} with key column names {} and value column names {}'.format(
                data_table_name,
                data_table_column_names['key_column_names'],
                data_table_column_names['value_column_names']
            ))
        self.schema = schema
        self._init(schema)

    def _init(self, schema):
        raise NotImplementedError('Method must be implemented by child class')

class DataTable:
    """
    Class to define a generic table for Wildflower base data
    """
    def __init__(
        self,
        key_column_names,
        value_column_names
    ):
        """
        Contructor for DataTable

        Parameters:
            key_column_names (list of str): Column names for key columns
            value_column_names (list of str): Column names for value columns
        """
        logger.info('Initializing data table with key column names {} and value column names {}'.format(
            key_column_names,
            value_column_names
        ))
        self.key_column_names = list(key_column_names)
        self.value_column_names = list(value_column_names)
        self._init(key_column_names, value_column_names)

    def _init(self, key_column_names, value_column_names):
        raise NotImplementedError('Method must be implemented by child class')

    def create_records(self, records):
        """
        Create records in data table.

        Input should be a Pandas data frame or an object easily convertible to a
        Pandas data frame via the pandas.DataFrame() constructor (e.g., dict of
        lists, list of dicts, etc.).

        Parameters:
            records (DataFrame): Records to create

        Returns:
            (list of tuples): Key values for created records
        """
        records = self.normalize_records(records)
        logger.info('Attempting to create {} records'.format(len(records)))
        return_key_values = self._create_records(records)
        return return_key_values

    def _create_records(self, records):
        raise NotImplementedError('Method must be implemented by child class')

    def update_records(self, records):
        """
        Update records in data table.

        Input should be a Pandas data frame or an object easily convertible to a
        Pandas data frame via the pandas.DataFrame() constructor (e.g., dict of
        lists, list of dicts, etc.).

        Parameters:
            records (DataFrame): Records to update

        Returns:
            (list of tuples): Key values for updated records
        """
        records = self.normalize_records(records)
        logger.info('Attempting to update {} records'.format(len(records)))
        return_key_values = self._update_records(records)
        return return_key_values

    def _update_records(self, records):
        raise NotImplementedError('Method must be implemented by child class')

    def delete_records(self, records):
        """
        Delete records in data table.

        Input should be a Pandas data frame or an object easily convertible to a
        Pandas data frame via the pandas.DataFrame() constructor (e.g., dict of
        lists, list of dicts, etc.). All columns other than key columns are ignored

        Parameters:
            records (DataFrame): Records to delete

        Returns:
            (list of tuples): Key values for deleted records
        """
        records = self.normalize_records(records)
        logger.info('Attempting to delete {} records'.format(len(records)))
        return_key_values = self._delete_records(records)
        return return_key_values

    def _delete_records(self, records):
        raise NotImplementedError('Method must be implemented by child class')

    def dataframe(self):
        """
        Returns a Pandas dataframe containing the data in the data table.

        Returns:
            (DataFrame): Pandas dataframe containing the data in the data table
        """
        dataframe = self._dataframe()
        return dataframe

    def _dataframe(self):
        raise NotImplementedError('Method must be implemented by child class')

    def index(self):
        """
        Returns a Pandas index containing all key values in the data table.

        Returns:
            (Index): Pandas index containing all key values in the data table
        """
        index = self._index()
        return index

    def _index(self):
        raise NotImplementedError('Method must be implemented by child class')

    def normalize_records(self, records, normalize_value_columns=True):
        """
        Normalize records to conform to structure of data table.

        Records object should be a Pandas data frame or an object easily
        convertible to a Pandas data frame via the pandas.DataFrame()
        constructor (e.g., dict of lists, list of dicts, etc.)

        Parameters:
            records(DataFrame): Records to normalize

        Returns:
        (DataFrame): Normalized records
        """
        drop=False
        if records.index.names == [None]:
            drop=True
        records = pd.DataFrame(records).reset_index(drop=drop)
        if not set(self.key_column_names).issubset(set(records.columns)):
            raise ValueError('Key columns {} missing from specified records'.format(
                set(self.key_column_names) - set(records.columns)
            ))
        records.set_index(self.key_column_names, inplace=True)
        if not normalize_value_columns:
            return records
        input_value_column_names = list(records.columns)
        spurious_value_column_names = set(input_value_column_names) - set(self.value_column_names)
        if len(spurious_value_column_names) > 0:
            logger.info('Specified records contain value column names not in data table: {}. These columns will be ignored'.format(
                spurious_value_column_names
            ))
        missing_value_column_names = set(self.value_column_names) - set(input_value_column_names)
        if len(missing_value_column_names) > 0:
            logger.info('Data table contains value column names not found in specified records: {}. These values will be empty'.format(
                missing_value_column_names
            ))
        records = records.reindex(columns=self.value_column_names)
        return records
