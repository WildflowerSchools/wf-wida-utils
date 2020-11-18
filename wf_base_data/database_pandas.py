from .database import Database, DataTable
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class DatabasePandas(Database):
    """
    Class to define a Pandas database for Wildflower base data
    """
    def _init(self, schema):
        self.data_tables = dict()
        for data_table_name, data_table_column_names in self.schema.items():
            self.data_tables[data_table_name] = DataTablePandas(
                key_column_names = data_table_column_names['key_column_names'],
                value_column_names = data_table_column_names['value_column_names']
            )

class DataTablePandas(DataTable):
    """
    Class to define a Pandas table for Wildflower base data
    """
    def _init(self, key_column_names, value_column_names):
        self.df = pd.DataFrame(columns = key_column_names + value_column_names)
        self.df.set_index(key_column_names, inplace=True)

    def _create_records(self, records):
        already_existing_records = self.df.index.intersection(records.index)
        if len(already_existing_records) > 0:
            logger.info('Of {} specified records, {} have key values that are already in the data table. Ignoring these.'.format(
                len(records),
                len(already_existing_records)
            ))
            records.drop(already_existing_records, inplace=True)
        self.df = pd.concat((self.df, records))
        self.df.sort_index(inplace=True)
        return_key_values = list(records.index)
        logger.info('Created {} records'.format(len(return_key_values)))
        return return_key_values

    def _update_records(self, records):
        non_existing_records = records.index.difference(self.df.index)
        if len(non_existing_records) > 0:
            logger.info('Of {} specified records, {} have key values that are not in data table. Ignoring these.'.format(
                len(records),
                len(non_existing_records)
            ))
            records.drop(non_existing_records, inplace=True)
        self.df.update(records)
        self.df.sort_index(inplace=True)
        return_key_values = list(records.index)
        logger.info('Updated {} records'.format(len(return_key_values)))
        return return_key_values

    def _delete_records(self, records):
        non_existing_records = records.index.difference(self.df.index)
        if len(non_existing_records) > 0:
            logger.info('Of {} specified records, {} have key values that are not in data table. Ignoring these.'.format(
                len(records),
                len(non_existing_records)
            ))
            records.drop(non_existing_records, inplace=True)
        self.df.drop(records.index, inplace=True)
        self.df.sort_index(inplace=True)
        return_key_values = list(records.index)
        logger.info('Deleted {} records'.format(len(return_key_values)))
        return return_key_values

    def _dataframe(self):
        return self.df

    def _index(self):
        return self.df.index
