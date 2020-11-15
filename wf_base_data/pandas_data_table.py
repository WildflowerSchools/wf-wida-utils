from .core import DataTable
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class PandasDataTable(DataTable):
    """
    Class to define a Pandas table for Wildflower base data
    """
    def __init__(self, key_column_names, value_column_names):
        super().__init__(
            key_column_names=key_column_names,
            value_column_names=value_column_names
        )
        self.data_table = pd.DataFrame(columns = key_column_names + value_column_names)
        self.data_table.set_index(key_column_names, inplace=True)

    def _create_records(self, records):
        already_existing_records = self.data_table.index.intersection(records.index)
        if len(already_existing_records) > 0:
            logger.info('Of {} specified records, {} have key values that are already in the data table. Ignoring these.'.format(
                len(records),
                len(already_existing_records)
            ))
            records.drop(already_existing_records, inplace=True)
        self.data_table = pd.concat((self.data_table, records))
        return_key_values = list(records.index)
        return return_key_values

    def _update_records(self, records):
        non_existing_records = records.index.difference(self.data_table.index)
        if len(non_existing_records) > 0:
            logger.info('Of {} specified records, {} have key values that are not in data table. Ignoring these.'.format(
                len(records),
                len(non_existing_records)
            ))
            records.drop(non_existing_records, inplace=True)
        self.data_table.update(records)
        return_key_values = list(records.index)
        return return_key_values

    def _delete_records(self, records):
        non_existing_records = records.index.difference(self.data_table.index)
        if len(non_existing_records) > 0:
            logger.info('Of {} specified records, {} have key values that are not in data table. Ignoring these.'.format(
                len(records),
                len(non_existing_records)
            ))
            records.drop(non_existing_records, inplace=True)
        self.data_table.drop(records.index, inplace=True)
        return_key_values = list(records.index)
        return return_key_values
