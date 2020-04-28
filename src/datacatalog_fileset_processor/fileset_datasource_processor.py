import logging

import pandas as pd
from google.api_core import exceptions

from . import constant, datacatalog_entity_factory, datacatalog_facade


class FilesetDatasourceProcessor:

    def __init__(self):
        self.__datacatalog_facade = datacatalog_facade.DataCatalogFacade()

    def create_entry_groups_and_entries_from_csv(self, file_path):
        """
        Creates Entry Groups and Entries, if they don't exist,
          by reading information from a CSV file.

        :param file_path: The CSV file path.
        :return: A list of Tuple (entry_group, entries)
         with all Entry Groups and Entries processed.
        """
        logging.info('')
        logging.info('===> Create Fileset Entry Groups and Entries from CSV [STARTED]')

        logging.info('')
        logging.info('Reading CSV file: %s...', file_path)
        dataframe = pd.read_csv(file_path)

        logging.info('')
        created_assets = self.__create_entry_groups_and_entries_from_dataframe(dataframe)

        logging.info('')
        logging.info(
            '==== Create Fileset Entry Groups and Entries from CSV [FINISHED] ===========')

        return created_assets

    def delete_entry_groups_and_entries_from_csv(self, file_path):
        """
        Delete Entry Groups and Entries by reading information from a CSV file.

        :param file_path: The CSV file path.
        """
        logging.info('')
        logging.info('===> Delete Fileset Entry Groups and Entries from CSV [STARTED]')

        logging.info('')
        logging.info('Reading CSV file: %s...', file_path)
        dataframe = pd.read_csv(file_path)

        logging.info('')
        logging.info('Deleting the Entries...')
        self.__delete_entry_groups_and_entries_from_dataframe(dataframe)

        logging.info('')
        logging.info(
            '==== Delete Fileset Entry Groups and Entries from CSV [FINISHED] ===========')

    def __create_entry_groups_and_entries_from_dataframe(self, dataframe):
        normalized_df = self.__normalize_dataframe(dataframe)

        entry_groups_dict = {'entry_groups': self.__extract_entry_groups_dict(normalized_df)}

        created_entry_groups = []
        for entry_group_dict in entry_groups_dict['entry_groups']:
            created_entry_groups.append(self.__create_entry_groups_from_dict(entry_group_dict))
        return created_entry_groups

    def __delete_entry_groups_and_entries_from_dataframe(self, dataframe):
        normalized_df = self.__normalize_dataframe(dataframe)

        entry_groups_dict = {'entry_groups': self.__extract_entry_groups_dict(normalized_df)}

        for entry_group_dict in entry_groups_dict['entry_groups']:
            entry_group_name = entry_group_dict['name']
            try:
                entries_dict = entry_group_dict['entries']
                for entry_dict in entries_dict:
                    entry_name = entry_dict['name']
                    try:
                        self.__datacatalog_facade.delete_entry(entry_name)
                    except exceptions.GoogleAPICallError as e:
                        logging.warning('Exception deleting Entry %s.: %s', entry_name, str(e))

                self.__datacatalog_facade.delete_entry_group(entry_group_name)
                logging.info('Entry Group %s deleted.', entry_group_name)
            except exceptions.GoogleAPICallError as e:
                logging.warning('Exception deleting Entry Group %s.: %s', entry_group_name, str(e))

    @classmethod
    def __normalize_dataframe(cls, dataframe):
        # Reorder dataframe columns.
        ordered_df = dataframe.reindex(columns=constant.FILESETS_COLUMNS_ORDER, copy=False)

        # Fill NA/NaN values by propagating the last valid observation forward to next valid.
        filled_subset = ordered_df[constant.FILESETS_FILLABLE_COLUMNS].fillna(method='pad')

        # Rebuild the dataframe by concatenating the fillable and non-fillable columns.
        rebuilt_df = pd.concat([filled_subset, ordered_df[constant.FILESETS_NON_FILLABLE_COLUMNS]],
                               axis=1)

        return rebuilt_df

    def __extract_entry_groups_dict(self, dataframe):
        dataframe.set_index(constant.FILESETS_ENTRY_GROUP_NAME_COLUMN_LABEL, inplace=True)
        key_values = dataframe.index.unique().tolist()
        array = []
        for key_value in key_values:
            # We use an array with: [key_value] to make sure the dataframe loc
            # always returns a dataframe, and not a Series
            entry_group_subset = dataframe.loc[[key_value]]

            dataframe.drop(key_value, inplace=True)

            entry_group_data = \
                entry_group_subset.loc[:, :constant.FILESETS_ENTRY_GROUP_DESCRIPTION_COLUMN_LABEL]

            entries = self.__extract_entries(
                key_value, entry_group_subset.loc[:, constant.FILESETS_ENTRY_ID_COLUMN_LABEL:])

            array.append({
                'name':
                key_value,
                'display_name':
                entry_group_data[constant.FILESETS_ENTRY_GROUP_DISPLAY_NAME_COLUMN_LABEL][0],
                'description':
                entry_group_data[constant.FILESETS_ENTRY_GROUP_DESCRIPTION_COLUMN_LABEL][0],
                'entries':
                entries
            })
        return array

    def __extract_entries(self, entry_group_name, dataframe):
        dataframe.set_index(constant.FILESETS_ENTRY_ID_COLUMN_LABEL, inplace=True)
        key_values = dataframe.index.unique().tolist()
        array = []
        for key_value in key_values:
            # We use an array with: [key_value] to make sure the dataframe loc
            # always returns a dataframe, and not a Series
            entry_subset = dataframe.loc[[key_value]]

            entry_name = '{}/entries/{}'.format(entry_group_name, key_value)

            schema_columns_subset = \
                entry_subset.loc[:, constant.FILESETS_ENTRY_SCHEMA_COLUMN_NAME_COLUMN_LABEL:]

            schema_columns_dict = self.__convert_schema_columns_dataframe_to_dict(
                schema_columns_subset)

            array.append({
                'id':
                key_value,
                'name':
                entry_name,
                'display_name':
                entry_subset['entry_display_name'][0],
                'description':
                entry_subset['entry_description'][0],
                'file_patterns':
                entry_subset['entry_file_patterns'][0].split(
                    constant.FILE_PATTERNS_VALUES_SEPARATOR),
                'schema_columns':
                schema_columns_dict
            })
        return array

    def __create_entry_groups_from_dict(self, entry_group_dict):
        entry_group_name = entry_group_dict['name']
        entries_dict = entry_group_dict['entries']
        entry_group = datacatalog_entity_factory.DataCatalogEntityFactory.make_entry_group(
            entry_group_dict)
        project_id, location_id, entry_group_id = \
            self.__datacatalog_facade.extract_resources_from_entry_group(entry_group_name)
        try:
            self.__datacatalog_facade.create_entry_group(project_id, location_id, entry_group_id,
                                                         entry_group)
        except exceptions.AlreadyExists:
            logging.warning('Entry Group %s already exists.', entry_group_name)

        created_entries = self.__create_entries_from_dict(entries_dict, entry_group_name)
        return entry_group_name, created_entries

    def __create_entries_from_dict(self, entries_dict, entry_group_name):
        created_entries = []
        for entry_dict in entries_dict:
            entry_name = entry_dict['name']
            entry = datacatalog_entity_factory.DataCatalogEntityFactory.make_entry(entry_dict)
            self.__datacatalog_facade.upsert_entry(entry_group_name, entry_name, entry_dict['id'],
                                                   entry)
            created_entries.append(entry_name)
        return created_entries

    @classmethod
    def __convert_schema_columns_dataframe_to_dict(cls, dataframe):
        base_dict = dataframe.to_dict(orient='records')

        id_to_column_schema_map = {}
        for base_object in base_dict:
            id_to_column_schema_map[
                base_object[constant.FILESETS_ENTRY_SCHEMA_COLUMN_NAME_COLUMN_LABEL]] = \
                {
                    constant.FILESETS_ENTRY_SCHEMA_COLUMN_DESCRIPTION_COLUMN_LABEL:
                        base_object[
                            constant.FILESETS_ENTRY_SCHEMA_COLUMN_DESCRIPTION_COLUMN_LABEL],
                    constant.FILESETS_ENTRY_SCHEMA_COLUMN_MODE_COLUMN_LABEL:
                        base_object[constant.FILESETS_ENTRY_SCHEMA_COLUMN_MODE_COLUMN_LABEL],
                    constant.FILESETS_ENTRY_SCHEMA_COLUMN_TYPE_COLUMN_LABEL:
                        base_object[constant.FILESETS_ENTRY_SCHEMA_COLUMN_TYPE_COLUMN_LABEL]
                }

        return id_to_column_schema_map
