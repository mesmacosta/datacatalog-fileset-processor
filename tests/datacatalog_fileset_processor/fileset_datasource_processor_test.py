import unittest
from unittest import mock

import pandas as pd

from datacatalog_fileset_processor import fileset_datasource_processor


@mock.patch('datacatalog_fileset_processor.fileset_datasource_processor.pd.read_csv')
class TagTemplateDatasourceProcessorTest(unittest.TestCase):

    @mock.patch('datacatalog_fileset_processor.datacatalog_facade.DataCatalogFacade')
    def setUp(self, mock_datacatalog_facade):
        self.__tag_datasource_processor = fileset_datasource_processor. \
            FilesetDatasourceProcessor()
        # Shortcut for the object assigned to self.__tag_datasource_processor.__datacatalog_facade
        self.__datacatalog_facade = mock_datacatalog_facade.return_value

    def test_constructor_should_set_instance_attributes(self, mock_read_csv):
        self.assertIsNotNone(self.__tag_datasource_processor.
                             __dict__['_FilesetDatasourceProcessor__datacatalog_facade'])

    def test_create_filesets_from_csv_should_succeed(self, mock_read_csv):
        mock_read_csv.return_value = pd.DataFrame(
            data={
                'entry_group_name': [
                    'projects/uat-env-1/locations/us-central1/entryGroups/entry_group_test_1a',
                    'projects/uat-env-1/locations/us-central1/entryGroups/entry_group_test_2a',
                    'projects/uat-env-1/locations/us-central1/entryGroups/entry_group_test_2a'
                ],
                'entry_group_display_name': [
                    'My Fileset Entry Group a', 'My Fileset Entry Group 2',
                    'My Fileset Entry Group 2'
                ],
                'entry_group_description': [
                    'This Entry Group consists of ....', 'This Entry Group consists of 2....',
                    'This Entry Group consists of 2....'
                ],
                'entry_id': ['entry_test_1', 'entry_test_2', 'entry_test_3'],
                'entry_display_name': ['My Fileset', 'My Fileset 2', 'My Fileset 3'],
                'entry_description': [
                    'This fileset consists of all files for bucket bucket_13c4',
                    'This fileset consists of all files for bucket bucket_23c4',
                    'This fileset consists of all files for bucket bucket_33c4'
                ],
                'entry_file_patterns': [
                    'gs://bucket_13c4/*', 'gs://bucket_23c4/*.csv|gs://bucket_23c4/*.png',
                    'gs://bucket_33c4/*.csv|gs://bucket_33c4/*.png'
                ],
                'schema_column_name': ['first_name_a', 'first_name', None],
                'schema_column_type': ['STRING', 'STRING', None],
                'schema_column_description': ['First name', 'First name', None],
                'schema_column_mode': ['REQUIRED', 'REQUIRED', None]
            })

        self.execute_create_filesets_and_assert()

    def test_create_filesets_from_csv_missing_values_should_succeed(self, mock_read_csv):
        mock_read_csv.return_value = pd.DataFrame(
            data={
                'entry_group_name': [
                    'projects/uat-env-1/locations/us-central1/entryGroups/entry_group_test_1a',
                    'projects/uat-env-1/locations/us-central1/entryGroups/entry_group_test_2a',
                    None
                ],
                'entry_group_display_name':
                ['My Fileset Entry Group a', 'My Fileset Entry Group 2', None],
                'entry_group_description':
                ['This Entry Group consists of ....', 'This Entry Group consists of 2....', None],
                'entry_id': ['entry_test_1', 'entry_test_2', 'entry_test_3'],
                'entry_display_name': ['My Fileset', 'My Fileset 2', 'My Fileset 3'],
                'entry_description': [
                    'This fileset consists of all files for bucket bucket_13c4',
                    'This fileset consists of all files for bucket bucket_23c4',
                    'This fileset consists of all files for bucket bucket_33c4'
                ],
                'entry_file_patterns': [
                    'gs://bucket_13c4/*', 'gs://bucket_23c4/*.csv|gs://bucket_23c4/*.png',
                    'gs://bucket_33c4/*.csv|gs://bucket_33c4/*.png'
                ],
                'schema_column_name': ['first_name_a', 'first_name', None],
                'schema_column_type': ['STRING', 'STRING', None],
                'schema_column_description': ['First name', 'First name', None],
                'schema_column_mode': ['REQUIRED', 'REQUIRED', None]
            })

        self.execute_create_filesets_and_assert()

    def test_create_filesets_from_csv_unordered_columns_should_succeed(self, mock_read_csv):
        mock_read_csv.return_value = pd.DataFrame(
            data={
                'entry_id': ['entry_test_1', 'entry_test_2', 'entry_test_3'],
                'entry_group_display_name':
                ['My Fileset Entry Group a', 'My Fileset Entry Group 2', None],
                'entry_group_description':
                ['This Entry Group consists of ....', 'This Entry Group consists of 2....', None],
                'schema_column_description': ['First name', 'First name', None],
                'entry_description': [
                    'This fileset consists of all files for bucket bucket_13c4',
                    'This fileset consists of all files for bucket bucket_23c4',
                    'This fileset consists of all files for bucket bucket_33c4'
                ],
                'entry_file_patterns': [
                    'gs://bucket_13c4/*', 'gs://bucket_23c4/*.csv|gs://bucket_23c4/*.png',
                    'gs://bucket_33c4/*.csv|gs://bucket_33c4/*.png'
                ],
                'entry_group_name': [
                    'projects/uat-env-1/locations/us-central1/entryGroups/entry_group_test_1a',
                    'projects/uat-env-1/locations/us-central1/entryGroups/entry_group_test_2a',
                    None
                ],
                'entry_display_name': ['My Fileset', 'My Fileset 2', 'My Fileset 3'],
                'schema_column_name': ['first_name_a', 'first_name', None],
                'schema_column_type': ['STRING', 'STRING', None],
                'schema_column_mode': ['REQUIRED', 'REQUIRED', None]
            })

        self.execute_create_filesets_and_assert()

    def test_create_filesets_from_csv_no_schema_should_succeed(self, mock_read_csv):
        mock_read_csv.return_value = pd.DataFrame(
            data={
                'entry_group_name': [
                    'projects/uat-env-1/locations/us-central1/entryGroups/entry_group_test_1a',
                    'projects/uat-env-1/locations/us-central1/entryGroups/entry_group_test_2a',
                    'projects/uat-env-1/locations/us-central1/entryGroups/entry_group_test_2a'
                ],
                'entry_group_display_name': [
                    'My Fileset Entry Group a', 'My Fileset Entry Group 2',
                    'My Fileset Entry Group 2'
                ],
                'entry_group_description': [
                    'This Entry Group consists of ....', 'This Entry Group consists of 2....',
                    'This Entry Group consists of 2....'
                ],
                'entry_id': ['entry_test_1', 'entry_test_2', 'entry_test_3'],
                'entry_display_name': ['My Fileset', 'My Fileset 2', 'My Fileset 3'],
                'entry_description': [
                    'This fileset consists of all files for bucket bucket_13c4',
                    'This fileset consists of all files for bucket bucket_23c4',
                    'This fileset consists of all files for bucket bucket_33c4'
                ],
                'entry_file_patterns': [
                    'gs://bucket_13c4/*', 'gs://bucket_23c4/*.csv|gs://bucket_23c4/*.png',
                    'gs://bucket_33c4/*.csv|gs://bucket_33c4/*.png'
                ],
                'schema_column_name': [None, None, None],
                'schema_column_type': [None, None, None],
                'schema_column_description': [None, None, None],
                'schema_column_mode': [None, None, None]
            })

        self.execute_create_filesets_and_assert()

    def test_delete_filesets_from_csv_should_succeed(self, mock_read_csv):
        mock_read_csv.return_value = pd.DataFrame(
            data={
                'entry_group_name': [
                    'projects/uat-env-1/locations/us-central1/entryGroups/entry_group_test_1a',
                    'projects/uat-env-1/locations/us-central1/entryGroups/entry_group_test_2a',
                    'projects/uat-env-1/locations/us-central1/entryGroups/entry_group_test_2a'
                ],
                'entry_group_display_name': [
                    'My Fileset Entry Group a', 'My Fileset Entry Group 2',
                    'My Fileset Entry Group 2'
                ],
                'entry_group_description': [
                    'This Entry Group consists of ....', 'This Entry Group consists of 2....',
                    'This Entry Group consists of 2....'
                ],
                'entry_id': ['entry_test_1', 'entry_test_2', 'entry_test_3'],
                'entry_display_name': ['My Fileset', 'My Fileset 2', 'My Fileset 3'],
                'entry_description': [
                    'This fileset consists of all files for bucket bucket_13c4',
                    'This fileset consists of all files for bucket bucket_23c4',
                    'This fileset consists of all files for bucket bucket_33c4'
                ],
                'entry_file_patterns': [
                    'gs://bucket_13c4/*', 'gs://bucket_23c4/*.csv|gs://bucket_23c4/*.png',
                    'gs://bucket_33c4/*.csv|gs://bucket_33c4/*.png'
                ],
                'schema_column_name': ['first_name_a', 'first_name', None],
                'schema_column_type': ['STRING', 'STRING', None],
                'schema_column_description': ['First name', 'First name', None],
                'schema_column_mode': ['REQUIRED', 'REQUIRED', None]
            })

        self.__tag_datasource_processor.delete_entry_groups_and_entries_from_csv('file-path')
        self.assertEqual(3, self.__datacatalog_facade.delete_entry.call_count)
        self.assertEqual(2, self.__datacatalog_facade.delete_entry_group.call_count)
        self.assertEqual(0, self.__datacatalog_facade.create_entry_group.call_count)
        self.assertEqual(0, self.__datacatalog_facade.upsert_entry.call_count)

    def execute_create_filesets_and_assert(self):
        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.create_entry_group.side_effect = mock_created_entry_group
        project_id, location_id, entry_group_id = 'my_project', 'my_location', 'my-entry-group'
        datacatalog_facade.extract_resources_from_entry_group.return_value = (project_id,
                                                                              location_id,
                                                                              entry_group_id)

        created_assets = self.__tag_datasource_processor.\
            create_entry_groups_and_entries_from_csv('file-path')

        self.assertEqual(2, datacatalog_facade.create_entry_group.call_count)
        self.assertEqual(3, datacatalog_facade.upsert_entry.call_count)
        self.assertEqual(2, len(created_assets))
        entry_group, entries = created_assets[0]
        self.assertEqual(
            'projects/uat-env-1/locations/us-central1/entryGroups/entry_group_test_1a',
            entry_group)
        self.assertEqual(
            'projects/uat-env-1/locations/us-central1/entryGroups/'
            'entry_group_test_1a/entries/entry_test_1', entries[0])

        entry_group, entries = created_assets[1]
        self.assertEqual(
            'projects/uat-env-1/locations/us-central1/entryGroups/entry_group_test_2a',
            entry_group)
        self.assertEqual(
            'projects/uat-env-1/locations/us-central1/entryGroups/'
            'entry_group_test_2a/entries/entry_test_2', entries[0])
        self.assertEqual(
            'projects/uat-env-1/locations/us-central1/entryGroups/'
            'entry_group_test_2a/entries/entry_test_3', entries[1])


def mock_created_entry_group(*args):
    entry_group_id = args[2]
    entry_group = args[3]
    entry_group.name = entry_group_id
    return entry_group
