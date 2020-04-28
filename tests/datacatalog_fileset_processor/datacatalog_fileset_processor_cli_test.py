import unittest
from unittest import mock

from datacatalog_fileset_processor import datacatalog_fileset_processor_cli


class DatacatalogFilesetProcessorCLITest(unittest.TestCase):

    def test_parse_args_invalid_subcommand_should_raise_system_exit(self):
        self.assertRaises(
            SystemExit,
            datacatalog_fileset_processor_cli.DatacatalogFilesetProcessorCLI._parse_args,
            ['invalid-subcommand'])

    def test_run_no_args_should_raise_attribute_error(self):
        self.assertRaises(AttributeError,
                          datacatalog_fileset_processor_cli.DatacatalogFilesetProcessorCLI.run,
                          None)

    @mock.patch('datacatalog_fileset_processor.datacatalog_fileset_processor_cli.'
                'fileset_datasource_processor.'
                'FilesetDatasourceProcessor')
    def test_run_create_filests_should_call_correct_method(
            self, mock_fileset_datasource_processor):  # noqa: E125

        datacatalog_fileset_processor_cli.DatacatalogFilesetProcessorCLI.run(
            ['filesets', 'create', '--csv-file', 'test.csv'])

        fileset_datasource_processor = mock_fileset_datasource_processor.return_value
        fileset_datasource_processor.create_entry_groups_and_entries_from_csv.assert_called_once()
        fileset_datasource_processor.create_entry_groups_and_entries_from_csv.assert_called_with(
            file_path='test.csv')

    @mock.patch('datacatalog_fileset_processor.datacatalog_fileset_processor_cli.'
                'fileset_datasource_processor.'
                'FilesetDatasourceProcessor')
    def test_run_delete_filesets_should_call_correct_method(
            self, mock_fileset_datasource_processor):  # noqa: E125

        datacatalog_fileset_processor_cli.DatacatalogFilesetProcessorCLI.run(
            ['filesets', 'delete', '--csv-file', 'test.csv'])

        fileset_datasource_processor = mock_fileset_datasource_processor.return_value
        fileset_datasource_processor.delete_entry_groups_and_entries_from_csv.assert_called_once()
        fileset_datasource_processor.delete_entry_groups_and_entries_from_csv.assert_called_with(
            file_path='test.csv')

    @mock.patch('datacatalog_fileset_processor.datacatalog_fileset_processor_cli.'
                'DatacatalogFilesetProcessorCLI')
    def test_main_should_call_cli_run(self, mock_cli):
        datacatalog_fileset_processor_cli.main()
        mock_cli.run.assert_called_once()
