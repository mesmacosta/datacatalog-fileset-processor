import unittest
from unittest import mock

from google.api_core import exceptions
from google.cloud import datacatalog_v1

from datacatalog_fileset_processor import datacatalog_facade


class DataCatalogFacadeTestCase(unittest.TestCase):

    @mock.patch('datacatalog_fileset_processor.datacatalog_facade.datacatalog_v1.DataCatalogClient'
                )
    def setUp(self, mock_datacatalog_client):
        self.__datacatalog_facade = datacatalog_facade.DataCatalogFacade()
        # Shortcut for the object assigned to self.__datacatalog_facade.__datacatalog
        self.__datacatalog_client = mock_datacatalog_client.return_value

    def test_constructor_should_set_instance_attributes(self):
        self.assertIsNotNone(self.__datacatalog_facade.__dict__['_DataCatalogFacade__datacatalog'])

    def test_create_entry_should_succeed(self):
        entry = create_entry('type', 'system', 'display_name', 'name', 'description',
                             'linked_resource', 11, 22)

        self.__datacatalog_facade.create_entry('entry_group_name', 'entry_id', entry)

        datacatalog = self.__datacatalog_client
        self.assertEqual(1, datacatalog.create_entry.call_count)

    def test_create_entry_should_return_original_on_permission_denied(self):
        datacatalog = self.__datacatalog_client
        datacatalog.create_entry.side_effect = \
            exceptions.PermissionDenied('Permission denied')

        entry = create_entry('type', 'system', 'display_name', 'name', 'description',
                             'linked_resource', 11, 22)

        result = self.__datacatalog_facade.create_entry('entry_group_name', 'entry_id', entry)

        self.assertEqual(1, datacatalog.create_entry.call_count)
        self.assertEqual(entry, result)

    def test_get_entry_should_succeed(self):
        self.__datacatalog_facade.get_entry('entry_name')

        datacatalog = self.__datacatalog_client
        self.assertEqual(1, datacatalog.get_entry.call_count)

    def test_update_entry_should_succeed(self):
        self.__datacatalog_facade.update_entry({})

        datacatalog = self.__datacatalog_client
        self.assertEqual(1, datacatalog.update_entry.call_count)

    def test_upsert_entry_nonexistent_should_create(self):
        datacatalog = self.__datacatalog_client
        datacatalog.get_entry.side_effect = \
            exceptions.PermissionDenied('Entry not found')

        entry = create_entry('type', 'system', 'display_name', 'name', 'description',
                             'linked_resource', 11, 22)

        self.__datacatalog_facade.upsert_entry('entry_group_name', 'name', 'entry_id', entry)

        self.assertEqual(1, datacatalog.get_entry.call_count)
        self.assertEqual(1, datacatalog.create_entry.call_count)

    def test_upsert_entry_changed_should_update(self):
        entry_1 = create_entry('type', 'system', 'display_name', 'name', 'description',
                               'linked_resource_1', 11, 22)

        datacatalog = self.__datacatalog_client
        datacatalog.get_entry.return_value = entry_1

        entry_2 = create_entry('type', 'system', 'display_name_2', 'name_2', 'description_2',
                               'linked_resource_1', 11, 22)

        self.__datacatalog_facade.upsert_entry('entry_group_name', 'name_2', 'entry_id', entry_2)

        self.assertEqual(1, datacatalog.get_entry.call_count)
        self.assertEqual(1, datacatalog.update_entry.call_count)
        datacatalog.update_entry.assert_called_with(entry=entry_2, update_mask=None)

    def test_upsert_entry_should_return_original_on_failed_precondition(self):
        entry_1 = create_entry('type', 'system', 'display_name', 'name', 'description',
                               'linked_resource_1', 11, 22)

        datacatalog = self.__datacatalog_client
        datacatalog.get_entry.return_value = entry_1
        datacatalog.update_entry.side_effect = \
            exceptions.FailedPrecondition('Failed precondition')

        entry_2 = create_entry('type', 'system', 'display_name', 'name', 'description',
                               'linked_resource_2', 11, 22)

        result = self.__datacatalog_facade.upsert_entry('entry_group_name', 'name', 'entry_id',
                                                        entry_2)

        self.assertEqual(1, datacatalog.get_entry.call_count)
        self.assertEqual(1, datacatalog.update_entry.call_count)
        self.assertEqual(entry_1, result)

    def test_upsert_entry_unchanged_should_not_update(self):
        entry = create_entry('type', 'system', 'display_name', 'name', 'description',
                             'linked_resource', 11, 22)

        datacatalog = self.__datacatalog_client
        datacatalog.get_entry.return_value = entry

        self.__datacatalog_facade.upsert_entry('entry_group_name', 'name', 'entry_id', entry)

        self.assertEqual(1, datacatalog.get_entry.call_count)
        datacatalog.update_entry.assert_not_called()

    def test_delete_entry_should_succeed(self):
        self.__datacatalog_facade.delete_entry('entry_name')

        datacatalog = self.__datacatalog_client
        self.assertEqual(1, datacatalog.delete_entry.call_count)

    def test_delete_entry_error_should_be_ignored(self):
        datacatalog = self.__datacatalog_client
        datacatalog.delete_entry.side_effect = \
            Exception('Error when deleting entry')

        self.__datacatalog_facade.delete_entry('entry_name')

        self.assertEqual(1, datacatalog.delete_entry.call_count)

    def test_create_entry_group_should_succeed(self):
        self.__datacatalog_facade.create_entry_group('my-project', 'location-id', 'entry_group_id',
                                                     {})

        datacatalog = self.__datacatalog_client
        self.assertEqual(1, datacatalog.create_entry_group.call_count)

    def test_delete_entry_group_should_succeed(self):
        self.__datacatalog_facade.delete_entry_group('entry_group_name')

        datacatalog = self.__datacatalog_client
        self.assertEqual(1, datacatalog.delete_entry_group.call_count)

    def test_extract_resources_from_template_should_return_values(self):
        resource_name = 'projects/my-project/locations/us-central1/entryGroups/my-entry-group'

        project_id, location_id, entry_group_id = \
            self.__datacatalog_facade.extract_resources_from_entry_group(resource_name)

        self.assertEqual('my-project', project_id)
        self.assertEqual('us-central1', location_id)
        self.assertEqual('my-entry-group', entry_group_id)


def create_entry(entry_type, system, display_name, name, description, linked_resource, create_time,
                 update_time):
    entry = datacatalog_v1.types.Entry()

    entry.user_specified_type = entry_type
    entry.user_specified_system = system

    entry.display_name = display_name

    entry.name = name

    entry.source_system_timestamps.create_time.seconds = create_time

    entry.source_system_timestamps.update_time.seconds = update_time

    entry.gcs_fileset_spec.file_patterns.extend([])
    entry.schema.columns.extend([])

    entry.description = description
    entry.linked_resource = linked_resource
    return entry
