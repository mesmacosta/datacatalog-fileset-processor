import logging
import re

from google.api_core import exceptions
from google.cloud import datacatalog_v1

from datacatalog_fileset_processor.values_comparable_object import ValuesComparableObject


class DataCatalogFacade:
    """Data Catalog API communication facade."""

    def __init__(self):
        # Initialize the API client.
        self.__datacatalog = datacatalog_v1.DataCatalogClient()

    def create_entry(self, entry_group_name, entry_id, entry):
        """Creates a Data Catalog Entry.

        :param entry_group_name: Parent Entry Group name.
        :param entry_id: Entry id.
        :param entry: An Entry object.
        :return: The created Entry.
        """
        try:
            entry = self.__datacatalog.create_entry(parent=entry_group_name,
                                                    entry_id=entry_id,
                                                    entry=entry)
            self.__log_entry_operation('created', entry=entry)
        except exceptions.PermissionDenied as e:
            entry_name = '{}/entries/{}'.format(entry_group_name, entry_id)
            self.__log_entry_operation('was not created', entry_name=entry_name)
            logging.warning('Error: %s', e)

        return entry

    def get_entry(self, name):
        """Retrieves Data Catalog Entry.

        :param name: The Entry name.
        :return: An Entry object if it exists.
        """
        return self.__datacatalog.get_entry(name=name)

    def update_entry(self, entry):
        """Updates an Entry.

        :param entry: An Entry object.
        :return: The updated Entry.
        """
        entry = self.__datacatalog.update_entry(entry=entry, update_mask=None)
        self.__log_entry_operation('updated', entry=entry)
        return entry

    def upsert_entry(self, entry_group_name, entry_name, entry_id, entry):
        """
        Update a Data Catalog Entry if it exists and has been changed.
        Creates a new Entry if it does not exist.

        :param entry_group_name: Parent Entry Group name.
        :param entry_name: Entry Name.
        :param entry_id: Entry id.
        :param entry: An Entry object.
        :return: The updated or created Entry.
        """
        persisted_entry = entry
        try:
            persisted_entry = self.get_entry(name=entry_name)
            self.__log_entry_operation('already exists', entry_name=entry_name)
            if self.__entry_was_updated(persisted_entry, entry):
                # merge fields.
                persisted_entry.name = entry_name
                persisted_entry.display_name = entry.display_name
                persisted_entry.description = entry.description
                # clear repeated message containers.
                del persisted_entry.gcs_fileset_spec.file_patterns[:]
                del persisted_entry.schema.columns[:]

                persisted_entry.gcs_fileset_spec.file_patterns.extend(
                    entry.gcs_fileset_spec.file_patterns)
                persisted_entry.schema.columns.extend(entry.schema.columns)
                persisted_entry = self.update_entry(entry=persisted_entry)
            else:
                self.__log_entry_operation('is up-to-date', entry=persisted_entry)
        except exceptions.PermissionDenied:
            self.__log_entry_operation('does not exist', entry_name=entry_name)
            persisted_entry = self.create_entry(entry_group_name=entry_group_name,
                                                entry_id=entry_id,
                                                entry=entry)
        except exceptions.FailedPrecondition as e:
            logging.warning('Entry was not updated: %s', entry_name)
            logging.warning('Error: %s', e)

        return persisted_entry

    @classmethod
    def __entry_was_updated(cls, current_entry, new_entry):
        # Update time comparison allows to verify whether the entry was
        # updated on the source system.
        current_update_time = \
            current_entry.source_system_timestamps.update_time.seconds
        new_update_time = \
            new_entry.source_system_timestamps.update_time.seconds

        updated_time_changed = \
            new_update_time != 0 and current_update_time != new_update_time

        return updated_time_changed or not cls.__entries_are_equal(current_entry, new_entry)

    @classmethod
    def __entries_are_equal(cls, entry_1, entry_2):
        object_1 = ValuesComparableObject()
        object_1.user_specified_system = entry_1.user_specified_system
        object_1.user_specified_type = entry_1.user_specified_type
        object_1.display_name = entry_1.display_name
        object_1.description = entry_1.description
        object_1.linked_resource = entry_1.linked_resource

        object_2 = ValuesComparableObject()
        object_2.user_specified_system = entry_2.user_specified_system
        object_2.user_specified_type = entry_2.user_specified_type
        object_2.display_name = entry_2.display_name
        object_2.description = entry_2.description
        object_2.linked_resource = entry_2.linked_resource

        return object_1 == object_2

    def delete_entry(self, name):
        """Deletes a Data Catalog Entry.

        :param name: The Entry name.
        """
        try:
            self.__datacatalog.delete_entry(name=name)
            self.__log_entry_operation('deleted', entry_name=name)
        except Exception as e:
            logging.info('An exception ocurred while attempting to' ' delete Entry: %s', name)
            logging.debug(str(e))

    @classmethod
    def __log_entry_operation(cls, description, entry=None, entry_name=None):

        formatted_description = 'Entry {}: '.format(description)
        logging.info('%s%s', formatted_description, entry.name if entry else entry_name)

        if entry:
            logging.info('%s^ %s', ' ' * len(formatted_description), entry.linked_resource)

    def create_entry_group(self, project_id, location_id, entry_group_id, entry_group):
        """Creates a Data Catalog Entry Group.

        :param project_id: Project id.
        :param location_id: Location id.
        :param entry_group_id: Entry Group id.
        :param entry_group: Entry Group.

        :return: The created Entry Group.
        """
        created_entry_group = self.__datacatalog.create_entry_group(
            parent=datacatalog_v1.DataCatalogClient.location_path(project_id, location_id),
            entry_group_id=entry_group_id,
            entry_group=entry_group)
        logging.info('Entry Group created: %s', created_entry_group.name)
        return created_entry_group

    def delete_entry_group(self, name):
        """
        Deletes a Data Catalog Entry Group.

        :param name: The Entry Group name.
        """
        self.__datacatalog.delete_entry_group(name=name)

    @classmethod
    def extract_resources_from_entry_group(cls, entry_group_name):
        re_match = re.match(
            r'^projects[/]([_a-zA-Z-\d]+)[/]locations[/]'
            r'([a-zA-Z-\d]+)[/]entryGroups[/]([@a-zA-Z-_\d]+)$', entry_group_name)

        if re_match:
            project_id, location_id, entry_group_id, = re_match.groups()
            return project_id, location_id, entry_group_id
