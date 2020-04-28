import unittest

from google.cloud.datacatalog import enums

from datacatalog_fileset_processor import datacatalog_entity_factory


class DataCatalogEntityFactoryTest(unittest.TestCase):
    __BOOL_TYPE = enums.FieldType.PrimitiveType.BOOL
    __DOUBLE_TYPE = enums.FieldType.PrimitiveType.DOUBLE
    __STRING_TYPE = enums.FieldType.PrimitiveType.STRING
    __TIMESTAMP_TYPE = enums.FieldType.PrimitiveType.TIMESTAMP

    def test_make_entry_valid_boolean_values_should_set_fields(self):

        entry_dict = {
            'display_name': 'My Entry',
            'description': 'My Entry Description',
            'file_patterns': ['gs://bucket_13c4/*', 'gs://bucket_23c4/*'],
            'schema_columns': {
                'has_pii': {
                    'schema_column_type': 'BOOL',
                    'schema_column_description': 'My BOOL field',
                    'schema_column_mode': 'REQUIRED',
                }
            }
        }

        entry = datacatalog_entity_factory.DataCatalogEntityFactory.make_entry(entry_dict)

        my_pii_field = entry.schema.columns[0]

        self.assertEqual(entry_dict['display_name'], entry.display_name)
        self.assertEqual(entry_dict['description'], entry.description)
        self.assertEqual(entry_dict['file_patterns'][0], entry.gcs_fileset_spec.file_patterns[0])
        self.assertEqual(entry_dict['file_patterns'][1], entry.gcs_fileset_spec.file_patterns[1])
        self.assertEqual('has_pii', my_pii_field.column)
        self.assertEqual(entry_dict['schema_columns']['has_pii']['schema_column_type'],
                         my_pii_field.type)
        self.assertEqual(entry_dict['schema_columns']['has_pii']['schema_column_description'],
                         my_pii_field.description)
        self.assertEqual(entry_dict['schema_columns']['has_pii']['schema_column_mode'],
                         my_pii_field.mode)

    def test_make_entry_multiple_schema_columns_should_set_fields(self):

        entry_dict = {
            'display_name': 'My Entry',
            'description': 'My Entry Description',
            'file_patterns': ['gs://bucket_13c4/*', 'gs://bucket_23c4/*'],
            'schema_columns': {
                'has_pii': {
                    'schema_column_type': 'BOOL',
                    'schema_column_description': 'My BOOL field',
                    'schema_column_mode': 'REQUIRED',
                },
                'first_name': {
                    'schema_column_type': 'STRING',
                    'schema_column_description': 'My STRING field',
                    'schema_column_mode': 'REQUIRED',
                }
            }
        }

        entry = datacatalog_entity_factory.DataCatalogEntityFactory.make_entry(entry_dict)

        my_pii_field = entry.schema.columns[0]
        first_name = entry.schema.columns[1]

        self.assertEqual(entry_dict['display_name'], entry.display_name)
        self.assertEqual(entry_dict['description'], entry.description)
        self.assertEqual(entry_dict['file_patterns'][0], entry.gcs_fileset_spec.file_patterns[0])
        self.assertEqual(entry_dict['file_patterns'][1], entry.gcs_fileset_spec.file_patterns[1])
        self.assertEqual('has_pii', my_pii_field.column)
        self.assertEqual(entry_dict['schema_columns']['has_pii']['schema_column_type'],
                         my_pii_field.type)
        self.assertEqual(entry_dict['schema_columns']['has_pii']['schema_column_description'],
                         my_pii_field.description)
        self.assertEqual(entry_dict['schema_columns']['has_pii']['schema_column_mode'],
                         my_pii_field.mode)
        self.assertEqual('first_name', first_name.column)
        self.assertEqual(entry_dict['schema_columns']['first_name']['schema_column_type'],
                         first_name.type)
        self.assertEqual(entry_dict['schema_columns']['first_name']['schema_column_description'],
                         first_name.description)
        self.assertEqual(entry_dict['schema_columns']['first_name']['schema_column_mode'],
                         first_name.mode)

    def test_make_entry_group_should_set_fields(self):

        make_entry_group = {
            'display_name': 'My Entry Group',
            'description': 'My Entry Group Description',
        }

        entry_group = datacatalog_entity_factory.DataCatalogEntityFactory.make_entry_group(
            make_entry_group)

        self.assertEqual(make_entry_group['display_name'], entry_group.display_name)
        self.assertEqual(make_entry_group['description'], entry_group.description)
