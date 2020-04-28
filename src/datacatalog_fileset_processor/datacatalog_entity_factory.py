from google.cloud import datacatalog_v1


class DataCatalogEntityFactory:

    @classmethod
    def make_entry_group(cls, entry_group_dict):
        entry_group = datacatalog_v1.types.EntryGroup()
        entry_group.display_name = entry_group_dict['display_name']
        entry_group.description = entry_group_dict['description']

        return entry_group

    @classmethod
    def make_entry(cls, entry_dict):
        entry = datacatalog_v1.types.Entry()
        entry.display_name = entry_dict['display_name']
        entry.description = entry_dict['description']
        entry.gcs_fileset_spec.file_patterns.extend(entry_dict['file_patterns'])
        entry.type = datacatalog_v1.enums.EntryType.FILESET

        schema_columns = entry_dict['schema_columns']

        columns = []
        for column_id, items in schema_columns.items():

            # Create the Schema, this is optional.
            columns.append(
                datacatalog_v1.types.ColumnSchema(column=column_id,
                                                  type=items['schema_column_type'],
                                                  description=items['schema_column_description'],
                                                  mode=items['schema_column_mode']))

        entry.schema.columns.extend(columns)
        return entry
