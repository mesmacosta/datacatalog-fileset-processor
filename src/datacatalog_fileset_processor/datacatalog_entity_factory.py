import pandas as pd

from google.cloud import datacatalog_v1


class DataCatalogEntityFactory:

    @classmethod
    def make_entry_group(cls, entry_group_dict):
        entry_group = datacatalog_v1.types.EntryGroup()

        display_name = entry_group_dict.get('display_name')
        if pd.notna(display_name):
            entry_group.display_name = display_name

        description = entry_group_dict['description']
        if pd.notna(description):
            entry_group.description = description

        return entry_group

    @classmethod
    def make_entry(cls, entry_dict):
        entry = datacatalog_v1.types.Entry()
        entry.display_name = entry_dict['display_name']
        description = entry_dict.get('description')
        if pd.notna(description):
            entry.description = description

        entry.gcs_fileset_spec.file_patterns.extend(entry_dict['file_patterns'])
        entry.type = datacatalog_v1.enums.EntryType.FILESET

        schema_columns = entry_dict['schema_columns']

        columns = []
        for column_id, items in schema_columns.items():
            if pd.notna(column_id):
                # Create the Schema, this is optional.
                columns.append(
                    datacatalog_v1.types.ColumnSchema(
                        column=column_id,
                        type=items['schema_column_type'],
                        description=items['schema_column_description'],
                        mode=items['schema_column_mode']))

        entry.schema.columns.extend(columns)
        return entry
