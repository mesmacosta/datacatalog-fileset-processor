import argparse
import logging
import sys

from datacatalog_fileset_processor import fileset_datasource_processor


class DatacatalogFilesetProcessorCLI:

    @classmethod
    def run(cls, argv):
        cls.__setup_logging()

        args = cls._parse_args(argv)
        args.func(args)

    @classmethod
    def __setup_logging(cls):
        logging.basicConfig(level=logging.INFO)

    @classmethod
    def _parse_args(cls, argv):
        parser = argparse.ArgumentParser(description=__doc__,
                                         formatter_class=argparse.RawDescriptionHelpFormatter)

        subparsers = parser.add_subparsers()

        cls.add_filesets_cmd(subparsers)

        return parser.parse_args(argv)

    @classmethod
    def add_filesets_cmd(cls, subparsers):
        filesets_parser = subparsers.add_parser("filesets", help="Filesets commands")

        filesets_subparsers = filesets_parser.add_subparsers()

        cls.add_create_filesets_cmd(filesets_subparsers)

        cls.add_delete_filesets_cmd(filesets_subparsers)

    @classmethod
    def add_delete_filesets_cmd(cls, subparsers):
        delete_filesets_parser = subparsers.add_parser('delete',
                                                       help='Delete Filesets Entry Groups'
                                                       ' and Entries from CSV')
        delete_filesets_parser.add_argument('--csv-file',
                                            help='CSV file with Fileset Entries information',
                                            required=True)
        delete_filesets_parser.set_defaults(func=cls.__delete_filesets_entry_groups_and_entries)

    @classmethod
    def add_create_filesets_cmd(cls, subparsers):
        create_filesets_parser = subparsers.add_parser('create',
                                                       help='Create Filesets Entry Groups'
                                                       ' and Entries from CSV')
        create_filesets_parser.add_argument('--csv-file',
                                            help='CSV file with Filesets Entries information',
                                            required=True)
        create_filesets_parser.add_argument('--validate-dataflow-sql-types',
                                            help='Flag if enabled will validate Data Flow SQL '
                                            'Types',
                                            action='store_true')
        create_filesets_parser.set_defaults(func=cls.__create_filesets_entry_groups_and_entries)

    @classmethod
    def __create_filesets_entry_groups_and_entries(cls, args):
        fileset_datasource_processor.FilesetDatasourceProcessor(
        ).create_entry_groups_and_entries_from_csv(
            file_path=args.csv_file, validate_dataflow_sql_types=args.validate_dataflow_sql_types)

    @classmethod
    def __delete_filesets_entry_groups_and_entries(cls, args):
        fileset_datasource_processor.FilesetDatasourceProcessor(
        ).delete_entry_groups_and_entries_from_csv(file_path=args.csv_file)


def main():
    argv = sys.argv
    DatacatalogFilesetProcessorCLI.run(argv[1:] if len(argv) > 0 else argv)


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
