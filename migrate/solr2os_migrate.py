import boto3
import json
import re
import requests

from reports.report import Report
from migrate.copy_field.copy_field_helper import CopyFieldHelper
from migrate.dynamic_field.dynamic_field_helper import DynamicFieldHelper
from migrate.fields.field_helper import FieldHelper, FieldException
from migrate.fieldtype.field_type_helper import FieldTypeHelper, FieldTypeException
from migrate.utils import write_json_file_data
from config import get_custom_logger

logger = get_custom_logger("migrate.solr2os_migrate")


class Solr2OSMigrate:

    def __init__(self, solrclient, opensearchclient, schema_config, data_config):
        self._schema_config = schema_config
        self._data_config = data_config
        self._solr_client = solrclient
        self._opensearch_client = opensearchclient
        self._field_type_service = FieldTypeHelper(self._solr_client, self._opensearch_client, self._schema_config)
        self._field_service = FieldHelper(self._solr_client, self._opensearch_client, self._field_type_service)
        self._dynamic_field_service = DynamicFieldHelper(self._solr_client, self._opensearch_client,
                                                         self._field_type_service)
        self._report = Report()
        self._s3_client = None
        if self._data_config.get('migrate_data', False):
            region = self._data_config['region']
            # Explicitly set the region for the S3 client
            session = boto3.session.Session(region_name=region)
            self._s3_client = session.client('s3')

    def _migrate_field_types(self):
        """
        Migrate field types
        """
        for solr_field_type in self._solr_client.read_schema()["fieldTypes"]:
            self._report.field_types_solr = self._report.field_types_solr + 1
            try:
                analyzers = self._field_type_service.map_field_type_analyzer(solr_field_type)
                for a in analyzers:
                    self._opensearch_client.add_analyzer(a)
                self._report.field_types_os = self._report.field_types_os + 1
            except FieldTypeException as e:
                self._report.field_type_exception_list.append(e)
                self._report.field_types_error = self._report.field_types_error + 1
            # except Exception as e:
            #     logger.error(e)
            #     self.report.field_types_error = self.report.field_types_error + 1
            #     pass

    def _migrate_fields(self):
        """
        Migrate fields
        """
        for solr_field in self._solr_client.read_schema()["fields"]:
            self._report.field_solr = self._report.field_solr + 1
            try:
                name, opensearch_field = self._field_service.map_field(solr_field)
                if opensearch_field is None:
                    continue
                self._opensearch_client.add_field(name, opensearch_field)
                self._report.field_os = self._report.field_os + 1
            except FieldException as e:
                self._report.field_exception_list.append(e)
                self._report.field_error = self._report.field_error + 1
                pass

    def _migrate_dynamic_fields(self):
        """
        Migrate dynamic fields
        """
        dynamic_field_service = self._dynamic_field_service

        for solr_dynamic_field in self._solr_client.read_schema()["dynamicFields"]:
            self._report.dynamic_field_solr = self._report.dynamic_field_solr + 1
            try:
                opensearch_dynamic_field = dynamic_field_service.map_dynamic_field(solr_dynamic_field)
                self._opensearch_client.add_dynamic_field(opensearch_dynamic_field)
                self._report.dynamic_field_os = self._report.dynamic_field_os + 1
            except Exception as e:
                self._report.dynamic_field_exception_list.append(e)
                self._report.dynamic_field_error = self._report.dynamic_field_error + 1
                pass

    def _migrate_copy_fields(self, all_fields):
        """
        Migrate copy fields
        :param all_fields:
        """
        copy_field_service = CopyFieldHelper(all_fields)

        for solr_copy_field in self._solr_client.read_schema()["copyFields"]:
            self._report.copy_field_solr = self._report.copy_field_solr + 1
            try:
                src, src_def, dst, dst_def = copy_field_service.map_copy_field(solr_copy_field)
                self._opensearch_client.add_field(src, src_def)
                self._opensearch_client.add_field(dst, dst_def)
                self._report.copy_field_os = self._report.copy_field_os + 1
            except Exception as e:
                self._report.copy_field_exception_list.append(e)
                self._report.copy_field_error = self._report.copy_field_error + 1
                pass


    def _get_binary_fields(self):
        """Get list of binary field names from schema"""
        binary_fields = []
        try:
            schema = self._solr_client.read_schema()
            binary_field_types = set()
            for field_type in schema.get('fieldTypes', []):
                if field_type.get('class', '').endswith('BinaryField'):
                    name = field_type.get('name') or field_type.get('class')
                    if name:
                        binary_field_types.add(name)
            
            for field in schema.get('fields', []):
                field_type = field.get('type')
                field_name = field.get('name')
                if field_type in binary_field_types and field_name:
                    binary_fields.append(field_name)
        except Exception as e:
            error_msg = f"Error identifying binary fields: {str(e)}"
            logger.error(error_msg)
            self._report.add_data_migration_error(error_msg)
        return binary_fields

    def _fix_binary_fields_in_json(self, response_text, binary_fields):
        """Fix unquoted binary field values in JSON response"""
        if binary_fields:
            for field in binary_fields:
                pattern = rf'"{field}":([^",:}}\s]+)'
                replacement = rf'"{field}":"\1"'
                response_text = re.sub(pattern, replacement, response_text)
        return response_text

    def _export_data_to_s3(self):
        """
        Export Solr data to S3 
        """
        if not self._data_config.get('migrate_data', False):
            logger.info("Skipping data export as migrate_data is set to false")
            return

        logger.info("Starting Solr data export to S3 using two-query approach")
        self._export_regular_data()

    def _export_regular_data(self):
        """
        Regular export for non-nested documents with binary field support
        """
        solr_config = self._solr_client.get_config()
        rows_per_page = self._data_config.get('rows_per_page', 500)
        max_rows = self._data_config.get('max_rows', 100000)
        s3_bucket = self._data_config.get('s3_export_bucket')
        s3_prefix = self._data_config.get('s3_export_prefix', 'solr-data/')

        query_url = f"{solr_config['host']}:{solr_config['port']}/solr/{solr_config['collection']}/select"
        auth = None
        if solr_config.get('username') and solr_config.get('password'):
            auth = (solr_config['username'], solr_config['password'])

        binary_fields = self._get_binary_fields()
        logger.info(f"Identified binary fields: {binary_fields}")

        # Get total document count
        params = {'q': '*:*', 'rows': 0, 'wt': 'json'}
        response = requests.get(query_url, params=params, auth=auth, timeout=30)
        response.raise_for_status()
        total_docs = response.json()['response']['numFound']
        
        logger.info(f"Found {total_docs} documents")
        
        exported_docs = 0
        batch_count = 0
        cursor_mark = "*"
        
        while exported_docs < min(total_docs, max_rows):
            batch_count += 1
            logger.info(f"Processing batch {batch_count} with cursor {cursor_mark}")
            
            try:
                params = {
                    'q': '{!parent which="*:* -_nest_path_:*"}',
                    'fl': '*,[child]',
                    'sort': 'id asc',
                    'cursorMark': cursor_mark,
                    'rows': rows_per_page,
                    'wt': 'json'
                }
                
                response = requests.get(query_url, params=params, auth=auth, timeout=300)
                response.raise_for_status()
                
                # Handle JSON parsing with binary field support
                try:
                    response_text = self._fix_binary_fields_in_json(response.text, binary_fields)
                    batch_data = json.loads(response_text)
                    
                except json.JSONDecodeError as e:
                    error_msg = f"JSON parsing error in batch {batch_count}: {str(e)}"
                    logger.error(error_msg)
                    self._report.add_data_migration_error(error_msg)
                    continue
                
                docs = batch_data['response']['docs']
                
                if not docs:
                    break
                
                # Export batch to S3
                s3_key = f"{s3_prefix}{solr_config['collection']}_batch_{batch_count}.json"
                self._s3_client.put_object(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    Body=json.dumps(docs),
                    ContentType='application/json'
                )
                
                exported_docs += len(docs)
                logger.info(f"Exported {len(docs)} documents in batch {batch_count}")
                
                next_cursor_mark = batch_data.get('nextCursorMark')
                if next_cursor_mark == cursor_mark:
                    break
                cursor_mark = next_cursor_mark
                
            except Exception as e:
                error_msg = f"Error processing batch {batch_count}: {str(e)}"
                logger.error(error_msg)
                self._report.add_data_migration_error(error_msg)
                break
        
        # Update final report
        self._report.update_data_migration_stats(
            enabled=True,
            total=total_docs,
            exported=exported_docs,
            batches=batch_count
        )
        
        logger.info(f"Completed regular data export: {exported_docs} documents")
        print(f"\n=== DATA MIGRATION COMPLETE ===")
        print(f"Total documents exported: {exported_docs}")
        print(f"================================\n")

    def migrate_schema(self, file_path_prefix="migration_schema"):
        """
        Method to migrate schema: field_types, fields, dynamic fields, copy fields
        """
        self._migrate_field_types()
        self._migrate_fields()
        self._migrate_dynamic_fields()
        self._migrate_copy_fields(self._opensearch_client.get_all_fields())

        index_path = f"{file_path_prefix}/index.json"
        report_path = f"{file_path_prefix}/report.html"
        write_json_file_data(self._opensearch_client.get_index_json(), index_path)
        self._report.report(report_path)

        if self._schema_config['create_index']:
            self._opensearch_client.create_index()

        return self._opensearch_client.get_index_json()

    def export_data(self, file_path_prefix="migration_schema"):
        """
        Method to export data to S3
        """
        if not self._data_config.get('migrate_data', False):
            logger.info("Skipping data export as migrate_data is set to false")
            return False

        try:
            self._export_data_to_s3()

            # Generate separate data migration report
            data_report_path = f"{file_path_prefix}/data_migration_report.html"
            self._report.data_migration_report(data_report_path)
            logger.info(f"Data migration report generated at: {data_report_path}")

            return True
        except Exception as e:
            logger.error(f"Error exporting data to S3: {str(e)}")
            self._report.add_data_migration_error(str(e))

            # Generate report even if there was an error
            data_report_path = f"{file_path_prefix}/data_migration_report.html"
            self._report.data_migration_report(data_report_path)
            logger.info(f"Data migration report with errors generated at: {data_report_path}")

            return False

    def migrate(self, file_path_prefix="migration_schema"):
        report_path = f"{file_path_prefix}/schema_migration_report.html"

        # Perform schema migration if enabled
        if self._schema_config.get('migrate_schema', True):
            self.migrate_schema(file_path_prefix)
            logger.info(f"Schema migration report generated at: {report_path}")
        else:
            logger.info("Schema migration is disabled")

        if self._data_config and self._data_config.get('migrate_data', False):
            data_migrated = self.export_data(file_path_prefix)
            data_report_path = f"{file_path_prefix}/data_migration_report.html"
            logger.info(f"Data migration report available at: {data_report_path}")
            print(f"Data migration report available at: {data_report_path}")

            # Print summary statistics
            print(f"\nData Migration Summary:")
            print(f"  - Total documents in Solr: {self._report.data_migration_docs_total}")
            print(f"  - Documents successfully exported: {self._report.data_migration_docs_exported}")
            if self._report.data_migration_docs_total > 0:
                if self._report.data_migration_errors == 0:
                    print(f"  - Success rate: 100.0%")
                else:
                    success_rate = round((self._report.data_migration_docs_total - self._report.data_migration_errors) / self._report.data_migration_docs_total * 100, 2)
                    print(f"  - Success rate: {success_rate}%")
            print(f"  - Errors encountered: {self._report.data_migration_errors}")

        return self._opensearch_client.get_index_json()
