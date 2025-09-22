import unittest
from unittest.mock import Mock, patch, MagicMock
import json
from migrate.solr2os_migrate import Solr2OSMigrate
from reports.report import Report


class TestDataMigration(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures"""
        self.mock_solr_client = Mock()
        self.mock_opensearch_client = Mock()
        self.schema_config = {'create_index': False}
        self.data_config = {
            'migrate_data': True,
            'region': 'us-east-1',
            's3_export_bucket': 'test-bucket',
            's3_export_prefix': 'solr-data/',
            'rows_per_page': 100,
            'max_rows': 1000
        }

    @patch('migrate.solr2os_migrate.boto3')
    @patch('migrate.solr2os_migrate.requests')
    def test_successful_data_migration(self, mock_requests, mock_boto3):
        """Test successful data migration with binary fields"""
        # Mock schema with binary fields
        schema = {
            'fieldTypes': [
                {'name': 'binary_type', 'class': 'solr.BinaryField'}
            ],
            'fields': [
                {'name': 'attachment', 'type': 'binary_type'},
                {'name': 'title', 'type': 'text'}
            ]
        }
        self.mock_solr_client.read_schema.return_value = schema
        self.mock_solr_client.get_config.return_value = {
            'host': 'http://localhost',
            'port': '8983',
            'collection': 'test'
        }

        # Mock successful responses
        count_response = Mock()
        count_response.json.return_value = {'response': {'numFound': 2}}
        
        # First batch with data
        data_response1 = Mock()
        data_response1.text = '{"response":{"docs":[{"id":"1","attachment":"UEsDBBQ","title":"test"}],"numFound":1},"nextCursorMark":"cursor2"}'
        
        # Second batch - same cursor (end of results)
        data_response2 = Mock()
        data_response2.text = '{"response":{"docs":[{"id":"2","title":"test2"}],"numFound":1},"nextCursorMark":"cursor2"}'
        
        mock_requests.get.side_effect = [count_response, data_response1, data_response2]
        
        # Mock S3 client
        mock_s3 = Mock()
        mock_boto3.session.Session.return_value.client.return_value = mock_s3

        # Create migrator and run
        migrator = Solr2OSMigrate(self.mock_solr_client, self.mock_opensearch_client, 
                                 self.schema_config, self.data_config)
        
        result = migrator.export_data()
        
        # Assertions
        self.assertTrue(result)
        self.assertEqual(migrator._report.data_migration_errors, 0)
        self.assertEqual(migrator._report.data_migration_docs_total, 2)
        self.assertEqual(mock_s3.put_object.call_count, 2)

    @patch('migrate.solr2os_migrate.boto3')
    @patch('migrate.solr2os_migrate.requests')
    def test_json_parsing_error(self, mock_requests, mock_boto3):
        """Test JSON parsing error handling"""
        # Mock schema
        self.mock_solr_client.read_schema.return_value = {'fieldTypes': [], 'fields': []}
        self.mock_solr_client.get_config.return_value = {
            'host': 'http://localhost',
            'port': '8983',
            'collection': 'test'
        }

        # Mock responses - count works, but data response is invalid JSON
        count_response = Mock()
        count_response.json.return_value = {'response': {'numFound': 1}}
        
        data_response = Mock()
        data_response.text = 'invalid json {{{'
        
        mock_requests.get.side_effect = [count_response, data_response]
        
        # Mock S3 client
        mock_s3 = Mock()
        mock_boto3.session.Session.return_value.client.return_value = mock_s3

        # Create migrator and run
        migrator = Solr2OSMigrate(self.mock_solr_client, self.mock_opensearch_client, 
                                 self.schema_config, self.data_config)
        
        result = migrator.export_data()
        
        # Assertions
        self.assertTrue(result)
        self.assertGreater(migrator._report.data_migration_errors, 0)
        self.assertIn("JSON parsing error", str(migrator._report.data_migration_error_list))
        mock_s3.put_object.assert_not_called()

    @patch('migrate.solr2os_migrate.boto3')
    @patch('migrate.solr2os_migrate.requests')
    def test_binary_field_json_error(self, mock_requests, mock_boto3):
        """Test binary field with unquoted Base64 that can't be fixed"""
        # Mock schema with binary fields
        schema = {
            'fieldTypes': [{'name': 'binary_type', 'class': 'solr.BinaryField'}],
            'fields': [{'name': 'attachment', 'type': 'binary_type'}]
        }
        self.mock_solr_client.read_schema.return_value = schema
        self.mock_solr_client.get_config.return_value = {
            'host': 'http://localhost', 'port': '8983', 'collection': 'test'
        }

        # Mock responses
        count_response = Mock()
        count_response.json.return_value = {'response': {'numFound': 1}}
        
        # Response with completely broken JSON that can't be fixed
        data_response = Mock()
        data_response.text = 'completely broken json { [ } invalid'
        
        mock_requests.get.side_effect = [count_response, data_response]
        
        # Mock S3 client
        mock_s3 = Mock()
        mock_boto3.session.Session.return_value.client.return_value = mock_s3

        # Create migrator and run
        migrator = Solr2OSMigrate(self.mock_solr_client, self.mock_opensearch_client, 
                                 self.schema_config, self.data_config)
        
        result = migrator.export_data()
        
        # Assertions
        self.assertTrue(result)
        self.assertGreater(migrator._report.data_migration_errors, 0)
        self.assertIn("JSON parsing error", str(migrator._report.data_migration_error_list))

    @patch('migrate.solr2os_migrate.boto3')
    @patch('migrate.solr2os_migrate.requests')
    def test_request_error(self, mock_requests, mock_boto3):
        """Test request error handling"""
        # Mock schema
        self.mock_solr_client.read_schema.return_value = {'fieldTypes': [], 'fields': []}
        self.mock_solr_client.get_config.return_value = {
            'host': 'http://localhost',
            'port': '8983',
            'collection': 'test'
        }
        
        # Mock request failure
        mock_requests.get.side_effect = Exception("Connection failed")

        # Create migrator and run
        migrator = Solr2OSMigrate(self.mock_solr_client, self.mock_opensearch_client, 
                                 self.schema_config, self.data_config)
        
        result = migrator.export_data()
        
        # Assertions
        self.assertFalse(result)
        self.assertGreater(migrator._report.data_migration_errors, 0)

    @patch('migrate.solr2os_migrate.boto3')
    @patch('migrate.solr2os_migrate.requests')
    def test_binary_field_missing_name(self, mock_requests, mock_boto3):
        """Test binary field type with missing name"""
        # Mock schema with binary field type missing name
        schema = {
            'fieldTypes': [
                {'class': 'solr.BinaryField'},  # Missing 'name'
                {'name': 'text_type', 'class': 'solr.TextField'}
            ],
            'fields': [
                {'name': 'title', 'type': 'text_type'}
            ]
        }
        self.mock_solr_client.read_schema.return_value = schema
        self.mock_solr_client.get_config.return_value = {
            'host': 'http://localhost', 'port': '8983', 'collection': 'test'
        }

        # Mock successful responses
        count_response = Mock()
        count_response.json.return_value = {'response': {'numFound': 1}}
        
        data_response = Mock()
        data_response.text = '{"response":{"docs":[{"id":"1","title":"test"}],"numFound":1},"nextCursorMark":"same"}'
        
        mock_requests.get.side_effect = [count_response, data_response]
        
        # Mock S3 client
        mock_s3 = Mock()
        mock_boto3.session.Session.return_value.client.return_value = mock_s3

        # Create migrator and run
        migrator = Solr2OSMigrate(self.mock_solr_client, self.mock_opensearch_client,
                                 self.schema_config, self.data_config)
        
        result = migrator.export_data()
        
        # Assertions
        self.assertTrue(result)
        self.assertEqual(migrator._report.data_migration_errors, 0)
        mock_s3.put_object.assert_called_once()

    def test_get_binary_fields_helper(self):
        """Test the new _get_binary_fields helper method"""
        # Mock schema with binary fields
        schema = {
            'fieldTypes': [
                {'name': 'binary_type', 'class': 'solr.BinaryField'},
                {'name': 'text_type', 'class': 'solr.TextField'}
            ],
            'fields': [
                {'name': 'attachment', 'type': 'binary_type'},
                {'name': 'title', 'type': 'text_type'}
            ]
        }
        self.mock_solr_client.read_schema.return_value = schema
        
        # Create migrator
        migrator = Solr2OSMigrate(self.mock_solr_client, self.mock_opensearch_client,
                                 self.schema_config, self.data_config)
        
        # Test helper method
        binary_fields = migrator._get_binary_fields()
        
        # Assertions
        self.assertEqual(binary_fields, ['attachment'])

    def test_fix_binary_fields_in_json_helper(self):
        """Test the new _fix_binary_fields_in_json helper method"""
        # Create migrator
        migrator = Solr2OSMigrate(self.mock_solr_client, self.mock_opensearch_client,
                                 self.schema_config, self.data_config)
        
        # Test JSON with unquoted binary field
        json_text = '{"id":"1","attachment":UEsDBBQ,"title":"test"}'
        binary_fields = ['attachment']
        
        # Fix the JSON
        fixed_json = migrator._fix_binary_fields_in_json(json_text, binary_fields)
        
        # Assertions
        expected = '{"id":"1","attachment":"UEsDBBQ","title":"test"}'
        self.assertEqual(fixed_json, expected)

    def test_fix_binary_fields_empty_list(self):
        """Test _fix_binary_fields_in_json with empty binary fields list"""
        # Create migrator
        migrator = Solr2OSMigrate(self.mock_solr_client, self.mock_opensearch_client,
                                 self.schema_config, self.data_config)
        
        # Test JSON with no binary fields to fix
        json_text = '{"id":"1","title":"test"}'
        binary_fields = []
        
        # Fix the JSON (should return unchanged)
        fixed_json = migrator._fix_binary_fields_in_json(json_text, binary_fields)
        
        # Assertions
        self.assertEqual(fixed_json, json_text)

    def test_data_migration_disabled(self):
        """Test when data migration is disabled"""
        data_config = self.data_config.copy()
        data_config['migrate_data'] = False
        
        migrator = Solr2OSMigrate(self.mock_solr_client, self.mock_opensearch_client,
                                 self.schema_config, data_config)
        
        result = migrator.export_data()
        
        # Should return False when migration is disabled
        self.assertFalse(result)


    @patch('migrate.solr2os_migrate.boto3')
    @patch('migrate.solr2os_migrate.requests')
    def test_empty_batch_handling(self, mock_requests, mock_boto3):
        """Test handling of empty batch response"""
        # Mock schema
        self.mock_solr_client.read_schema.return_value = {'fieldTypes': [], 'fields': []}
        self.mock_solr_client.get_config.return_value = {
            'host': 'http://localhost',
            'port': '8983',
            'collection': 'test'
        }

        # Mock responses
        count_response = Mock()
        count_response.json.return_value = {'response': {'numFound': 1}}
        
        # Empty batch response
        data_response = Mock()
        data_response.text = '{"response":{"docs":[],"numFound":0},"nextCursorMark":"same"}'
        
        mock_requests.get.side_effect = [count_response, data_response]
        
        # Mock S3 client
        mock_s3 = Mock()
        mock_boto3.session.Session.return_value.client.return_value = mock_s3

        # Create migrator and run
        migrator = Solr2OSMigrate(self.mock_solr_client, self.mock_opensearch_client,
                                 self.schema_config, self.data_config)
        
        result = migrator.export_data()
        
        # Assertions
        self.assertTrue(result)
        self.assertEqual(migrator._report.data_migration_errors, 0)
        mock_s3.put_object.assert_not_called()  # No docs to export


if __name__ == '__main__':
    unittest.main()