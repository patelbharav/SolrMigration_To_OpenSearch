import os
import pytest
import json
from unittest.mock import Mock, MagicMock, patch
from migrate.fields import FieldHelper, FieldException
import xmltodict
from ..utils.xml_converter import assert_dictionary_properties

import logging
logger = logging.getLogger(__name__)

class TestFields:
    @pytest.fixture(autouse=True)
    def setup_and_cleanup(self):
        """Setup and cleanup for all tests"""
        # Store mocks for cleanup
        self.mocks = []
        
        # Setup
        self.solr_client = self._create_mock('solr_client')
        self.opensearch_client = self._create_mock('opensearch_client')
        self.field_type_helper = self._create_mock('field_type_helper')

        # Define field type mapping
        self.field_type_mappings = {
            'text_general': 'text',
            'string': 'keyword',
            'int': 'integer',
            'long': 'long',
            'date': 'date',
            'boolean': 'boolean',
            'float': 'float',
            'double': 'double',
            'binary_field': 'binary',
            'nested_path': 'nested',
            'point_field': 'xy_type',
            'spatial_rpt': 'geo_shape'
        }
            
        # Mock the get_field_type method
        self.field_type_helper.get_field_type.side_effect = lambda x: self.field_type_mappings.get(x)

        # Create MagicMock with specific name and store it
        get_analyzers_mock = self._create_mock('get_all_analyzers', return_value=[])
        self.opensearch_client.get_all_analyzers = get_analyzers_mock

        self.field_helper = FieldHelper(self.solr_client, self.opensearch_client, self.field_type_helper)
        self.field_dir = os.path.join(os.path.dirname(__file__))

        yield

        # Cleanup
        self._cleanup_mocks()

    def _create_mock(self, name, **kwargs):
        """Create and track a new mock"""
        mock = MagicMock(name=name, **kwargs)
        self.mocks.append(mock)
        return mock

    def _cleanup_mocks(self):
        """Cleanup all tracked mocks"""
        try:
            for mock in self.mocks:
                mock.reset_mock()
                del mock
            self.mocks.clear()
            patch.stopall()
        except Exception as e:
            logger.error(f"Error during mock cleanup: {e}")

    @pytest.fixture
    def test_data(self):
        """Fixture for loading test data"""
        fields_path = os.path.join(self.field_dir, "fields.xml")
        expected_path = os.path.join(self.field_dir, "mapped_fields.json")

        with open(fields_path, 'r') as f:
            xml_content = f.read()

        with open(expected_path, 'r') as f:
            expected_content = json.load(f)

        return {
            'xml_content': xml_content,
            'expected_content': expected_content
        }

    def test_fields(self, test_data):
        """Test mapping for fields"""
        try:
            input_schema = xmltodict.parse(
                test_data['xml_content'], 
                attr_prefix='', 
                cdata_key='#text'
            )['schema']
            
            fields = (
                input_schema['fields']['field'] 
                if isinstance(input_schema['fields']['field'], list) 
                else [input_schema['fields']['field']]
            )

            for field in fields:
                try:
                    field_name = field['name']
                    mapped_name, mapped_attrs = self.field_helper.map_field(field)
                    assert mapped_name == field_name
                    assert_dictionary_properties(
                        mapped_attrs, 
                        test_data['expected_content'][field_name]
                    )
                    logger.info(f"Field mapping successful for: {field_name}")
                except FieldException as e:
                    logger.error(f"Field mapping failed for {field_name}: {e.reason}")
                    pytest.fail(f"Field mapping failed for {field_name}: {e.reason}")
                except Exception as e:
                    logger.error(f"Unexpected error: {str(e)}")
                    pytest.fail(f"Unexpected error: {str(e)}")
        finally:
            self._cleanup_mocks()

    def test_unknown_field_type(self):
        """Test handling of unknown field type"""
        try:
            test_field = {
                "name": "test_field",
                "type": "unknown_type",
                "indexed": "true",
                "stored": "true"
            }

            with pytest.raises(FieldException) as exc_info:
                self.field_helper.map_field(test_field)

            exception = exc_info.value
            assert exception.name == "test_field"
            assert exception.reason == "MappingNotFound"
            assert exception.field_type is None
            logger.info("Successfully caught FieldException for unknown field type")
        finally:
            self._cleanup_mocks()

    def test_extra_attributes(self, caplog):
        """Test logging of unknown/extra attributes"""
        try:
            caplog.set_level(logging.INFO)

            test_field = {
                "name": "test_field",
                "type": "text_general",
                "indexed": "true",
                "stored": "true",
                "extra_attr1": "value1",
                "unknown_attr": "value2"
            }

            mapped_name, mapped_attrs = self.field_helper.map_field(test_field)

            # Verify logging
            assert any("Unknown attrs" in record.message for record in caplog.records)
            assert any("extra_attr1" in record.message for record in caplog.records)
            assert any("unknown_attr" in record.message for record in caplog.records)

            # Verify attributes not in mapped result
            assert "extra_attr1" not in mapped_attrs
            assert "unknown_attr" not in mapped_attrs

            assert mapped_name == "test_field"

            # Verify correct mapping of valid attributes
            assert mapped_attrs["type"] == "text"
            assert mapped_attrs["index"] == "true"
            assert mapped_attrs["store"] == "true"

            logger.info("Successfully verified extra attributes logging")
        finally:
            self._cleanup_mocks()
