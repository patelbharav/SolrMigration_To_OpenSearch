import sys
import os
import toml
import pytest
import traceback
import json
import xmltodict
import logging
from unittest.mock import Mock, MagicMock, patch
from migrate.fieldtype import FieldTypeHelper, FieldTypeException
from ..utils.xml_converter import assert_dictionary_properties

logger = logging.getLogger(__name__)

def pluralize(path, key, value):
    if key == 'filter':
        return key + 's', value
    if key == 'analyzer':
        if "type" in value.keys():
            if value['type'] == 'index':
                return "indexAnalyzer", value
            if value['type'] == 'query':
                return "queryAnalyzer", value
    return key, value

class TestFieldType:
    FIELD_TYPES = [
        'TextField', 'NestPathField', 'StrField', 'BoolField',
        'IntPointField', 'LongPointField', 'FloatPointField',
        'PointType', 'RandomSortField', 'SortableTextField',
        'DoublePointField', 'DatePointField', 'BinaryField',
        'LatLonPointSpatialField', 'SpatialRecursivePrefixTreeFieldType'
    ]

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        """Setup and teardown for tests"""
        try:
            # Setup
            self.solr_client = self._create_mock('solr_client')
            self.opensearch_client = self._create_mock('opensearch_client')
            
            self.migration_config = {
                "create_package": False,
                "expand_files_array": False
            }
            
            self.field_type_helper = FieldTypeHelper(
                self.solr_client,
                self.opensearch_client,
                self.migration_config
            )
            
            # Path to the current test directory
            self.field_type_dir = os.path.dirname(os.path.abspath(__file__))
            
            yield
            
        finally:
            # Teardown
            self._cleanup_mocks()

    def _create_mock(self, name, **kwargs):
        """Create and track a new mock"""
        try:
            mock = MagicMock(name=name, **kwargs)
            
            # Set default string representation
            mock.__str__ = MagicMock(return_value=f"Mock_{name}")
            mock.__repr__ = MagicMock(return_value=f"Mock_{name}")
            
            # Set default return values for common methods
            if name == 'solr_client':
                mock.get_collection.return_value = "test_collection"
                mock.get_schema.return_value = {}
                mock.get_field_types.return_value = []
                mock.get_fields.return_value = []
                mock.get_dynamic_fields.return_value = []
                mock.get_copy_fields.return_value = []
            elif name == 'opensearch_client':
                mock.get_index.return_value = "test_index"
                mock.get_mapping.return_value = {}
                mock.get_settings.return_value = {}
                
            return mock
        except Exception as e:
            logger.error(f"Error creating mock {name}: {e}")
            raise

    def _cleanup_mocks(self):
        """Clean up mock objects"""
        try:
            # Reset mocks
            if hasattr(self, 'solr_client'):
                self.solr_client.reset_mock()
                self.solr_client = None
            if hasattr(self, 'opensearch_client'):
                self.opensearch_client.reset_mock()
                self.opensearch_client = None
            if hasattr(self, 'field_type_helper'):
                self.field_type_helper = None
                
        except Exception as e:
            logger.error(f"Error during mock cleanup: {e}")
            logger.error(traceback.format_exc())

    def _load_test_files(self, field_type):
        """Load test files for a given field type"""
        field_type_path = os.path.join(self.field_type_dir, f"{field_type}", "class_fieldtype.xml")
        expected_path = os.path.join(self.field_type_dir, f"{field_type}", "mapped_fieldtype.json")

        with open(field_type_path, 'r') as f:
            xml_content = f.read()

        with open(expected_path, 'r') as f:
            expected_content = json.load(f)

        return xml_content, expected_content

    @pytest.mark.parametrize("field_type", FIELD_TYPES)
    def test_field_type(self, field_type):
        """Test field type mapping"""
        try:
            logger.info(f"Testing field type: {field_type}")
            
            xml_content, expected_content = self._load_test_files(field_type)
            
            input_schema = xmltodict.parse(
                xml_content,
                attr_prefix='',
                force_list='fieldType, filters',
                postprocessor=pluralize
            )['schema']

            for field_type_def in input_schema['fieldType']:
                field_type_name = field_type_def['name']
                logger.info(f"Testing field type: {field_type_name}")
                try:
                    analyzer = self.field_type_helper.map_field_type_analyzer(field_type_def)
                    assert (
                        self.field_type_helper.get_field_type(field_type_name) == 
                        expected_content[field_type_name]['type']
                    )
                except Exception as e:
                    logger.error(f"Error mapping field type: {field_type_name}")
                    traceback.print_exc()
                    pytest.fail(f"Test failed: {str(e)}")
        except Exception as e:
            logger.error(f"Test failed: {str(e)}")
            traceback.print_exc()
            pytest.fail(str(e))

    @pytest.mark.parametrize("field_type", ["CustomField"])
    def test_field_type_custom(self, field_type):
        """Test custom field type mapping"""
        try:
            logger.info(f"Testing field type: {field_type}")
            
            xml_content, expected_content = self._load_test_files(field_type)
            
            input_schema = xmltodict.parse(
                xml_content,
                attr_prefix='',
                force_list='fieldType, filters',
                postprocessor=pluralize
            )['schema']

            result = {}
            for field_type_def in input_schema['fieldType']:
                field_type_name = field_type_def['name']
                try:
                    analyzers = self.field_type_helper.map_field_type_analyzer(field_type_def)
                    for analyzer in analyzers:
                        analyzer_definition = analyzer.get_analysis_definition()['analyzer']
                        result.update(analyzer_definition)
                except Exception as e:
                    logger.error(f"Error mapping field type: {field_type_name}")
                    traceback.print_exc()
                    pytest.fail(f"Test failed: {str(e)}")

            assert_dictionary_properties(result, expected_content['analyzer'])

        except Exception as e:
            logger.error(f"Test failed: {str(e)}")
            traceback.print_exc()
            pytest.fail(str(e))
