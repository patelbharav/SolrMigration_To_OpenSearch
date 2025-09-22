import pytest
import json
import os
from unittest.mock import Mock, MagicMock, patch
from migrate.dynamic_field import DynamicFieldHelper, DynamicFieldException
import xmltodict
from ..utils.xml_converter import assert_dictionary_properties

import logging
logger = logging.getLogger(__name__)

class TestDynamicFields:
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
            'double': 'double'
        }
            
        self.field_type_helper.get_field_type.side_effect = lambda x: self.field_type_mappings.get(x)

        # Create MagicMock with specific name and store it
        get_analyzers_mock = self._create_mock('get_all_analyzers', return_value=[])
        self.opensearch_client.get_all_analyzers = get_analyzers_mock

        self.dynamic_field_helper = DynamicFieldHelper(
            self.solr_client, 
            self.opensearch_client, 
            self.field_type_helper
        )
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
        except Exception as e:
            logger.error(f"Error during mock cleanup: {e}")

    @pytest.fixture
    def test_data(self):
        """Fixture for loading test data"""
        fields_path = os.path.join(self.field_dir, "dynamic_fields.xml")
        expected_path = os.path.join(self.field_dir, "mapped_dynamic_fields.json")

        with open(fields_path, 'r') as f:
            xml_content = f.read()

        with open(expected_path, 'r') as f:
            expected_content = json.load(f)

        return {
            'xml_content': xml_content,
            'expected_content': expected_content
        }

    def test_dynamic_fields(self, test_data):
        """Test mapping for dynamic fields"""
        try:
            input_schema = xmltodict.parse(
                test_data['xml_content'], 
                attr_prefix='', 
                cdata_key='#text'
            )['schema']
            
            dynamic_fields = (
                input_schema['dynamicField'] 
                if isinstance(input_schema['dynamicField'], list) 
                else [input_schema['dynamicField']]
            )

            for dynamic_field in dynamic_fields:
                try:
                    result = self.dynamic_field_helper.map_dynamic_field(dynamic_field)
                    pattern = dynamic_field['name']
                    
                    logger.info(f"Dynamic field: {dynamic_field}")
                    logger.info(f"Mapped result: {result}")

                    assert_dictionary_properties(
                        result[pattern], 
                        test_data['expected_content'][pattern]
                    )
                    logger.info(f"Dynamic field mapping successful for: {pattern}")
                    
                except DynamicFieldException as e:
                    logger.error(f"Dynamic field mapping failed for {e.name}: {e.reason}")
                    pytest.fail(f"Dynamic field mapping failed for {e.name}: {e.reason}")
                except Exception as e:
                    logger.error(f"Unexpected error: {str(e)}")
                    pytest.fail(f"Unexpected error: {str(e)}")
        finally:
            # Ensure mocks are cleaned up even if test fails
            self._cleanup_mocks()

    def test_unknown_field_type(self):
        """Test handling of unknown field type"""
        try:
            test_field = {
                "name": "*_unknown",
                "type": "unknown_type",
                "indexed": "true",
                "stored": "true"
            }

            with pytest.raises(DynamicFieldException) as exc_info:
                self.dynamic_field_helper.map_dynamic_field(test_field)

            exception = exc_info.value
            assert exception.name == "*_unknown"
            assert exception.reason == "MappingNotFound"
            assert exception.field_type == "unknown_type"
            logger.info("Successfully caught DynamicFieldException for unknown field type")
        finally:
            # Ensure mocks are cleaned up even if test fails
            self._cleanup_mocks()



    def test_map_dynamic_field_generic_exception_branch(self):
        # Create a dynamic field with an extra unexpected attribute.
        test_field = {
            "name": "foo",
            "type": "text_general",
            "indexed": "true",
            "stored": "true",
            "unexpected": "value"  # This extra attribute will trigger the problematic code.
        }
        with pytest.raises(DynamicFieldException) as exc_info:
            self.dynamic_field_helper.map_dynamic_field(test_field)
        
        # The underlying exception (AttributeError) should be wrapped in the DynamicFieldException.
        assert "has no attribute" in str(exc_info.value.reason)
        assert exc_info.value.name == "foo"
        assert exc_info.value.field_type == "text_general"


    def test_map_dynamic_field_dynamic_exception_branch(self):
        # Create a dynamic subclass of dict with a faulty items method.
        FaultyDict = type("FaultyDict", (dict,), {})
        
        def faulty_items(self):
            raise DynamicFieldException(
                name=self.get("name", "foo"),
                reason="Simulated dynamic field exception",
                field_type=self.get("type", "text_general")
            )
        FaultyDict.items = faulty_items

        # Instantiate the faulty dictionary.
        test_field = FaultyDict({
            "name": "foo",
            "type": "text_general",
            "indexed": "true",
            "stored": "true"
        })
        
        with pytest.raises(DynamicFieldException) as exc_info:
            self.dynamic_field_helper.map_dynamic_field(test_field)

        # The exception should be the one we simulated.
        assert "Simulated dynamic field exception" in str(exc_info.value.reason)
        assert exc_info.value.name == "foo"
        assert exc_info.value.field_type == "text_general"







