import pytest
import json
import os
from unittest.mock import Mock
from migrate.copy_field import CopyFieldHelper, CopyFieldException
import xmltodict
from ..utils.xml_converter import assert_dictionary_properties

import logging
logger = logging.getLogger(__name__)

class TestCopyFields:
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures"""
        self.all_fields = {
            "title": {
                "type": "text",
                "index": True,
                "store": True
            },
            "content": {
                "type": "text",
                "index": True,
                "store": True
            },
            "price": {
                "type": "float",
                "index": True,
                "store": True
            },
            "quantity": {
                "type": "integer",
                "index": True,
                "store": True
            },
            "stock": {
                "type": "integer",
                "index": True,
                "store": True
            }
        }
        
        self.copy_field_helper = CopyFieldHelper(self.all_fields)
        self.field_dir = os.path.join(os.path.dirname(__file__))

    def _test_copy_field_scenario(self, xml_file, json_file):
        """Helper method to test copy field scenarios"""
        fields_path = os.path.join(self.field_dir, xml_file)
        expected_path = os.path.join(self.field_dir, json_file)

        with open(fields_path, 'r') as f:
            xml_content = f.read()

        with open(expected_path, 'r') as f:
            expected_content = json.load(f)

        input_schema = xmltodict.parse(xml_content, attr_prefix='', cdata_key='#text')['schema']
        
        if isinstance(input_schema['copyField'], list):
            copy_fields = input_schema['copyField']
        else:
            copy_fields = [input_schema['copyField']]

        # Track processed fields
        processed_fields = {}

        for copy_field in copy_fields:
            try:
                src, src_def, dst, dst_def = self.copy_field_helper.map_copy_field(copy_field)
                
                logger.info(f"Processing copy field: {src} -> {dst}")
                logger.info(f"Source definition: {src_def}")
                logger.info(f"Destination definition: {dst_def}")

                # Store the latest definitions
                processed_fields[src] = src_def
                processed_fields[dst] = dst_def
                
                logger.info(f"Processing successful")
                
            except Exception as e:
                logger.error(f"Processing failed: {str(e)}")
                pytest.fail(f"Processing failed: {str(e)}")

        for field_name, expected_def in expected_content.items():
            try:
                assert_dictionary_properties(processed_fields[field_name], expected_def)
                logger.info(f"Verification successful for {field_name}")
            except Exception as e:
                logger.error(f"Verification failed for {field_name}: {str(e)}")
                pytest.fail(f"Verification failed for {field_name}: {str(e)}")


    def test_basic_copy_field(self):
        """Test basic copy field scenario"""
        self._test_copy_field_scenario(
            "basic_copy_fields.xml",
            "basic_mapped_fields.json"
        )

    def test_one_source_multiple_destinations(self):
        """Test one source copying to multiple destinations"""
        self._test_copy_field_scenario(
            "one_source_multiple_dest.xml",
            "one_source_multiple_dest.json"
        )

    def test_multiple_sources_one_destination(self):
        """Test multiple sources copying to one destination"""
        self._test_copy_field_scenario(
            "multiple_source_one_dest.xml",
            "multiple_source_one_dest.json"
        )



    def test_copy_field_exceptions(self):
        """Test exception handling in map_copy_field"""
        
        # Test for CopyFieldException (when source field doesn't exist)
        non_existent_copy_field = {
            "source": "non_existent_field",
            "dest": "title_copy"
        }
        
        with pytest.raises(CopyFieldException) as exc_info:
            self.copy_field_helper.map_copy_field(non_existent_copy_field)
        assert exc_info.value.name == "non_existent_field"
        assert exc_info.value.reason == "MappingNotFound"
        
        # Test for general Exception
        # Mock the source field definition to raise an exception during copy
        self.all_fields["title"] = Mock()
        self.all_fields["title"].copy.side_effect = ValueError("Test error")
        
        problematic_copy_field = {
            "source": "title",
            "dest": "title_copy"
        }
        
        with pytest.raises(CopyFieldException) as exc_info:
            self.copy_field_helper.map_copy_field(problematic_copy_field)
        assert exc_info.value.name == "title"
        assert isinstance(exc_info.value.reason, ValueError)
        assert exc_info.value.src_field == "title"

