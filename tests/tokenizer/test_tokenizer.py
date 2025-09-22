import sys
import os
import pytest
import json
from migrate.tokenizer import TokenizerHelper, TokenizerException
import xmltodict, json

from ..utils.xml_converter import assert_dictionary_properties

from unittest.mock import Mock
import logging
logger = logging.getLogger(__name__)

class TestTokenizer:
    # List of all tokenizer types
    TOKENIZER_TYPES = [
        'standard',
        'classic',
        'letter',
        'keyword',
        'nGram',
        'edgeNGram',
        'pathHierarchy',
        'pattern',
        'simplePattern',
        'simplePatternSplit',
        'uax29URLEmail',
        'whitespace'
     ]

    @pytest.fixture(autouse=True)
    def setup(self):
        self.solr_client = Mock()
        self.opensearch_client = Mock()
        self.tokenizer_helper = TokenizerHelper(self.solr_client, self.opensearch_client)
        self.tokenizer_dir = os.path.join(os.path.dirname(__file__))



    @pytest.mark.parametrize("tokenizer_type",TOKENIZER_TYPES)
    def test_tokenizer_name(self, tokenizer_type):

        print(f"Testing tokenizer: {tokenizer_type}")
        tokenizer_path = os.path.join(self.tokenizer_dir, f"{tokenizer_type}", "name_tokenizer.xml")
        expected_path  = os.path.join(self.tokenizer_dir, f"{tokenizer_type}", "mapped_tokenizer.json")

        with open(tokenizer_path, 'r') as f:
            xml_content = f.read()
            f.close()

        with open(expected_path, 'r') as f:
            expected_content = json.load(f)
            f.close()

        input_schema = xmltodict.parse(xml_content,  attr_prefix='', cdata_key='#text')['schema']
        logger.info(f"input_schema: {input_schema}")
        # Ensure fieldType is always a list
        field_types = input_schema['fieldType'] if isinstance(input_schema['fieldType'], list) else [input_schema['fieldType']]
        field_type_tokenizer_dict = [{
            "name": field['name'],
            "tokenizer": field['analyzer']['tokenizer'] } for field in field_types]

        for t in field_type_tokenizer_dict:
            result = self.tokenizer_helper.map_tokenizer(t['tokenizer'])
            assert_dictionary_properties(result.get_definition(), expected_content.get(t['name']))


    @pytest.mark.parametrize("tokenizer_type",TOKENIZER_TYPES)
    def test_tokenizer_class(self, tokenizer_type):

        print(f"Testing tokenizer: {tokenizer_type}")
        tokenizer_path = os.path.join(self.tokenizer_dir, f"{tokenizer_type}", "class_tokenizer.xml")
        expected_path  = os.path.join(self.tokenizer_dir, f"{tokenizer_type}", "mapped_tokenizer.json")

        with open(tokenizer_path, 'r') as f:
            xml_content = f.read()
            f.close()

        with open(expected_path, 'r') as f:
            expected_content = json.load(f)
            f.close()

        input_schema = xmltodict.parse(xml_content,  attr_prefix='', cdata_key='#text')['schema']
        logger.info(f"input_schema: {input_schema}")
         # Ensure fieldType is always a list
        field_types = input_schema['fieldType'] if isinstance(input_schema['fieldType'], list) else [input_schema['fieldType']]
        field_type_tokenizer_dict = [{
            "name": field['name'],
            "tokenizer": field['analyzer']['tokenizer'] } for field in field_types]

        for t in field_type_tokenizer_dict:
            result = self.tokenizer_helper.map_tokenizer(t['tokenizer'])
            assert_dictionary_properties(result.get_definition(), expected_content.get(t['name']))



    def test_invalid_mapping(self):
        """Test for invalid tokenizer mapping"""
        # Define an invalid tokenizer with a missing mapping
        invalid_tokenizer = {"name": "invalidTokenizer"}

        with pytest.raises(TokenizerException) as excinfo:
            self.tokenizer_helper.map_tokenizer(invalid_tokenizer)

        # Verify the exception details
        assert excinfo.value.name == "invalidtokenizer"
        assert excinfo.value.reason == "MappingNotFound"
        logger.info("Test for invalid mapping passed successfully.")
