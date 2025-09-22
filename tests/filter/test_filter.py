import sys
import os
import pytest
import json
import xmltodict
from migrate.filters import FilterHelper, FilterException, CharFilterException
from unittest.mock import Mock
from ..utils.xml_converter import assert_dictionary_properties
import logging

logger = logging.getLogger(__name__)

class TestFilters:

    CHAR_FILTER_TYPES = [
        'htmlStrip',
        'mapping',
        'patternReplace'
    ]

    FILTER_TYPES = [
        'asciiFolding',
        'classic',
        'doubleMetaphone',
        'commonGrams',
        'englishMinimalStem',
        'edgeNGram',
        'fingerprint',
        'keepWord',
        'kStem',
        'length',
        'limitTokenCount',
        'lowercase',
        # 'minHash',
        'ngram',
        'phonetic',
        'patternReplace',
        'porterStem',
        'removeDuplicates',
        'shingle',
        'snowbalPorter',
        'synonym',
        'synonymGraph',
        'wordDelimiterGraph',
        'trim',
    ]

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup the test fixtures."""
        self.solr_client = Mock()
        self.solr_client.get_collection.return_value = "test_collection"
        self.solr_client.get_solr_file_data.return_value = "line1\nline2\nline3"
        self.opensearch_client = Mock()

        self.migration_config = {
            "create_package": False,
            "expand_files_array": True
        }
        self.filter_helper = FilterHelper(
            self.solr_client,
            self.opensearch_client,
            self.migration_config
        )
        self.filter_dir = os.path.join(os.path.dirname(__file__))

    @pytest.mark.parametrize("charfilter_type", CHAR_FILTER_TYPES)
    def test_charfilter_class(self, charfilter_type):
        print(f"Testing charFilter: {charfilter_type}")
        charfilter_path = os.path.join(self.filter_dir, "char_filters", charfilter_type, "class_filter.xml")
        expected_path = os.path.join(self.filter_dir, "char_filters", charfilter_type, "mapped_filter.json")

        with open(charfilter_path, 'r') as f:
            xml_content = f.read()

        with open(expected_path, 'r') as f:
            expected_content = json.load(f)

        input_schema = xmltodict.parse(xml_content, attr_prefix='', cdata_key='#text')['schema']
        field_types = input_schema['fieldType'] if isinstance(input_schema['fieldType'], list) else [input_schema['fieldType']]

        field_type_charFilter_dict = [{
            "name": field['name'],
            "charFilter": field['analyzer']['charFilter']
        } for field in field_types]

        for t in field_type_charFilter_dict:
            result = self.filter_helper.map_char_filters([t['charFilter']])
            result_definition = result[0].get_definition()
            assert_dictionary_properties(result_definition, expected_content.get(t['name']))

    @pytest.mark.parametrize("charfilter_type", CHAR_FILTER_TYPES)
    def test_charfilter_name(self, charfilter_type):
        print(f"Testing charFilter: {charfilter_type}")
        charfilter_path = os.path.join(self.filter_dir, "char_filters", charfilter_type, "name_filter.xml")
        expected_path = os.path.join(self.filter_dir, "char_filters", charfilter_type, "mapped_filter.json")

        with open(charfilter_path, 'r') as f:
            xml_content = f.read()

        with open(expected_path, 'r') as f:
            expected_content = json.load(f)

        input_schema = xmltodict.parse(xml_content, attr_prefix='', cdata_key='#text')['schema']
        field_types = input_schema['fieldType'] if isinstance(input_schema['fieldType'], list) else [input_schema['fieldType']]

        field_type_charFilter_dict = [{
            "name": field['name'],
            "charFilter": field['analyzer']['charFilter']
        } for field in field_types]

        for t in field_type_charFilter_dict:
            result = self.filter_helper.map_char_filters([t['charFilter']])
            result_definition = result[0].get_definition()
            assert_dictionary_properties(result_definition, expected_content.get(t['name']))

    @pytest.mark.parametrize("filter_type", FILTER_TYPES)
    def test_filter_name(self, filter_type):
        print(f"Testing filter: {filter_type}")
        filter_path = os.path.join(self.filter_dir, "filters", filter_type, "name_filter.xml")
        expected_path = os.path.join(self.filter_dir, "filters", filter_type, "mapped_filter.json")

        with open(filter_path, 'r') as f:
            xml_content = f.read()

        with open(expected_path, 'r') as f:
            expected_content = json.load(f)

        input_schema = xmltodict.parse(xml_content, attr_prefix='', cdata_key='#text')['schema']
        field_types = input_schema['fieldType'] if isinstance(input_schema['fieldType'], list) else [input_schema['fieldType']]

        field_type_filter_dict = [{
            "name": field['name'],
            "filter": field['analyzer']['filter']
        } for field in field_types]

        for t in field_type_filter_dict:
            result = self.filter_helper.map_filters([t['filter']])
            result_definition = result[0].get_definition()
            assert_dictionary_properties(result_definition, expected_content.get(t['name']))

    @pytest.mark.parametrize("filter_type", FILTER_TYPES)
    def test_filter_class(self, filter_type):
        print(f"Testing filter: {filter_type}")
        filter_path = os.path.join(self.filter_dir, "filters", filter_type, "class_filter.xml")
        expected_path = os.path.join(self.filter_dir, "filters", filter_type, "mapped_filter.json")

        with open(filter_path, 'r') as f:
            xml_content = f.read()

        with open(expected_path, 'r') as f:
            expected_content = json.load(f)

        input_schema = xmltodict.parse(xml_content, attr_prefix='', cdata_key='#text')['schema']
        field_types = input_schema['fieldType'] if isinstance(input_schema['fieldType'], list) else [input_schema['fieldType']]

        field_type_filter_dict = [{
            "name": field['name'],
            "filter": field['analyzer']['filter']
        } for field in field_types]

        for t in field_type_filter_dict:
            result = self.filter_helper.map_filters([t['filter']])
            result_definition = result[0].get_definition()
            assert_dictionary_properties(result_definition, expected_content.get(t['name']))

    @pytest.mark.parametrize("invalid_charfilter", [{"name": "nonExistentCharFilter"},])  # Invalid char filter with no mapping
    def test_invalid_charfilter(self, invalid_charfilter):
        """Test for invalid char filter mapping"""
        with pytest.raises(CharFilterException) as excinfo:
            self.filter_helper.map_char_filters([invalid_charfilter])

        # Verify exception details
        assert excinfo.value.name == invalid_charfilter['name'].lower()
        assert excinfo.value.reason == "MappingNotFound"
        logger.info(f"Invalid char filter test passed for: {invalid_charfilter['name']}")


    @pytest.mark.parametrize("invalid_filter", [{"name": "nonExistentFilter"},])  # Invalid filter with no mapping
    def test_invalid_filter(self, invalid_filter):
        """Test for invalid filter mapping"""
        with pytest.raises(FilterException) as excinfo:
            self.filter_helper.map_filters([invalid_filter])

        # Verify exception details
        assert excinfo.value.name == invalid_filter['name'].lower()
        assert excinfo.value.reason == "MappingNotFound"
        logger.info(f"Invalid filter test passed for: {invalid_filter['name']}")


