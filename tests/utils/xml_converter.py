# xml_converter.py
import xml.etree.ElementTree as ET
from typing import Dict, Any


def assert_dictionary_properties(result, expected):
    for key, value in expected.items():
        actual_value = result[key]
        if isinstance(value, dict):
            # Recursively check nested dictionaries
            assert_dictionary_properties(actual_value, value)
        else:
            if isinstance(value, bool) and isinstance(actual_value, str):
                actual_value = actual_value.lower() == 'true'  # Check boolean first
            elif isinstance(value, int) and isinstance(actual_value, str):
                actual_value = int(actual_value)  # Then check integer
            assert actual_value == value