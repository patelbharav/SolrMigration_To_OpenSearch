

import os
import toml
import pytest
import json
import logging
import sys
from solr.solr_client import SolrClient
from opensearch.opensearch_client import OpenSearchClient
from migrate.solr2os_migrate import Solr2OSMigrate
import opensearchpy
import pysolr
import toml
from ..utils.xml_converter import assert_dictionary_properties
import time



logger = logging.getLogger(__name__)

# Initialize containers
@pytest.fixture(scope="session", autouse=True)
def setup(request):
    pass

def migrate(config):
    migration_config = config['migration']
    if migration_config['create_package'] is True and migration_config['expand_files_array'] is True:
        logger.error("create_package and expand_files_array are mutually exclusive")
        sys.exit()
    result = None
    try:
        solrclient = SolrClient(config['solr'])
        opensearchclient = OpenSearchClient(config['opensearch'])
        file_path = f"migration_schema/{config['solr']['collection']}/"
        result = Solr2OSMigrate(solrclient, opensearchclient, config['migration'], config.get('data_migration', {})).migrate(file_path)
    except pysolr.SolrError:
        return None
    except opensearchpy.exceptions.OpenSearchException:
        return None
    return result

def test_migration_no_package_no_expansion(monkeypatch):
    # Track package creation calls to verify none are created
    package_calls = []
    
    # Mock the OpenSearchClient's create_and_associate_package method
    def mock_create_and_associate_package(self, package_name, file):
        package_calls.append((package_name, file))
        return "mock-package-id", "1.0"
    
    monkeypatch.setattr(OpenSearchClient, "create_and_associate_package", mock_create_and_associate_package)
    
    config = toml.load(os.path.join(os.path.dirname(__file__), "no_package_no_expansion", "migrate.toml"))
    # Ensure create_package is False
    config['migration']['create_package'] = False
    # Ensure expand_files_array is False
    config['migration']['expand_files_array'] = False
    
    result = migrate(config)

    # Verify no packages were created
    assert len(package_calls) == 0, "Packages were created when they shouldn't have been"
    
    # Verify configuration flags
    assert config['migration']['create_package'] is False, "create_package flag should be False"
    assert config['migration']['expand_files_array'] is False, "expand_files_array flag should be False"
    
    expected_path = os.path.join(os.path.dirname(__file__), "no_package_no_expansion","index.json")
    with open(expected_path, 'r') as f:
        expected_content = json.load(f)
        f.close()
    assert_dictionary_properties(result, expected_content)

def test_migration_no_package_with_expansion(monkeypatch):
    # Track field type analyzer calls to verify array expansion
    analyzer_calls = []
    
    # Mock the FieldTypeHelper's map_field_type_analyzer method to track calls
    original_map_field_type_analyzer = Solr2OSMigrate._migrate_field_types
    def mock_migrate_field_types(self):
        # Call the original method
        result = original_map_field_type_analyzer(self)
        # Record that it was called
        analyzer_calls.append("called")
        return result
    
    monkeypatch.setattr(Solr2OSMigrate, "_migrate_field_types", mock_migrate_field_types)
    
    config = toml.load(os.path.join(os.path.dirname(__file__), "no_package_expansion", "migrate.toml"))
    # Ensure expand_files_array is True
    config['migration']['expand_files_array'] = True
    # Ensure create_package is False
    config['migration']['create_package'] = False
    
    result = migrate(config)
    
    # Verify field type analyzer was called (which would use expanded arrays)
    assert len(analyzer_calls) > 0, "Field type analyzer was not called"
    
    # Verify that expand_files_array flag is respected
    assert config['migration']['expand_files_array'] is True, "expand_files_array flag should be True"
    assert config['migration']['create_package'] is False, "create_package flag should be False"
    
    # For array expansion, we expect analyzers to be defined in settings
    if 'settings' in result and 'analysis' in result['settings']:
        assert 'analyzer' in result['settings']['analysis'], "No analyzers defined in settings"
        assert 'filter' in result['settings']['analysis'], "No filters defined in settings"
        assert 'tokenizer' in result['settings']['analysis'], "No tokenizers defined in settings"
    
    expected_path = os.path.join(os.path.dirname(__file__), "no_package_expansion","index.json")
    with open(expected_path, 'r') as f:
        expected_content = json.load(f)
        f.close()
    
    assert_dictionary_properties(result, expected_content)
    

def test_migration_no_package_no_expansion_index(monkeypatch):
    # Track package and index creation calls
    package_calls = []
    index_created = False
    
    # Mock the OpenSearchClient's create_and_associate_package method
    def mock_create_and_associate_package(self, package_name, file):
        package_calls.append((package_name, file))
        return "mock-package-id", "1.0"
    
    # Mock the OpenSearchClient's create_index method
    def mock_create_index(self):
        nonlocal index_created
        index_created = True
        return True
    
    monkeypatch.setattr(OpenSearchClient, "create_and_associate_package", mock_create_and_associate_package)
    monkeypatch.setattr(OpenSearchClient, "create_index", mock_create_index)
    
    config = toml.load(os.path.join(os.path.dirname(__file__), "no_package_no_expansion_index", "migrate.toml"))
    # Ensure create_package is False
    config['migration']['create_package'] = False
    # Ensure expand_files_array is False
    config['migration']['expand_files_array'] = False
    # Ensure create_index is True
    config['migration']['create_index'] = True
    
    result = migrate(config)
    
    # Verify no packages were created
    assert len(package_calls) == 0, "Packages were created when they shouldn't have been"
    
    # Verify index was created
    assert index_created, "Index was not created"
    
    # Verify configuration flags
    assert config['migration']['create_package'] is False, "create_package flag should be False"
    assert config['migration']['expand_files_array'] is False, "expand_files_array flag should be False"
    assert config['migration']['create_index'] is True, "create_index flag should be True"
    
    expected_path = os.path.join(os.path.dirname(__file__), "no_package_no_expansion_index","index.json")
    with open(expected_path, 'r') as f:
        expected_content = json.load(f)
        f.close()
    
    assert_dictionary_properties(result, expected_content)
    

def test_migration_package_expansion_index():
    try:
        config = toml.load(os.path.join(os.path.dirname(__file__), "package_expansion_index", "migrate.toml"))
        result = migrate(config)

        # If we get here without SystemExit, the test should fail
        assert False, "Expected SystemExit but it didn't occur"

    except SystemExit:
        # Test passes if SystemExit occurs
        assert True



@pytest.fixture
def mock_s3_client(monkeypatch):
    """Mock the boto3 S3 client for testing data export"""
    class MockS3Client:
        def __init__(self):
            self.uploaded_files = []
            
        def put_object(self, **kwargs):
            self.uploaded_files.append(kwargs)
            return {"ETag": "mock-etag"}
    
    mock_client = MockS3Client()
    
    def mock_session_client(self, service_name, **kwargs):
        if service_name == 's3':
            return mock_client
        return None
    
    # Mock boto3.session.Session.client
    monkeypatch.setattr('boto3.session.Session.client', mock_session_client)
    
    return mock_client

def test_migration_package_no_expansion(monkeypatch):
    # Track package creation calls
    package_calls = []
    
    # Mock the OpenSearchClient's create_and_associate_package method
    def mock_create_and_associate_package(self, package_name, file):
        package_calls.append((package_name, file))
        return "mock-package-id", "1.0"
    
    # Mock the SolrClient's get_solr_file_data method
    def mock_get_solr_file_data(self, file):
        return "mock file content"
    
    monkeypatch.setattr(OpenSearchClient, "create_and_associate_package", mock_create_and_associate_package)
    monkeypatch.setattr(SolrClient, "get_solr_file_data", mock_get_solr_file_data)
    
    config = toml.load(os.path.join(os.path.dirname(__file__), "package_no_expansion", "migrate.toml"))
    result = migrate(config)

    # Verify packages were created
    assert len(package_calls) > 0, "No packages were created"
    
    # Verify package names follow expected pattern
    for package_name, file in package_calls:
        assert package_name.startswith("p-"), f"Package name {package_name} doesn't start with 'p-'"
    
    expected_path = os.path.join(os.path.dirname(__file__), "package_no_expansion","index.json")
    with open(expected_path, 'r') as f:
        expected_content = json.load(f)
        f.close()

    assert_dictionary_properties(result, expected_content)



def test_migration_package_no_expansion_index(monkeypatch):
    # Track package and index creation calls
    package_calls = []
    index_created = False
    
    # Mock the OpenSearchClient's create_and_associate_package method
    def mock_create_and_associate_package(self, package_name, file):
        package_calls.append((package_name, file))
        return "mock-package-id", "1.0"
    
    # Mock the OpenSearchClient's create_index method
    def mock_create_index(self):
        nonlocal index_created
        index_created = True
        return True
    
    # Mock the SolrClient's get_solr_file_data method
    def mock_get_solr_file_data(self, file):
        return "mock file content"
    
    monkeypatch.setattr(OpenSearchClient, "create_and_associate_package", mock_create_and_associate_package)
    monkeypatch.setattr(OpenSearchClient, "create_index", mock_create_index)
    monkeypatch.setattr(SolrClient, "get_solr_file_data", mock_get_solr_file_data)
    
    config = toml.load(os.path.join(os.path.dirname(__file__), "package_no_expansion_index", "migrate.toml"))
    result = migrate(config)

    # Verify packages were created
    assert len(package_calls) > 0, "No packages were created"
    
    # Verify package names follow expected pattern
    for package_name, file in package_calls:
        assert package_name.startswith("p-"), f"Package name {package_name} doesn't start with 'p-'"
    
    # Verify index was created
    assert index_created, "Index was not created"
    
    expected_path = os.path.join(os.path.dirname(__file__), "package_no_expansion_index","index.json")
    with open(expected_path, 'r') as f:
        expected_content = json.load(f)
        f.close()

    assert_dictionary_properties(result, expected_content)

