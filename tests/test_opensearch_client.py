import pytest
from unittest.mock import patch, Mock
from opensearch.opensearch_client import OpenSearchClient
from opensearchpy.connection.http_requests import RequestsHttpConnection
from botocore.exceptions import ClientError
from opensearchpy.exceptions import  OpenSearchException  
from opensearchpy import  RequestError


class TestOpensearchClient:
    @pytest.fixture
    def base_config(self):
        return {
            'host': 'test-host',
            'port': 9200,
            'use_ssl': True,
            'index': 'test-index',
            'assert_hostname': False,
            'verify_certs': False,
            'use_aws_auth_sigv4': False,
            'domain': 'test-domain',
            'bucket': 'XXXXXXXXXXX',
            'username': 'test_user',
            'password': 'test_password',
            'region': 'us-east-1'
        }

    @pytest.fixture
    def mock_setup(self):
        with patch('opensearch.opensearch_client.boto3.client') as mock_boto3, \
             patch('opensearch.opensearch_client.OpenSearch') as mock_opensearch, \
             patch('opensearch.opensearch_client.RequestsHttpConnection', autospec=True) as mock_requests_connection:
            
            # Setup mocks
            mock_opensearch_instance = Mock()
            mock_opensearch.return_value = mock_opensearch_instance
            # Setup indices mock
            mock_indices = Mock()
            mock_opensearch_instance.indices = mock_indices
            
            
            mock_boto3_instance = Mock()
            mock_boto3.return_value = mock_boto3_instance
            mock_client = Mock()
            mock_opensearch.return_value = mock_client
            mock_client.info.return_value = {"version": {"number": "2.0"}}
            

            
            yield {
                'boto3': mock_boto3,
                'opensearch': mock_opensearch,
                'opensearch_instance': mock_opensearch_instance,
                'boto3_instance': mock_boto3_instance,
                'connection': mock_requests_connection,
                'client': mock_client
            }

    @pytest.fixture
    def mock_opensearch_client(self):
        # Create a mock client
        with patch('opensearch.opensearch_client.boto3.client') as mock_boto3, \
             patch('opensearch.opensearch_client.OpenSearch') as mock_opensearch, \
             patch('opensearch.opensearch_client.RequestsHttpConnection', autospec=True) as mock_connection:
            
            config = {
                'host': 'test-host',
                'port': 9200,
                'use_ssl': True,
                'index': 'test-index',
                'assert_hostname': False,
                'verify_certs': False,
                'use_aws_auth_sigv4': False,
                'domain': 'test-domain',
                'bucket': 'XXXXXXXXXXX',
                'username': 'test_user',
                'password': 'test_password',
                'region': 'us-east-1'
            }
            
            client = OpenSearchClient(config)

            
            # Add the required mock methods to the client
            client._get_all_package_names = Mock(return_value=[])
            client._upload_to_s3 = Mock()
            client._create_package = Mock()
            client._get_opensearch_package_by_name = Mock(return_value={
                "PackageID": "test_package_id",
                "PackageVersion": "1.0"
            })
            
            yield client

    @pytest.fixture
    def client(self, mock_setup):
        config = {
                'host': 'test-host',
                'port': 9200,
                'use_ssl': True,
                'index': 'test-index',
                'assert_hostname': False,
                'verify_certs': False,
                'use_aws_auth_sigv4': False,
                'domain': 'test-domain',
                'bucket': 'XXXXXXXXXXX',
                'username': 'test_user',
                'password': 'test_password',
                'region': 'us-east-1'
        }
        client = OpenSearchClient(config)
        client._opensearch_index = Mock()
        client._mapping = Mock()
        client._dynamic_templates = []
        return client
   

    @pytest.fixture
    def mock_setup_index(self):
        with patch('opensearch.opensearch_client.OpenSearch') as mock_opensearch:
            # Setup main mock instance
            mock_client = Mock()
            mock_opensearch.return_value = mock_client
            
            # Setup indices mock
            mock_indices = Mock()
            mock_client.indices = mock_indices
            
            # Setup info mock for connection test
            mock_client.info.return_value = {"version": {"number": "2.0"}}
            
            yield {
                'opensearch': mock_opensearch,
                'client': mock_client
            }

    @pytest.fixture
    def mock_setup(self):
        with patch('opensearch.opensearch_client.boto3.client') as mock_boto3, \
             patch('opensearch.opensearch_client.OpenSearch') as mock_opensearch, \
             patch('opensearch.opensearch_client.RequestsHttpConnection', autospec=True) as mock_requests_connection:
            
            mock_opensearch_instance = Mock()
            mock_indices = Mock()
            mock_opensearch_instance.indices = mock_indices
            mock_opensearch.return_value = mock_opensearch_instance
            
            mock_boto3_instance = Mock()
            mock_boto3.return_value = mock_boto3_instance
            
            yield {
                'boto3': mock_boto3,
                'opensearch': mock_opensearch,
                'opensearch_instance': mock_opensearch_instance,
                'boto3_instance': mock_boto3_instance,
                'connection': mock_requests_connection
            }

    @pytest.fixture
    def mock_client(self, base_config, mock_setup):
        client = OpenSearchClient(base_config)
        client._opensearch_client = mock_setup['opensearch_instance']
        client._s3_client_boto3 = Mock()
        client._opensearch_index = Mock()
        client._mapping = Mock()
        client._dynamic_templates = []
        return client

    @pytest.fixture
    def mock_package_response(self):
        return {
            "PackageID": "test-package-id",
            "PackageVersion": "1.0",
            "AvailablePackageVersion": "1.1"
        }

    def test_create_and_associate_package_new_package(self, mock_opensearch_client, mock_package_response):
        """Test creating and associating a new package"""
        # Arrange
        package_name = "test-package"
        file_path = "test/file.txt"
        
        # Setup mocks
        mock_opensearch_client._get_all_package_names = Mock(return_value=[])
        mock_opensearch_client._get_opensearch_package_by_name = Mock(return_value=mock_package_response)
        mock_opensearch_client._upload_to_s3 = Mock()
        mock_opensearch_client._create_package = Mock() 
        
        # Act
        package_id, version = mock_opensearch_client.create_and_associate_package(package_name, file_path)
        
        # Assert
        assert package_id == "test-package-id"
        assert version == "1.1"
        mock_opensearch_client._upload_to_s3.assert_called_once() 
        mock_opensearch_client._create_package.assert_called_once()


    def test_create_and_associate_package_existing_unchanged(self, mock_opensearch_client, mock_package_response):
        """Test handling an existing package with unchanged content"""
        # Arrange
        package_name = "test-package"
        file_path = "test/file.txt"
        
        # Setup mocks
        mock_opensearch_client.get_all_package_names = Mock(return_value=[package_name])
        mock_opensearch_client.get_opensearch_package_by_name = Mock(return_value=mock_package_response)
        mock_opensearch_client._calculate_etag = Mock(return_value=("md5-etag","sha256-etag"))

        mock_opensearch_client._s3_client_boto3.head_object.return_value = {"ETag": "same-etag"}
        mock_opensearch_client.upload_to_s3 = Mock()
        mock_opensearch_client.update_package = Mock()

        # Act
        package_id, version = mock_opensearch_client.create_and_associate_package(package_name, file_path)

        # Assert
        assert package_id == "test_package_id"
        assert version == "1.0"
        mock_opensearch_client.upload_to_s3.assert_not_called()
        mock_opensearch_client.update_package.assert_not_called()



    def test_create_and_associate_package_existing_changed(self, mock_opensearch_client, mock_package_response):
        """Test handling an existing package with changed content"""
        # Arrange
        package_name = "test-package"
        file_path = "test/file.txt"
        
        # Setup mocks with consistent method names (using underscores for private methods)
        mock_opensearch_client._get_all_package_names = Mock(return_value=[package_name])
        mock_opensearch_client._get_opensearch_package_by_name = Mock(return_value=mock_package_response)
        mock_opensearch_client._calculate_etag = Mock(return_value=("md5-etag","sha256-etag"))

        mock_opensearch_client._s3_client_boto3.head_object.return_value = {"ETag": "old-etag"}
        
        mock_opensearch_client._upload_to_s3 = Mock()
        mock_opensearch_client._update_package = Mock()
        mock_opensearch_client._associate_package = Mock()
        mock_opensearch_client._wait_for_association = Mock()

        # Act
        package_id, version = mock_opensearch_client.create_and_associate_package(package_name, file_path)

        # Assert
        assert package_id == "test-package-id"
        assert version == "1.1"
        mock_opensearch_client._upload_to_s3.assert_called_once()
        mock_opensearch_client._update_package.assert_called_once()

    def test_create_and_associate_package_existing_unchanged_s3_404(self, mock_opensearch_client, mock_package_response):

        """Test create_and_associate_package when file doesn't exist in S3 (404 error)"""
        # Arrange
        package_name = "test_package"
        file_key = "test_file.zip"
        
        # Mock package exists in package list
        mock_opensearch_client._get_all_package_names.return_value = [package_name]
        mock_opensearch_client._update_package = Mock()
        mock_opensearch_client._associate_package = Mock()
        mock_opensearch_client._wait_for_association = Mock()

        
        # Create a real ClientError instance
        mock_opensearch_client._s3_client_boto3.head_object.side_effect = ClientError(
            {
                'Error': {
                    'Code': '404',
                    'Message': 'Not Found'
                }
            },
            'HeadObject'
        )
        
        # Mock package details
        package_details = {
            "PackageID": "test_package_id",
            "PackageVersion": "1.0"
        }
        mock_opensearch_client._get_opensearch_package_by_name.return_value = package_details
        
        # Mock logger to prevent actual logging
        with patch('opensearch.opensearch_client.logger', autospec=True) as mock_logger:
            # Act
            package_id, version = mock_opensearch_client.create_and_associate_package(
                package_name, 
                file_key
            )
        
        # Assert
        mock_opensearch_client._s3_client_boto3.head_object.assert_called_once_with(
            Bucket=mock_opensearch_client._bucket,
            Key=file_key
        )
        mock_opensearch_client._upload_to_s3.assert_called_once_with(
            file_key,
            mock_opensearch_client._bucket,
            file_key
        )
        mock_opensearch_client._get_opensearch_package_by_name.assert_called_with(package_name)
        mock_opensearch_client._update_package.assert_called_once_with(
            package_details["PackageID"],
            mock_opensearch_client._bucket,
            file_key
        )
        mock_opensearch_client._associate_package.assert_called_once_with(
            package_details["PackageID"]
        )
        mock_opensearch_client._wait_for_association.assert_called_once_with(
            package_details["PackageID"]
        )
        
        assert package_id == package_details["PackageID"]
        assert version == package_details["PackageVersion"]


    def test_create_and_associate_package_s3_other_error(self, mock_opensearch_client):
            """Test handling of non-404 S3 errors"""
            # Arrange
            package_name = "test_package"
            file_key = "test_file.zip"
            
            # Mock package exists in package list
            mock_opensearch_client._get_all_package_names.return_value = [package_name]
            
            # Create a proper ClientError exception for a different error
            error_response = {
                'Error': {
                    'Code': '403',
                    'Message': 'Forbidden'
                }
            }
            mock_opensearch_client._s3_client_boto3.head_object.side_effect = \
                ClientError(operation_name='HeadObject', error_response=error_response)
            
            # Act & Assert
            with pytest.raises(ClientError) as exc_info:
                mock_opensearch_client.create_and_associate_package(package_name, file_key)
            
            assert exc_info.value.response['Error']['Code']

    def test_create_index_already_exists(self, mock_opensearch_client):
        """Test index creation when index already exists"""
        # Arrange
        mock_indices = Mock()
        # Mock the create method to raise RequestError for existing index
        error_message = {"error": {"root_cause": [{"type": "resource_already_exists_exception"}]}}
        mock_indices.create = Mock(side_effect=RequestError(400, "resource_already_exists_exception", error_message))
        mock_opensearch_client._opensearch_client.indices = mock_indices
        
        # Mock the index data
        mock_opensearch_client._opensearch_index.to_dict = Mock(return_value={})
        mock_opensearch_client._index = "test-index"

        # Act
        mock_opensearch_client.create_index()

        # Assert
        mock_indices.create.assert_called_once_with("test-index", body={})

    def test_create_index_request_error(self, mock_opensearch_client):
        """Test index creation when index already exists"""
        # Arrange
        error_message = "index already exists"
        mock_indices = Mock()
        # Mock the create method to raise RequestError
        mock_indices.create = Mock(side_effect=RequestError(400, error_message))
        mock_opensearch_client._opensearch_client.indices = mock_indices
        
        # Mock the index data
        mock_opensearch_client._opensearch_index.to_dict = Mock(return_value={})
        mock_opensearch_client._index = "test-index"

        # Act
        with patch('opensearch.opensearch_client.logger') as mock_logger:
            mock_opensearch_client.create_index()

        # Assert
        mock_logger.error.assert_called_once_with(error_message)

    @pytest.mark.parametrize("package_version,expected", [
        ({"PackageVersion": "1.0", "AvailablePackageVersion": "1.1"}, "1.1"),
        ({"PackageVersion": "1.0", "AvailablePackageVersion": None}, "1.0"),
        ({"PackageVersion": "1.0"}, "1.0"),
    ])
    
    def test_package_version_handling(self, mock_opensearch_client, package_version, expected):
        """Test different package version scenarios"""
        # Arrange
        package_name = "test-package"
        file_path = "test/file.txt"
        
        # Setup mocks with underscore prefix for private methods
        mock_opensearch_client._get_all_package_names = Mock(return_value=[package_name])
        mock_opensearch_client._get_opensearch_package_by_name = Mock(return_value={
            "PackageID": "test-package-id",
            **package_version
        })
        
        # Setup additional required mocks
        mock_opensearch_client._upload_to_s3 = Mock()
        mock_opensearch_client._create_package = Mock()
        mock_opensearch_client._update_package = Mock()
        mock_opensearch_client._associate_package = Mock()
        mock_opensearch_client._wait_for_association = Mock()
        
        # Setup S3 client mock
        mock_s3 = Mock()
        mock_s3.head_object = Mock(return_value={"ETag": "test-etag"})
        mock_opensearch_client._s3_client_boto3 = mock_s3
        
        # Setup calculate_etag mock
        mock_opensearch_client._calculate_etag = Mock(return_value=("md5-etag","sha256-etag"))


        # Act
        _, version = mock_opensearch_client.create_and_associate_package(package_name, file_path)

        # Debug print
        print(f"\nPackage version: {package_version}")
        print(f"Expected version: {expected}")
        print(f"Actual version: {version}")

        # Assert
        assert version == expected



    def test_create_and_associate_package_1(self, mock_opensearch_client):
        """
        Test create_and_associate_package when package_name is not in package_name_list.
        """
        # Arrange 
        package_name = "new_package"
        file = "test_file.txt"

        # Act
        result = mock_opensearch_client.create_and_associate_package(package_name, file)

        # Assert
        assert result == ("test_package_id", "1.0")
        mock_opensearch_client._get_all_package_names.assert_called_once()
        mock_opensearch_client._upload_to_s3.assert_called_once_with(file, mock_opensearch_client._bucket, file)
        mock_opensearch_client._create_package.assert_called_once_with(package_name, mock_opensearch_client._bucket, file)
        mock_opensearch_client._get_opensearch_package_by_name.assert_called_once_with(package_name)

    def test_create_index_successful(self, mock_setup_index):
        """Test successful index creation"""
        # Arrange
        config = {
             'host': 'test-host',
             'port': 9200,
             'use_ssl': True,
             'index': 'test-index',
             'assert_hostname': False,
             'verify_certs': False,
             'use_aws_auth_sigv4': False,
             'domain': 'test-domain',
             'bucket': 'XXXXXXXXXXX',
             'username': 'test_user',
             'password': 'test_password',
             'region': 'us-east-1'
        }
        
        expected_response = {"acknowledged": True}
        mock_index_data = {
            "settings": {
                "index": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                }
            }
        }
        
        client = OpenSearchClient(config)
        client._opensearch_index = Mock()
        client._opensearch_index.to_dict.return_value = mock_index_data
        client._index = "test-index"
        
        # Configure mock response
        mock_setup_index['client'].indices.create.return_value = expected_response
        
        # Act
        result = client.create_index()
        
        # Assert
        #assert result == expected_response
        mock_setup_index['client'].indices.create.assert_called_once_with(
            "test-index",
            body=mock_index_data
        )
        client._opensearch_index.to_dict.assert_called_once()

        
    def test_client_initialization(self, mock_setup):
        config = {
            'host': 'test-host',
            'port': 9200,
            'use_ssl': True,
            'index': 'test-index',
            'assert_hostname': False,
            'verify_certs': False,
            'use_aws_auth_sigv4': False,
            'domain': 'test-domain',
            'bucket': 'XXXXXXXXXXX',
            'username': 'test_user',
            'password': 'test_password',
            'region': 'us-east-1'
        }
        
        # Configure mock responses
        mock_setup['opensearch_instance'].info.return_value = {"version": {"number": "2.0"}}
        mock_setup['opensearch_instance'].cat.indices.return_value = "mock indices"
        
        # Create client
        client = OpenSearchClient(config)
        
        # Verify the OpenSearch client was initialized with correct parameters
        mock_setup['opensearch'].assert_called_once_with(
            hosts=[{'host': 'test-host', 'port': 9200}],
            http_auth=('test_user', 'test_password'),
            use_ssl=True,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
            connection_class=mock_setup['connection']

            )


    def test_create_index_request_error(self, base_config):
            """Test index creation when it raises a RequestError"""
            # Arrange
            with patch('opensearch.opensearch_client.OpenSearch') as mock_opensearch, \
                patch('opensearch.opensearch_client.logger') as mock_logger:
                
                # Setup mock OpenSearch client
                mock_opensearch_instance = Mock()
                mock_opensearch.return_value = mock_opensearch_instance
                
                # Setup mock indices
                mock_indices = Mock()
                mock_opensearch_instance.indices = mock_indices
                
                # Configure the create method to raise RequestError
                error_message = "Invalid index creation request"
                error_response = {
                    "error": {
                        "root_cause": [
                            {"type": "invalid_index_name_exception"}
                        ]
                    }
                }
                mock_indices.create.side_effect = RequestError(
                    400,
                    error_message,
                    error_response
                )

                # Create client instance
                client = OpenSearchClient(base_config)
                
                # Mock the index data
                test_index_data = {"settings": {}}
                client._opensearch_index = Mock()
                client._opensearch_index.to_dict.return_value = test_index_data

                # Act
                client.create_index()

                # Assert
                mock_indices.create.assert_called_once_with(
                    'test-index',
                    body=test_index_data
                )
                mock_logger.error.assert_called_once_with(error_message)

        
    def test_add_analyzer(self, client):
        """Test adding an analyzer"""
        # Arrange
        analyzer = {
            "custom_analyzer": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": ["lowercase"]
            }
        }

        # Act
        client.add_analyzer(analyzer)

        # Assert
        client._opensearch_index.analyzer.assert_called_once_with(analyzer)

    def test_add_field(self, client):
        """Test adding a field"""
        # Arrange
        field_name = "test_field"
        field_config = {"type": "text"}

        # Act
        client.add_field(field_name, field_config)

        # Assert
        client._mapping.field.assert_called_once_with(field_name, field_config)
        client._opensearch_index.mapping.assert_called_once_with(client._mapping)

    def test_add_copy_field(self, client):
        """Test adding a copy field"""
        # Arrange
        field_name = "copy_field"
        field_config = {
            "type": "text",
            "copy_to": ["target_field"]
        }

        # Act
        client.add_copy_field(field_name, field_config)

        # Assert
        client._mapping.field.assert_called_once_with(field_name, field_config)
        client._opensearch_index.mapping.assert_called_once_with(client._mapping)

    def test_add_dynamic_field(self, client):
        """Test adding a dynamic field"""
        # Arrange
        dynamic_field = {
            "match": "*_text",
            "mapping": {"type": "text"}
        }

        # Act
        client.add_dynamic_field(dynamic_field)

        # Assert
        assert dynamic_field in client._dynamic_templates
        client._mapping.meta.assert_called_once_with("dynamic_templates", client._dynamic_templates)
        client._opensearch_index.mapping.assert_called_once_with(client._mapping)

    def test_get_all_analyzers(self, client):
        """Test getting all analyzers"""
        # Arrange
        expected_analyzers = {
            "custom_analyzer": {
                "type": "custom",
                "tokenizer": "standard"
            }
        }
        client._opensearch_index.to_dict.return_value = {
            "settings": {
                "analysis": {
                    "analyzer": expected_analyzers
                }
            }
        }

        # Act
        result = client.get_all_analyzers()

        # Assert
        assert result == expected_analyzers
        client._opensearch_index.to_dict.assert_called_once()

    def test_get_all_fields(self, client):
        """Test getting all fields"""
        # Arrange
        expected_fields = {
            "field1": {"type": "text"},
            "field2": {"type": "keyword"}
        }
        client._opensearch_index.to_dict.return_value = {
            "mappings": {
                "properties": expected_fields
            }
        }

        # Act
        result = client.get_all_fields()

        # Assert
        assert result == expected_fields
        client._opensearch_index.to_dict.assert_called_once()

    def test_get_all_tokenizers(self, client):
        """Test getting all tokenizers"""
        # Arrange
        expected_tokenizers = {
            "custom_tokenizer": {
                "type": "pattern",
                "pattern": "\\W+"
            }
        }
        client._opensearch_index.to_dict.return_value = {
            "settings": {
                "analysis": {
                    "tokenizer": expected_tokenizers
                }
            }
        }

        # Act
        result = client.get_all_tokenizers()

        # Assert
        assert result == expected_tokenizers
        client._opensearch_index.to_dict.assert_called_once()

    def test_get_all_filters(self, client):
        """Test getting all filters"""
        # Arrange
        expected_filters = {
            "custom_filter": {
                "type": "stop",
                "stopwords": ["a", "the", "is"]
            }
        }
        client._opensearch_index.to_dict.return_value = {
            "settings": {
                "analysis": {
                    "filter": expected_filters
                }
            }
        }

        # Act
        result = client.get_all_filters()

        # Assert
        assert result == expected_filters
        client._opensearch_index.to_dict.assert_called_once()

    def test_get_index_json(self, client):
        """Test getting complete index JSON"""
        # Arrange
        expected_index_json = {
            "settings": {
                "index": {
                    "number_of_shards": 1
                }
            },
            "mappings": {
                "properties": {
                    "field1": {"type": "text"}
                }
            }
        }
        client._opensearch_index.to_dict.return_value = expected_index_json

        # Act
        result = client.get_index_json()

        # Assert
        assert result == expected_index_json
        client._opensearch_index.to_dict.assert_called_once()

    def test_get_empty_analyzers(self, client):
        """Test getting analyzers when none exist"""
        # Arrange
        client._opensearch_index.to_dict.return_value = {}

        # Act
        result = client.get_all_analyzers()

        # Assert
        assert result == {}
        client._opensearch_index.to_dict.assert_called_once()

    def test_get_empty_fields(self, client):
        """Test getting fields when none exist"""
        # Arrange
        client._opensearch_index.to_dict.return_value = {}

        # Act
        result = client.get_all_fields()

        # Assert
        assert result == {}
        client._opensearch_index.to_dict.assert_called_once()

    def test_multiple_dynamic_fields(self, client):
        """Test adding multiple dynamic fields"""
        # Arrange
        dynamic_fields = [
            {
                "match": "*_text",
                "mapping": {"type": "text"}
            },
            {
                "match": "*_keyword",
                "mapping": {"type": "keyword"}
            }
        ]

        # Act
        for field in dynamic_fields:
            client.add_dynamic_field(field)

        # Assert
        assert len(client._dynamic_templates) == 2
        assert all(field in client._dynamic_templates for field in dynamic_fields)
        assert client._mapping.meta.call_count == 2
        assert client._opensearch_index.mapping.call_count == 2

    def test_add_field_with_nested_config(self, client):
        """Test adding a field with nested configuration"""
        # Arrange
        field_name = "nested_field"
        field_config = {
            "type": "nested",
            "properties": {
                "subfield1": {"type": "text"},
                "subfield2": {"type": "keyword"}
            }
        }

        # Act
        client.add_field(field_name, field_config)

        # Assert
        client._mapping.field.assert_called_once_with(field_name, field_config)
        client._opensearch_index.mapping.assert_called_once_with(client._mapping)


