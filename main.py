# Press the green button in the gutter to run the script.
import sys

import opensearchpy
import pysolr
import toml
import boto3

from config import get_custom_logger
from migrate.solr2os_migrate import Solr2OSMigrate
from opensearch.opensearch_client import OpenSearchClient
from solr.solr_client import SolrClient

logger = get_custom_logger("main")

if __name__ == '__main__':    
    config = toml.load("migrate.toml")
    migration_config = config['migration']
    data_migration_config = config.get('data_migration', {})
    if config.get('migrate_schema', False):
        if migration_config['create_package'] is True and migration_config['expand_files_array'] is True:
            logger.error("create_package and expand_files_array are mutually exclusive")
            sys.exit()
        
    # Validate data migration configuration if enabled
    if data_migration_config.get('migrate_data', False):
        if not data_migration_config.get('s3_export_bucket'):
            logger.error("s3_export_bucket must be specified when migrate_data is enabled")
            sys.exit()
        
        # Verify AWS credentials are available
        try:
            region = data_migration_config.get('region', 'us-east-1')
            boto3.client('sts', region_name=region).get_caller_identity()
        except Exception as e:
            logger.error(f"AWS credentials not properly configured: {str(e)}")
            logger.error("Please configure AWS credentials for S3 access")
            sys.exit()
            
    try:
        solrclient = SolrClient(config['solr'])
        opensearchclient = OpenSearchClient(config['opensearch'])
        file_path = f"migration_schema/{config['solr']['collection']}/"
        
        # Initialize the migration object
        migrator = Solr2OSMigrate(
            solrclient, 
            opensearchclient, 
            config['migration'],
            data_migration_config
        )
        logger.info("Migration object initialized")
        # Handle schema migration if enabled
        if migration_config.get('migrate_schema', False):
            logger.info("Starting schema migration")
            migrator.migrate_schema(file_path)
            logger.info("Schema migration completed")
        else:
            logger.info("Schema migration is disabled")
        
        # Handle data migration if enabled
        if data_migration_config.get('migrate_data', False):
            logger.info("Starting data export")
            migrator.export_data()
            logger.info(f"Data export completed. Check S3 bucket: {data_migration_config['s3_export_bucket']}")
            
    except pysolr.SolrError as e:
        logger.error(f"Solr error: {str(e)}")
        sys.exit()
    except opensearchpy.exceptions.OpenSearchException as e:
        logger.error(f"OpenSearch error: {str(e)}")
        sys.exit()
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit()
