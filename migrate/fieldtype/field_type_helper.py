from config import get_custom_logger
from migrate.analyzer.analyzer_helper import AnalyzerHelper, AnalyzerException
from migrate.utils import read_json_file_data

logger = get_custom_logger("migrate.field_type")


class FieldTypeException(Exception):
    def __init__(self, name, analyzer_exception=None):
        self.name = name
        self.analyzer_exception = analyzer_exception


class FieldTypeHelper(object):
    def __init__(self, solrclient, opensearch_client, migration_config):
        self._solrclient = solrclient
        self._opensearch_client = opensearch_client
        self._migration_config = migration_config
        self._field_data_types_mapping = read_json_file_data(
            "./migrate/fieldtype/field_data_types.json"
        )
        self._field_types_map = {}
        self._analyzer_helper = AnalyzerHelper(solrclient, opensearch_client, migration_config)

    def get_field_type(self, field_type):
        return self._field_types_map.get(field_type)

    def _map_field_data_type(self, solr_field_type):
        return solr_field_type["name"], self._field_data_types_mapping.get(solr_field_type["class"])

    def map_field_type_analyzer(self, solr_field_type):
        field_type_name = solr_field_type['name']
        try:
            (field_type_name, field_type_data_type) = self._map_field_data_type(solr_field_type)
            field_type_element_analyzer = self._analyzer_helper.map_analyzer(solr_field_type)
            self._field_types_map[field_type_name] = field_type_data_type
            return field_type_element_analyzer
        except AnalyzerException as e:
            raise FieldTypeException(name=field_type_name, analyzer_exception=e)
