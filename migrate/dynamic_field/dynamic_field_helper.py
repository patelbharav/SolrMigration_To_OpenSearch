from config import get_custom_logger
from migrate.utils import read_json_file_data

logger = get_custom_logger("migrate.dynamic_field")


class DynamicFieldException(Exception):
    def __init__(self, name, reason=None, field_type=None):
        self.name = name
        self.reason = reason
        self.field_type = field_type


class DynamicFieldHelper(object):
    def __init__(self, solrclient, opensearchclient, field_type_helper):
        self._solrclient = solrclient
        self._opensearchclient = opensearchclient
        self._field_type_helper = field_type_helper

        self._attributes_mapping = read_json_file_data("./migrate/fields/attributes.json")
        self._basic_types_mapping = read_json_file_data("./mappings/basic_types.json")
        self._field_types_map = {}

    def map_dynamic_field(self, solr_dynamic_field):
        pattern = solr_dynamic_field["name"]
        field_type = solr_dynamic_field["type"]
        dynamic_field_type = self._field_type_helper.get_field_type(field_type)
        if dynamic_field_type is None:
            raise DynamicFieldException(name=pattern, reason="MappingNotFound", field_type=field_type)
        try:
            extra_attrs = set(solr_dynamic_field.keys()).difference(
                {
                    "name",
                    "type",
                    "indexed",
                    "stored",
                    "docValues",
                    "multiValued",
                    "useDocValuesAsStored",
                }
            )
            if any(extra_attrs):
                print("Unknown attrs:", solr_dynamic_field.tag, pattern, extra_attrs)

            attrs = {}
            for k, v in solr_dynamic_field.items():
                if k in self._attributes_mapping:
                    attrs[self._attributes_mapping[k]] = v

            index_analyzer = (
                # field_type not in self.self.basic_types_mapping
                # and
                    (
                            f"{field_type}_index" in self._opensearchclient.get_all_analyzers() or field_type in self._opensearchclient.get_all_analyzers())
                    and f"{field_type}" or None
            )
            query_analyzer = (
                # field_type not in self.self.basic_types_mapping
                # and
                    f"{field_type}s_query" in self._opensearchclient.get_all_analyzers()
                    and f"{field_type}s_query"
                    or None
            )

            mapping = {
                "type": dynamic_field_type,
                **attrs,
            }

            if index_analyzer:
                mapping["analyzer"] = index_analyzer
            if query_analyzer:
                mapping["search_analyzer"] = query_analyzer

            return {pattern: {"match": pattern, "mapping": mapping}}
        except DynamicFieldException as e:
            raise e
        except Exception as e:
            raise DynamicFieldException(name=pattern, reason=e, field_type=field_type)
