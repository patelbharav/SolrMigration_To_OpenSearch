from config import get_custom_logger
from migrate.utils import read_json_file_data
logger = get_custom_logger("migrate.field")


class FieldException(Exception):
    def __init__(self, name, reason=None, field_type=None):
        self.name = name
        self.reason = reason
        self.field_type = field_type


class FieldHelper(object):
    def __init__(self, solrclient, opensearchclient, field_type_helper):
        self._solrclient = solrclient
        self._opensearchclient = opensearchclient
        self._field_type_helper = field_type_helper
        self._attributes_mapping = read_json_file_data("./migrate/fields/attributes.json")

    def map_field(self, solr_field):
        name = solr_field["name"]
        field_field_type = solr_field["type"]

        field_type = self._field_type_helper.get_field_type(field_field_type)

        logger.info("Starting mapping for field %s", name)

        if field_type is None:
            logger.info("Mapping for field %s not Found", name)
            raise FieldException(name=name, reason="MappingNotFound", field_type=field_type)
        try:
            extra_attrs = set(solr_field.keys()).difference(
                {
                    "name",
                    "type",
                    "indexed",
                    "stored",
                    "docValues",
                    "multiValued",
                    "required",
                    "useDocValuesAsStored",
                    "omitNorms",
                    "termOffsets",
                    "termVectors",
                    "termPositions",
                }
            )
            if any(extra_attrs):
                logger.info("Unknown attrs: %s, %s", name, extra_attrs)

            attrs = {}
            for k, v in solr_field.items():
                if k in self._attributes_mapping:
                    attrs[self._attributes_mapping[k]] = v

            analyzer = (field_field_type in self._opensearchclient.get_all_analyzers() and field_field_type or None)

            index_analyzer = (
                    f"{field_field_type}_index" in self._opensearchclient.get_all_analyzers() and
                    f"{field_field_type}_index" or None
            )

            query_analyzer = (
                    f"{field_field_type}_query" in self._opensearchclient.get_all_analyzers() and f"{field_field_type}_query" or None
            )

            if analyzer:
                attrs["analyzer"] = analyzer
            if index_analyzer:
                attrs["analyzer"] = index_analyzer
            if query_analyzer:
                attrs["search_analyzer"] = query_analyzer

            attrs["type"] = field_type
            if attrs is None:
                logger.info("Mapping for field %s failed", name)
                raise FieldException(name=name, field_type=field_type, reason="Attributes")

            if field_type == "nested":
                if "index" in attrs:
                    del attrs["index"]
                if "store" in attrs:
                    del attrs["store"]
            
            # geo_shape fields don't support index and store parameters in OpenSearch
            if field_type == "geo_shape":
                if "index" in attrs:
                    del attrs["index"]
                if "store" in attrs:
                    del attrs["store"]
                if "doc_values" in attrs:
                    del attrs["doc_values"]
            
            # # nested fields only need type in OpenSearch
            # if field_type == "nested":
            #     attrs = {"type": "nested"}

            return name, attrs
        except FieldException as e:
            logger.info("Mapping for field %s failed FieldException", name)
            raise e
        except Exception as e:
            logger.info(e)
            raise FieldException(name=name, field_type=field_type, reason=e)