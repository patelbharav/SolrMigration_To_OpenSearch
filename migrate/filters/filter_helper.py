import json
import os

from opensearchpy import token_filter
from opensearchpy import char_filter

from config import get_custom_logger
from migrate.utils import read_json_file_data, write_json_file_data, get_hash
from opensearch.opensearch_client import OpenSearchClientException

logger = get_custom_logger("migrate.filters")

class FilterException(Exception):
    def __init__(self, name, reason=None):
        self.name = name
        self.reason = reason


class CharFilterException(Exception):
    def __init__(self, name, reason=None):
        self.name = name
        self.reason = reason


class FilterHelper(object):

    def __init__(self, solrclient, opensearchclient, migration_config):
        self._solrclient = solrclient
        self._opensearchclient = opensearchclient
        self._migration_config = migration_config
        self._filter_mapping = read_json_file_data("./migrate/filters/filter_mapping.json")
        self._filter_mapping = {k.lower(): v for k, v in self._filter_mapping.items()}

        logger.info("Loaded mapping for %s filters", len(self._filter_mapping.keys()))
        self._char_filter_mapping = read_json_file_data("./migrate/filters/char_filters_mapping.json")
        self._char_filter_mapping = {k.lower(): v for k, v in self._char_filter_mapping.items()}
        self._char_filter_mapping = {k.lower(): v for k, v in self._char_filter_mapping.items()}
        logger.info("Loaded mapping for %s char filters", len(self._char_filter_mapping.keys()))

        self._filter_map = {}
        self._char_filter_map = {}

        self._packages_map = {}

        self._solr_collection_dir = f"solr/{self._solrclient.get_collection()}"
        self._opensearch_collection_dir = f"migration_schema/{self._solrclient.get_collection()}"
        self._opensearch_package_dir = f"{self._opensearch_collection_dir}/packages"

        if not os.path.exists(self._opensearch_package_dir):
            os.makedirs(self._opensearch_package_dir)

        self.opensearch_packages_file = (
            f"{self._opensearch_collection_dir}/opensearch_packages.json"
        )

    def _get_filter_name(self, filter):
        name = filter.get("name")
        if name is None:
            name = filter.get("class")
            name = name.split(".")[1]
            if "TokenFilterFactory" in name:
                name = name.split("TokenFilterFactory")[0].lower()
                # name = name[0].lower + name[1:]
            elif "CharFilterFactory" in name:
                name = name.split("CharFilterFactory")[0].lower()
            elif "FilterFactory" in name:
                name = name.split("FilterFactory")[0].lower()
        else:
            name = name.lower()

        return name

    def _create_package_file(self, file_path_old, file_path_new, solr_file, filter_name):
        logger.info("%s,%s,%s,%s", file_path_old, file_path_new, solr_file, filter_name)
        file_data = self._get_file_data(filter_name, solr_file)

        with open(file_path_new, "w", encoding="utf-8") as f:
            f.write('\n'.join(file_data))

    def _get_file_data(self, filter_name, filename):
        data = self._solrclient.get_solr_file_data(filename)
        res = []

        for line in data.split("\n"):
            line = line.strip()

            if not line.startswith("#") and not line.startswith("|") and line.strip() and line.strip() != "|":
                if filter_name.lower().startswith("stemmeroverride"):
                    line = line.replace('\t', ' => ')
                res.append(line)
        return res

    def _map_filter(self, solr_filter):

        solr_filter_name = self._get_filter_name(solr_filter)

        try:
            hashed_solr_filter_name = solr_filter_name + get_hash(solr_filter)

            logger.info("mapping filter with name %s", solr_filter_name)

            filter_mapping = self._filter_mapping.get(solr_filter_name)
            if filter_mapping is None:
                logger.warning("Filter mapping not found for name %s", solr_filter_name)
                raise FilterException(name=solr_filter_name, reason="MappingNotFound")

            if self._filter_map.get(hashed_solr_filter_name) is not None:
                logger.info("returning from map")
                return self._filter_map.get(hashed_solr_filter_name)

            custom_filter_def = {}
            for key in filter_mapping.keys():

                if key == "type":
                    continue

                if "valueFrom" in filter_mapping[key]:
                    custom_filter_def[key] = solr_filter.get(filter_mapping[key]['valueFrom'])
                    if custom_filter_def[key] is None:
                        custom_filter_def[key] = filter_mapping[key]['default']
                elif "valueFromFile" in filter_mapping[key]:
                    filename = solr_filter.get(filter_mapping[key]['valueFromFile'])

                    if "create_package" in filter_mapping[key]:
                        if self._migration_config['create_package']:
                            try:
                                filter_attrib_value = self._handle_packages(f"filter_{solr_filter_name}", filename)
                                custom_filter_def[key] = filter_attrib_value
                                break
                            except OpenSearchClientException as e:
                                logger.warning("Could not retrieve or create package for filter %s", solr_filter_name)
                                raise e
                    else:
                        if self._migration_config['expand_files_array']:
                            logger.info("retrieve data from file %s for filter %s", filename, solr_filter_name)
                            custom_filter_def[key] = self._get_file_data(solr_filter_name, filename)
                        else:
                            custom_filter_def[key] = []

                else:
                    custom_filter_def[key] = filter_mapping[key]['default']

            tf = token_filter(hashed_solr_filter_name, filter_mapping['type'], **custom_filter_def)
            self._filter_map[hashed_solr_filter_name] = tf
            logger.info("mapping filter with name %s completed", solr_filter_name)
            return tf

        except FilterException as e:
            logger.warning("mapping filter with name %s failed", solr_filter_name)
            raise e
        except OpenSearchClientException as e:
            logger.warning("mapping filter with name %s failed", solr_filter_name)
            raise FilterException(name=solr_filter_name, reason=e.reason)

    def _map_char_filter(self, solr_char_filter):

        solr_filter_name = self._get_filter_name(solr_char_filter)

        try:
            logger.info("mapping CharFilter with name %s", solr_filter_name)

            char_filter_mapping = self._char_filter_mapping.get(solr_filter_name)
            if char_filter_mapping is None:
                logger.warning("CharFilter mapping not found for name %s", solr_filter_name)
                raise CharFilterException(name=solr_filter_name, reason="MappingNotFound")

            custom_filter_def = {}
            for key in char_filter_mapping.keys():
                if key == "type":
                    continue

                if "valueFrom" in char_filter_mapping[key]:
                    custom_filter_def[key] = solr_char_filter.get(char_filter_mapping[key]['valueFrom'])
                    if custom_filter_def[key] is None:
                        custom_filter_def[key] = char_filter_mapping[key]['default']
                elif "valueFromFile" in char_filter_mapping[key]:
                    filename = solr_char_filter.get(char_filter_mapping[key]['valueFromFile'])
                    if "create_package" in char_filter_mapping[key]:
                        if self._migration_config['create_package']:
                            try:
                                filter_attrib_value = self._handle_packages(f"char_filter{solr_filter_name}", filename)
                                custom_filter_def[key] = filter_attrib_value
                                break
                            except OpenSearchClientException as e:
                                logger.warning("Could not retrieve or create package for filter %s", solr_filter_name)
                                raise e
                    else:
                        if self._migration_config['expand_files_array']:
                            logger.info("retrieve data from file %s for CharFilter %s", filename, solr_filter_name)
                            custom_filter_def[key] = self._get_file_data(solr_filter_name, filename)
                        else:
                            custom_filter_def[key] = []
                else:
                    logger.info("Setting default value")
                    custom_filter_def[key] = char_filter_mapping[key]['default']

            solr_filter_name = solr_filter_name + get_hash(custom_filter_def)
            self._filter_map[solr_filter_name] = custom_filter_def

            cf = char_filter(solr_filter_name, char_filter_mapping['type'], **custom_filter_def)
            logger.info("mapping CharFilter with name %s completed", solr_filter_name)
            return cf
        except OpenSearchClientException as e:
            logger.warning("mapping Char filter with name %s failed", solr_filter_name)
            raise CharFilterException(name=solr_filter_name, reason=e.reason)
        except CharFilterException as e:
            logger.warning("mapping Char filter with name %s failed", solr_filter_name)
            raise CharFilterException(name=solr_filter_name, reason=e)

    def _handle_packages(self, solr_filter_name, filename):
        solr_file_path = f"{self._solr_collection_dir}/{filename}"
        p_name = filename.replace("/", "-").replace(".", "-").replace("_", "-")
        package_name = f"p-{self._solrclient.get_collection()}-{p_name}".lower()
        package_file = f"{self._opensearch_package_dir}/{package_name}"
        package_key = solr_filter_name + "_" + package_file
        if package_key in self._packages_map:
            logger.info("retrieve package details from existing map for filter %s", solr_filter_name)
            filter_attrib_value = self._packages_map[package_key]["filter_attrib_value"]
        else:
            logger.info("create package details for filter %s", solr_filter_name)
            self._create_package_file(solr_file_path, package_file, filename, solr_filter_name)
            package_id, package_version = self._opensearchclient.create_and_associate_package(package_name,
                                                                                              package_file)
            (package_name, package_attrib, filter_attrib_value) = \
                (package_name, solr_filter_name, f"analyzers/{package_id}")

            self._packages_map[package_key] = {
                "package_name": package_name,
                "package_attrib": package_attrib,
                "filter_attrib_value": filter_attrib_value,
            }
            logger.info("saving package details from existing map for Char filter %s", solr_filter_name)

        return filter_attrib_value

    def map_filters(self, solr_filters):
        my_filters = []
        try:
            # Ensure all filters have mapping defined. Else break. This avoids creating unnecessary filters which have
            # package requirements.
            for solr_filter in solr_filters:
                solr_filter_name = self._get_filter_name(solr_filter)

                filter_mapping = self._filter_mapping.get(solr_filter_name)
                if filter_mapping is None:
                    logger.warning("Pre Check Filter mapping not found for name %s", solr_filter_name)
                    raise FilterException(name=solr_filter_name, reason="MappingNotFound")

            # Start mapping filters have mapping defined. Else break.
            for solr_filter in solr_filters:
                f = self._map_filter(solr_filter)
                my_filters.append(f)

            return my_filters
        except FilterException as e:
            logger.warning("An error occurred during filter mapping: %s", str(e))
            raise

    def map_char_filters(self, solr_char_filters):
        my_char_filters = []
        try:
            for solr_char_filter in solr_char_filters:
                solr_char_filter_name = self._get_filter_name(solr_char_filter)

                filter_mapping = self._char_filter_mapping.get(solr_char_filter_name)
                if filter_mapping is None:
                    logger.warning("Pre Check CharFilter mapping not found for name %s", solr_char_filter_name)
                    raise CharFilterException(name=solr_char_filter_name, reason="MappingNotFound")

            for solr_char_filter in solr_char_filters:
                f = self._map_char_filter(solr_char_filter)
                my_char_filters.append(f)

            return my_char_filters
        except CharFilterException as e:
            logger.exception("An error occurred during char filter mapping: %s", str(e))
            raise
