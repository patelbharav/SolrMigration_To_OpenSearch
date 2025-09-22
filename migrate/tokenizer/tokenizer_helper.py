from opensearchpy import tokenizer
from migrate.utils import read_json_file_data, get_hash
from config import get_custom_logger
import os

logger = get_custom_logger("migrate.tokenizer")


class TokenizerException(Exception):
    def __init__(self, name, reason=None):
        self.name = name
        self.reason = reason


class TokenizerHelper(object):

    def __init__(self, solrclient, opensearchclient):
        self._solrclient = solrclient
        self._opensearchclient = opensearchclient
        
        # Get the correct path to the mapping file
        current_dir = os.path.dirname(os.path.abspath(__file__))
        mapping_file = os.path.join(current_dir, "tokenizer_mapping.json")
        
        logger.info("Current directory: %s", current_dir)
        logger.info("Loading mapping file from: %s", mapping_file)
        
        self._tokenizer_mapping = read_json_file_data(mapping_file)
        logger.debug("Raw mapping content: %s", self._tokenizer_mapping)
        
        self._tokenizer_mapping = {k.lower(): v for k, v in self._tokenizer_mapping.items()}
        logger.debug("Processed mappings: %s", self._tokenizer_mapping)
        logger.debug("Available tokenizer types: %s", list(self._tokenizer_mapping.keys()))
        self._tokenizerMap = {}

    def _get_tokenizer_name(self, tokenizer):
        name = tokenizer.get("name")
        if name is None:
            name = tokenizer.get("class").split(".")[1]
            if "TokenizerFactory" in name:
                name = name.split("TokenizerFactory")[0].lower()
        else:
            name = name.lower()
        return name

    def map_tokenizer(self, solr_tokenizer):

        tokenizer_name = self._get_tokenizer_name(solr_tokenizer)

        try:
            logger.info("mapping toknizer with name %s", tokenizer_name)
            tokenizer_mapping = self._tokenizer_mapping.get(tokenizer_name)
            if tokenizer_mapping is None:
                logger.error("Toknizer mapping not found for name %s", tokenizer_name)
                raise TokenizerException(name=tokenizer_name, reason="MappingNotFound")
            tokenizer_def = {}
            # tokenizer_def['type'] = tokenizer_mapping['type']
            for key in tokenizer_mapping.keys():
                if key == "type":
                    continue
                if "valueFrom" in tokenizer_mapping[key].keys():
                    tokenizer_def[key] = solr_tokenizer.get(tokenizer_mapping[key]['valueFrom'])
                    if tokenizer_def[key] is None:
                        tokenizer_def[key] = tokenizer_mapping[key]['default']
                else:
                    tokenizer_def[key] = tokenizer_mapping[key]['default']

            tokenizer_name = tokenizer_name + get_hash(tokenizer_def)
            self._tokenizerMap[tokenizer_name] = tokenizer_name

            opensearch_tokenizer = tokenizer(
                tokenizer_name, tokenizer_mapping['type'], **tokenizer_def
            )
            logger.info("mapping Toknizer with name %s completed", tokenizer_name)
            return opensearch_tokenizer
        except TokenizerException as e:
            raise e
        except Exception as e:
            logger.info("Mapping Toknizer with name %s failed", tokenizer_name)
            raise TokenizerException(name=tokenizer_name, reason=e)