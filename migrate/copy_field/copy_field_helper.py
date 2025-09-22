from config import get_custom_logger
from collections import namedtuple

logger = get_custom_logger("migrate.copy_field")

# Define CopyFieldResult namedtuple
CopyFieldResult = namedtuple('CopyFieldResult', ['src', 'src_def', 'dst', 'dst_def'])

class CopyFieldException(Exception):
    def __init__(self, name, reason=None, src_field=None):
        self.name = name
        self.reason = reason
        self.src_field = src_field


class CopyFieldHelper(object):
    def __init__(self, all_fields):
        self._all_fields = all_fields
        self._field_copy_to = {}  # Just to track source -> destinations for array handling

    def map_copy_field(self, solr_copy_field):
        src = solr_copy_field["source"]
        dst = solr_copy_field["dest"]

        src_def = self._all_fields.get(src)
        if src_def is None:
            raise CopyFieldException(name=src, reason="MappingNotFound", src_field=src)
        try:
            dst_def = src_def.copy()
            
            # Handle multiple destinations for same source
            if src not in self._field_copy_to:
                self._field_copy_to[src] = [dst]
                src_def["copy_to"] = dst
            else:
                self._field_copy_to[src].append(dst)
                if "copy_to" in src_def:
                    if isinstance(src_def["copy_to"], list):
                        src_def["copy_to"].append(dst)
                    else:
                        src_def["copy_to"] = [src_def["copy_to"], dst]
                else:
                    src_def["copy_to"] = [dst]

            return CopyFieldResult(src, src_def, dst, dst_def)
        except CopyFieldException as e:
            raise e
        except Exception as e:
            raise CopyFieldException(name=src, reason=e, src_field=src)
