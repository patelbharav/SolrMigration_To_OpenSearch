import hashlib
import json
import os

def get_hash(d):
    return str(
        int(hashlib.sha256(json.dumps(d, sort_keys=True, ensure_ascii=True).encode("utf-8"), usedforsecurity=False).hexdigest(), 16) % (10 ** 8))


def read_json_file_data(file):
    if os.path.exists(file):
        with open(file, encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {}
    return data


def write_json_file_data(data, file_name):
    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, sort_keys=True)
