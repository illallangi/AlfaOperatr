from functools import reduce
from hashlib import sha256

import six

import yaml


# https://stackoverflow.com/a/7205107
def merge(original, patch):
    "merges patch into a"
    result = {}
    for key in {*original.keys(), *patch.keys()}:
        if isinstance(original.get(key, None), dict) and isinstance(patch.get(key, None), dict):
            result[key] = merge(original[key], patch[key])
        else:
            if key in original:
                result[key] = original[key]
            if key in patch:
                result[key] = patch[key]

    return result


# https://stackoverflow.com/a/38506628
def common(a, b):
    "Returns a dictionary containing only the common elements in a and b"
    result = {}

    for k in a.keys() & b.keys():
        v1 = a[k]
        v2 = b[k]
        if isinstance(v1, dict) and isinstance(v2, dict):
            result[k] = common(v1, v2)
        elif v1 == v2:
            result[k] = v1

    return result


def unique_dict(input):
    return [yaml.load(y, Loader=yaml.FullLoader) for y in {yaml.dump(d) for d in input}]


# https://stackoverflow.com/a/28225747
def recursive_get(d, *keys, default=None, sep='.'):
    keys = [
        k
        for key in keys
        for k in key.split(sep)
    ]
    result = reduce(lambda c, k: c.get(k, {}) if isinstance(c, dict) else {}, keys, d)
    if result == {}:
        return default
    return result


# https://stackoverflow.com/posts/14023440/timeline#history_4c28e0a3-82ef-4080-9c59-11a95a097fee
# cc by-sa 3.0
def cheap_hash(string, length=6, default=None):
    if string is None or string == '':
        return default
    if length < len(sha256(string.encode('utf-8')).hexdigest()):
        return sha256(string.encode('utf-8')).hexdigest()[:length]
    else:
        raise Exception("Length too long. Length of {y} when hash length is {x}.".format(x=str(len(sha256(string.encode('utf-8')).hexdigest())), y=length))


# Get around pyyaml removing leading 0s
# https://github.com/yaml/pyyaml/issues/98
def string_representer(dumper, value):
    if value.startswith("0"):
        return dumper.represent_scalar("tag:yaml.org,2002:str", value, style="'")
    return dumper.represent_scalar("tag:yaml.org,2002:str", value)


yaml.Dumper.add_representer(six.text_type, string_representer)
