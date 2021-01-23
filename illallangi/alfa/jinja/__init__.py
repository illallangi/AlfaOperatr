import base64
import json
import os
from functools import reduce
from hashlib import sha256

from illallangi.alfa.functions import merge

import jinja2
from jinja2.ext import Extension

import jmespath
from jmespath import functions

from loguru import logger

from more_itertools import one

from netaddr import IPAddress

import yaml


class AlfaJinja:
    def __init__(self, name):
        self.name = name
        self.environment = jinja2.Environment(
            loader=jinja2.BaseLoader,
            trim_blocks=True,
            lstrip_blocks=True,
            extensions=[
                "jinja2_ansible_filters.AnsibleCoreFiltersExtension",
                "illallangi.alfa.jinja.AlfaJinjaFiltersExtension",
            ],
        )
        self.environment.tests["is_subset"] = is_subset
        self.environment.tests["is_superset"] = is_superset

    def render(self, template, **kwargs):
        try:
            jinja2_template = self.environment.from_string(source=template)
        except jinja2.TemplateSyntaxError as e:
            logger.error(f"Template Syntax Error Loading Template: {e}:{e.lineno})")
            return None
        except LabelsFilteredInputBelowMinimumException as e:
            logger.error(f"Filter Error Rendering Template: {e}:{e.lineno})")
            return None
        except Exception as e:
            logger.error(f"Unknown Exception Loading Template: {repr(e)}")
            return None

        try:
            jinja2_result = jinja2_template.render(**kwargs)
        except jinja2.TemplateSyntaxError as e:
            logger.error(f"Template Syntax Error Rendering Template: {e}:{e.lineno})")
            return None
        except LabelsFilteredInputBelowMinimumException as e:
            logger.error(f"Filter Error Rendering Template: {e}:{e.lineno})")
            return None
        except Exception as e:
            logger.error(f"Unknown Exception Rendering Template: {repr(e)}")
            return None

        return jinja2_result.strip()


class AlfaJinjaFiltersExtension(Extension):
    def __init__(self, environment):
        super().__init__(environment)
        environment.filters["b64decode"] = b64decode
        environment.filters["ipaddr"] = ipaddr
        environment.filters["json_query"] = json_query
        environment.filters["json_query_one"] = json_query_one
        environment.filters["json_query_unique"] = json_query_unique
        environment.filters["unique_dict"] = unique_dict
        environment.filters["cheap_hash"] = cheap_hash
        environment.filters["path_join"] = path_join
        environment.filters["merge"] = merge
        environment.filters["alfa_query"] = alfa_query
        environment.filters["one_by_labels"] = one_by_labels
        environment.filters["many_by_labels"] = many_by_labels


def by_labels(input, namespace, *labels, min=None):
    if not isinstance(namespace, str):
        raise TypeError("namespace must be specified and string")
    f = reduce(merge, labels)
    kind = input[0]["kind"]
    input = [
        i for i in input if i.get("metadata", {}).get("namespace", "") == namespace
    ]
    result = None
    for k in f:
        result = [
            i
            for i in result or input
            if i.get("metadata", {}).get("labels", {}).get(k, None) == f[k]
        ]
        if min and len(result) < min:
            raise LabelsFilteredInputBelowMinimumException(kind, input, k, f[k], min)
    return result


def many_by_labels(input, namespace, *labels):
    return by_labels(input, namespace, *labels)


def one_by_labels(input, namespace, *labels):
    return one(by_labels(input, namespace, *labels, min=1))


class LabelsFilteredInputBelowMinimumException(Exception):
    def __init__(self, kind, input, key, value, min):
        self.kind = kind
        self.input = input
        self.key = key
        self.value = value
        self.min = min
        super().__init__(
            f"{self.key}=={self.value} filtered below {self.min} {self.kind}(s)"
        )

    def __repr__(self):
        return f'{super().__repr__()}:\ninput:\n{json.dumps([{"namespace": x["metadata"]["namespace"], "name": x["metadata"]["name"], "labels": x["metadata"]["labels"]} for x in self.input], indent=True)}'


# https://stackoverflow.com/posts/18335110/timeline
# cc-by-sa 4.0
def is_subset(input, superset):
    try:
        for key, value in input.items():
            if type(value) is dict:
                result = is_subset(value, superset[key])
                assert result
            else:
                assert superset[key] == value
                result = True
    except (AssertionError, KeyError):
        result = False
    return result


def is_superset(input, subset):
    return is_subset(subset, input)


# https://gist.github.com/tobinquadros/1862543f719b72b57cf682918c99683c
def b64decode(input):
    return base64.b64decode(input).decode()


def ipaddr(value, action):
    if action == "revdns":
        return IPAddress(value).reverse_dns.strip(".")
    raise NotImplementedError


def json_query(input, f):
    result = jmespath.search(
        f, input, options=jmespath.Options(custom_functions=CustomFunctions())
    )
    return list(result)


def json_query_one(input, f):
    result = jmespath.search(
        f, input, options=jmespath.Options(custom_functions=CustomFunctions())
    )
    if (0 if result is None else len(result)) != 1:
        raise Exception(
            f"Incorrect number of items in iterable (expected 1, received {0 if result is None else len(result)} from {f} in {json.dumps(input)})"
        )
    return one(result)


def json_query_unique(input, f):
    result = jmespath.search(
        f, input, options=jmespath.Options(custom_functions=CustomFunctions())
    )
    return [
        yaml.load(y, Loader=yaml.FullLoader) for y in {yaml.dump(d) for d in result}
    ]


def unique_dict(input):
    return [yaml.load(y, Loader=yaml.FullLoader) for y in {yaml.dump(d) for d in input}]


# https://stackoverflow.com/posts/14023440/timeline#history_4c28e0a3-82ef-4080-9c59-11a95a097fee
# cc by-sa 3.0
def cheap_hash(string, length=6):
    if length < len(sha256(string.encode("utf-8")).hexdigest()):
        return sha256(string.encode("utf-8")).hexdigest()[:length]
    else:
        raise Exception(
            "Length too long. Length of {y} when hash length is {x}.".format(
                x=str(len(sha256(string.encode("utf-8")).hexdigest())), y=length
            )
        )


def path_join(input):
    return os.path.join(input[0], *input[1:]).strip("/")


def alfa_query(
    input, parent_kind, child_kind, child_group, child_version, spec_filter=None
):
    query = f"[?kind=='{parent_kind}']."
    if spec_filter is not None:
        query = f"[?kind=='{parent_kind}' && spec.{spec_filter} && [spec.{spec_filter}.count,`1`][?@]|[0] > `0`].[loop(@, spec.{spec_filter}.count)][][]."

    query = (
        query
        + f"""
            {{
                "apiVersion": '{child_group}/{child_version}',
                "kind": '{child_kind}',
                "metadata":
                {{
                    "labels":
                    {{
                        "app.kubernetes.io/name": join(
                            '-',
                            [
                                [
                                    item.labels.["app.kubernetes.io/name"][0],
                                    template.labels.["app.kubernetes.io/name"][0]
                                ][?@]|[0]
                            ][?@]
                        ),
                        "app.kubernetes.io/instance": join(
                            '-',
                            [
                                [
                                    item.labels.["app.kubernetes.io/instance"][0],
                                    item.name
                                ][?@]|[0],
                                [
                                    __index
                                ][?@]|[0]
                            ][?@]
                        ),
                        "app.kubernetes.io/component":    join(
                            '-',
                            [
                                [
                                    item.labels.["app.kubernetes.io/component"][0],
                                    template.labels.["app.kubernetes.io/component"][0]
                                ][?@]|[0]
                            ][?@]
                        )
                    }},
                    "name": join(
                        '-',
                        [
                            [
                                item.labels.["app.kubernetes.io/name"][0],
                                template.labels.["app.kubernetes.io/name"][0]
                            ][?@]|[0],
                            [
                                item.labels.["app.kubernetes.io/instance"][0],
                                template.labels.["app.kubernetes.io/instance"][0],
                                item.name
                            ][?@]|[0],
                            [
                                __index
                            ][?@]|[0],
                            [
                                item.labels.["app.kubernetes.io/component"][0],
                                template.labels.["app.kubernetes.io/component"][0]
                            ][?@]|[0]
                        ]|[?@]
                    ),
                    "namespace": item.namespace,
                    "ownerReferences": [
                        {{
                            "apiVersion": apiVersion,
                            "blockOwnerDeletion": `true`,
                            "controller": `false`,
                            "kind": kind,
                            "name": item.name,
                            "uid": item.uid
                        }}
                    ]
                }},
                "spec": spec,
                "__index": __index,
                "__number": __number
            }}"""
    )
    result = json_query(input, query)
    return result


class CustomFunctions(functions.Functions):
    @functions.signature({"types": ["object"]}, {"types": ["null", "number"]})
    def _func_loop(self, p, c):
        return [
            {
                **p,
                **{
                    "__number": index,
                    "__index": None if index is None else f"{index:02d}",
                },
            }
            for index in ([None] + list(range(0, c or 0)))
        ]
