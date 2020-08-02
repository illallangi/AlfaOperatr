import json
import os
from asyncio import Queue, gather, get_event_loop, sleep
from functools import reduce

from aiohttp import ClientSession

import six

import yaml

from yarl import URL

from .jinja import AlfaJinja
from .log import AlfaLog
from .producer import AlfaProducer


class AlfaTemplate:
    def __init__(self, alfa_template, config, queue=None, session=None, logger=None):
        self.alfa_template = alfa_template
        self.config = config
        self.session = ClientSession() if session is None else session
        self.queue = Queue() if queue is None else queue
        self.logger = AlfaLog.get_logger(f'AlfaTemplate({alfa_template["metadata"]["name"]})', self.config.log_level) if logger is None else logger

    async def loop(self):
        self.logger.info('loop starting')
        self.task = gather(*self.get_coroutines())
        await self.task
        self.logger.info('loop completed')

    def get_coroutines(self):
        yield AlfaTemplateConsumer(
            self.alfa_template,
            session=self.session,
            queue=self.queue,
            config=self.config).loop()

        for kind in [
            *[self.alfa_template['spec']['kinds']['parent']['kind']],
            *[kind['kind'] for kind in self.alfa_template['spec']['kinds']['monitored']]
        ]:
            yield AlfaProducer(
                kind=kind,
                resource_version=None,
                session=self.session,
                queue=self.queue,
                config=self.config).loop()

    def __del__(self):
        self.logger.info('__del__ starting')
        if not get_event_loop().is_closed() and self.task is not None:
            self.task.cancel()
        self.logger.info('__del__ completed')


class AlfaTemplateConsumer:
    def __init__(self, alfa_template, config, jinja=None, session=None, queue=None, logger=None):
        self.alfa_template = alfa_template
        self.config = config
        self.jinja = AlfaJinja(config) if jinja is None else jinja
        self.session = ClientSession() if session is None else session
        self.queue = Queue() if queue is None else queue
        self.logger = AlfaLog.get_logger(f'AlfaTemplateConsumer({alfa_template["metadata"]["name"]})', self.config.log_level) if logger is None else logger

    async def loop(self):
        while True:
            self.logger.debug('Sleeping until next event')
            await self.queue.get()
            self.logger.info(f'Consumer awaiting cooldown for {self.config.cooldown} seconds')
            await sleep(self.config.cooldown)
            while not self.queue.empty():
                self.queue.get_nowait()
            await self.consume()

    async def get_items(self, kind):
        items = []
        async with self.session.request('get', self.config[kind]["url"]) as item_collection_response:
            item_collection = await item_collection_response.json()
            for item in item_collection["items"]:
                async with self.session.request('get', URL(item_collection_response.url).with_path(item["metadata"]["selfLink"])) as item_response:
                    item = await item_response.json()
                    items.append(item)
        return items

    def get_indexes(self, item):
        if self.alfa_template['spec']['filter'] == '':
            return [None]

        value = reduce(lambda d, key: d.get(key) if d else None, self.alfa_template['spec']['filter'].strip('.').split('.'), item)

        if value is None:
            return []

        if isinstance(value, bool) and not value:
            return []

        if isinstance(value, int):
            return range(0, value)

        return [None]

    async def consume(self):
        try:
            self.logger.info('Getting Items')
            items = {
                kind: await self.get_items(kind)
                for kind in [
                    *[self.alfa_template['spec']['kinds']['parent']['kind']],
                    *[kind['kind'] for kind in self.alfa_template['spec']['kinds']['monitored']]
                ]
            }
        except Exception as e:
            self.logger.error(f'Error Getting Items: {repr(e)}')
            return

        for kind in [
            *[self.alfa_template['spec']['kinds']['parent']['kind']],
            *[kind['kind'] for kind in self.alfa_template['spec']['kinds']['monitored']]
        ]:
            self.logger.info(f' - Retrieved {len(items[kind])} {kind}')

        children = unique_dict(
            [
                {
                    'kind': self.config[self.alfa_template['spec']['kinds']['child']['kind']]['kind'],
                    'apiVersion': '/'.join([
                        self.config[self.alfa_template['spec']['kinds']['child']['kind']]['group'],
                        self.config[self.alfa_template['spec']['kinds']['child']['kind']]['version']
                    ]),
                    'metadata': {
                        'namespace': o['metadata']['namespace'],
                        'labels': {
                            self.alfa_template['spec']['labels']['name']:
                                o.get('metadata', {})
                                 .get('labels', {})
                                 .get(self.alfa_template['spec']['labels']['name'],
                                      self.alfa_template
                                          .get('metadata', {})
                                          .get('labels', {})
                                          .get(self.alfa_template['spec']['labels']['name'])),
                            self.alfa_template['spec']['labels']['part-of']:
                                o.get('metadata', {})
                                 .get('labels', {})
                                 .get(self.alfa_template['spec']['labels']['part-of'],
                                      self.alfa_template
                                          .get('metadata', {})
                                          .get('labels', {})
                                          .get(self.alfa_template['spec']['labels']['part-of'], o['metadata']['name'])),
                            self.alfa_template['spec']['labels']['instance']:
                                '-'.join(
                                    filter(
                                        None,
                                        [
                                            o.get('metadata', {})
                                             .get('labels', {})
                                             .get(self.alfa_template['spec']['labels']['instance'],
                                                  self.alfa_template
                                                      .get('metadata', {})
                                                      .get('labels', {})
                                                      .get(self.alfa_template['spec']['labels']['instance'], o['metadata']['name'])),
                                            None if i is None else f'{i:02}',
                                        ]
                                    )
                            ),
                            self.alfa_template['spec']['labels']['component']:
                                o.get('metadata', {})
                                 .get('labels', {})
                                 .get(self.alfa_template['spec']['labels']['component'],
                                      self.alfa_template
                                          .get('metadata', {})
                                          .get('labels', {})
                                          .get(self.alfa_template['spec']['labels']['component'])),
                            self.alfa_template['spec']['labels']['scope']:
                                self.alfa_template['spec']['scope'],
                            self.alfa_template['spec']['labels']['managed-by']:
                                self.alfa_template['metadata']['name']
                        },
                        'name': '-'.join(
                            filter(
                                None,
                                [
                                    o.get('metadata', {})
                                    .get('labels', {})
                                    .get(self.alfa_template['spec']['labels']['name'],
                                         self.alfa_template
                                             .get('metadata', {})
                                             .get('labels', {})
                                             .get(self.alfa_template['spec']['labels']['name'])),
                                    o.get('metadata', {})
                                    .get('labels', {})
                                    .get(self.alfa_template['spec']['labels']['instance'],
                                         self.alfa_template
                                             .get('metadata', {})
                                             .get('labels', {})
                                             .get(self.alfa_template['spec']['labels']['instance'],
                                                  o['metadata']['name'])),
                                    None if i is None else f'{i:02}',
                                    o.get('metadata', {})
                                    .get('labels', {})
                                    .get(self.alfa_template['spec']['labels']['component'],
                                         self.alfa_template
                                             .get('metadata', {})
                                             .get('labels', {})
                                             .get(self.alfa_template['spec']['labels']['component'])),
                                ]
                            )
                        )
                    },
                    'index':
                    {
                        'int': i,
                        'string': None if i is None else f'{i:02}'
                    },
                    'spec': o['spec']
                }
                for o in items[self.alfa_template['spec']['kinds']['parent']['kind']]
                for i in self.get_indexes(o)
            ]
        )

        objects = unique_dict(
            [
                {
                    'kind': o['kind'],
                    'apiVersion': o['apiVersion'],
                    'metadata': {
                        'namespace': o['metadata']['namespace'],
                        'labels': {
                            self.alfa_template['spec']['labels']['name']: o['metadata']['labels'][self.alfa_template['spec']['labels']['name']],
                            self.alfa_template['spec']['labels']['instance']: o['metadata']['labels'][self.alfa_template['spec']['labels']['part-of']],
                            self.alfa_template['spec']['labels']['component']: o['metadata']['labels'][self.alfa_template['spec']['labels']['component']],
                            self.alfa_template['spec']['labels']['scope']: o['metadata']['labels'][self.alfa_template['spec']['labels']['scope']],
                            self.alfa_template['spec']['labels']['managed-by']: o['metadata']['labels'][self.alfa_template['spec']['labels']['managed-by']],
                        },
                        'name': '-'.join(
                            filter(
                                None,
                                [
                                    o['metadata']['labels'][self.alfa_template['spec']['labels']['name']],
                                    o['metadata']['labels'][self.alfa_template['spec']['labels']['part-of']],
                                    o['metadata']['labels'][self.alfa_template['spec']['labels']['component']]
                                ]
                            )
                        )
                    },
                    'spec': o['spec']
                }
                for o in children
            ]
        )

        namespaces = unique_dict(
            [
                {
                    'kind': o['kind'],
                    'apiVersion': o['apiVersion'],
                    'metadata': {
                        'namespace': o['metadata']['namespace'],
                        'labels': {
                            self.alfa_template['spec']['labels']['name']: o['metadata']['labels'][self.alfa_template['spec']['labels']['name']],
                            self.alfa_template['spec']['labels']['component']: o['metadata']['labels'][self.alfa_template['spec']['labels']['component']],
                            self.alfa_template['spec']['labels']['scope']: o['metadata']['labels'][self.alfa_template['spec']['labels']['scope']],
                            self.alfa_template['spec']['labels']['managed-by']: o['metadata']['labels'][self.alfa_template['spec']['labels']['managed-by']],
                        },
                        'name': '-'.join(
                            filter(
                                None,
                                [
                                    o['metadata']['labels'][self.alfa_template['spec']['labels']['name']],
                                    o['metadata']['labels'][self.alfa_template['spec']['labels']['component']]
                                ]
                            )
                        )
                    },
                    'spec': o['spec']
                }
                for o in objects
            ]
        )

        clusters = unique_dict(
            [
                {
                    'kind': o['kind'],
                    'apiVersion': o['apiVersion'],
                    'metadata': {
                        'labels': {
                            self.alfa_template['spec']['labels']['name']: o['metadata']['labels'][self.alfa_template['spec']['labels']['name']],
                            self.alfa_template['spec']['labels']['component']: o['metadata']['labels'][self.alfa_template['spec']['labels']['component']],
                            self.alfa_template['spec']['labels']['scope']: o['metadata']['labels'][self.alfa_template['spec']['labels']['scope']],
                            self.alfa_template['spec']['labels']['managed-by']: o['metadata']['labels'][self.alfa_template['spec']['labels']['managed-by']],
                        },
                        'name': '-'.join(
                            filter(
                                None,
                                [
                                    o['metadata']['labels'][self.alfa_template['spec']['labels']['name']],
                                    o['metadata']['labels'][self.alfa_template['spec']['labels']['component']]
                                ]
                            )
                        )
                    },
                    'spec': o['spec']
                }
                for o in namespaces
            ]
        )

        inputs = {
            'Cluster': clusters,
            'Namespace': namespaces,
            'Object': objects,
            'Child': children
        }.get(self.alfa_template['spec']['scope'])

        if self.config.debug_path:
            with open(os.path.join(self.config.debug_path, "alfatemplate-" + self.alfa_template["metadata"]["name"] + "-input.yaml"), 'w') as outfile:
                outfile.write(yaml.dump_all(inputs))

        try:
            self.logger.info('Rendering Template')
            renders = unique_dict(
                [
                    merge(
                        yaml.load(
                            self.jinja.render(
                                self.alfa_template['spec']['template'],
                                items=items,
                                parent=self.config[self.alfa_template['spec']['kinds']['parent']['kind']],
                                child=self.config[self.alfa_template['spec']['kinds']['child']['kind']],
                                item=o,
                                **o
                            ),
                            Loader=yaml.FullLoader
                        ),
                        {
                            i: o[i]
                            for i in o
                            if i in ['apiVersion', 'kind', 'metadata']
                        }
                    )
                    for o in inputs
                ]
            )
            self.logger.info(f' - Rendered {len(renders)} items')
            if self.config.debug_path:
                with open(os.path.join(self.config.debug_path, "alfatemplate-" + self.alfa_template["metadata"]["name"] + "-result.yaml"), 'w') as outfile:
                    outfile.write(yaml.dump_all(renders))
        except Exception as e:
            self.logger.error(f'Unknown Exception Rendering Template: {repr(e)}')
            return

        for render in renders:
            if render is None or "kind" not in render or self.config.debug_path:
                continue
            try:
                url = URL(self.config[render["kind"]]["url"]).parent / URL(self.config[render["kind"]]["url"]).name / render["metadata"]["name"]
                if "namespace" in render["metadata"]:
                    url = URL(self.config[render["kind"]]["url"]).parent / URL("namespaces").name / URL(render["metadata"]["namespace"]).name / URL(self.config[render["kind"]]["url"]).name / render["metadata"]["name"]
                self.logger.info(f'Getting {render["kind"]} {url}')
                async with self.session.request('get', url) as item_get_response:
                    if item_get_response.status in [404]:
                        url = url.parent
                        try:
                            self.logger.info(f'Creating {render["kind"]} {url}')
                            self.logger.debug(f'HTTP POST {url}: {json.dumps(render)}')
                            async with self.session.request('post', url, json=render) as item_post_response:
                                if item_post_response.headers.get('content-type') not in ['application/json'] or item_post_response.status in [404]:
                                    item_post = await item_post_response.text()
                                    self.logger.error(f'HTTP POST {url} {item_post_response.status} {item_post}')
                                    continue
                                item_post = await item_post_response.json()
                                if item_post['kind'] == 'Status' and item_post['status'] == 'Failure':
                                    self.logger.error(f'HTTP POST {url} failed: {item_post["message"]} {json.dumps(item_post)}')
                                    continue
                                self.logger.debug(f'HTTP POST {url} {item_post_response.status} {json.dumps(item_post)}')
                                if self.config.debug_path:
                                    with open(os.path.join(self.config.debug_path, f'{item_post["metadata"].get("namespace","cluster")}-{item_post["metadata"]["name"]}-{item_post["kind"]}-{item_post["metadata"]["resourceVersion"]}.yaml'), 'w') as outfile:
                                        yaml.dump(item_post, outfile)
                                self.logger.info(f'Created {render["kind"]} {url / render["metadata"]["name"]}, resourceVersion {item_post["metadata"]["resourceVersion"]}')
                        except Exception as e:
                            self.logger.error(f'Error Creating {render["kind"]}: {repr(e)}')
                            continue

                    else:
                        item_get = await item_get_response.json()
                        if self.config.debug_path:
                            with open(os.path.join(self.config.debug_path, f'{item_get["metadata"].get("namespace","cluster")}-{item_get["metadata"]["name"]}-{item_get["kind"]}-{item_get["metadata"]["resourceVersion"]}.yaml'), 'w') as outfile:
                                yaml.dump(item_get, outfile)
                        render["metadata"]["resourceVersion"] = item_get["metadata"]["resourceVersion"]

                        if not self.alfa_template['spec']['update']:
                            self.logger.info(f'Not updating as update set to false {render["kind"]} {url}')
                            continue

                        # Ugly hack to avoid race condition
                        if "deployment.kubernetes.io/revision" in item_get.get("metadata", {}).get("annotations", {}):
                            if "metadata" not in render:
                                render["metadata"] = {}
                            if "annotations" not in render["metadata"]:
                                render["metadata"]["annotations"] = {}
                            render["metadata"]["annotations"]["deployment.kubernetes.io/revision"] = item_get["metadata"]["annotations"]["deployment.kubernetes.io/revision"]

                        # Ugly hack to avoid race condition
                        if "clusterIP" in item_get.get("spec", {}):
                            if "spec" not in render:
                                render["spec"] = {}
                            render["spec"]["clusterIP"] = item_get["spec"]["clusterIP"]

                        try:
                            self.logger.info(f'Updating {render["kind"]} {url}')
                            self.logger.debug(f'HTTP PUT {url}: {json.dumps(render)}')
                            async with self.session.request('put', url, json=render) as item_put_response:
                                if item_put_response.headers.get('content-type') not in ['application/json'] or item_put_response.status in [404]:
                                    item_post = await item_put_response.text()
                                    self.logger.error(f'HTTP PUT {url} {item_put_response.status} {item_post}')
                                    continue
                                item_put = await item_put_response.json()
                                if item_put['kind'] == 'Status' and item_put['status'] == 'Failure':
                                    self.logger.error(f'HTTP PUT {url} failed: {item_put["message"]} {json.dumps(item_put)}')
                                    continue
                                if "resourceVersion" not in item_put["metadata"] or \
                                        "resourceVersion" not in item_get["metadata"] or \
                                        item_put["metadata"].get("resourceVersion", None) != item_get["metadata"].get("resourceVersion", None):
                                    if self.config.debug_path:
                                        with open(os.path.join(self.config.debug_path, f'{item_put["metadata"].get("namespace","cluster")}-{item_put["metadata"]["name"]}-{item_put["kind"]}-{item_put["metadata"]["resourceVersion"]}.yaml'), 'w') as outfile:
                                            yaml.dump(item_put, outfile)
                                    self.logger.info(f'Updated {render["kind"]} {render["metadata"].get("namespace","cluster")}\\{render["metadata"]["name"]}, from resourceVersion {item_get["metadata"]["resourceVersion"]} to {item_put["metadata"]["resourceVersion"]}')
                                else:
                                    self.logger.info(f'No change to {render["kind"]} {render["metadata"].get("namespace","cluster")}\\{render["metadata"]["name"]}, resourceVersion still {item_get["metadata"]["resourceVersion"]}')
                        except Exception as e:
                            self.logger.error(f'Error Updating {render["kind"]}: {repr(e)}')
                            continue
            except Exception as e:
                self.logger.error(f'Error Getting Render: {repr(e)}')
                continue


def merge(a, b, path=None, override=True):
    "merges b into a"
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge(a[key], b[key], path=path + [str(key)], override=override)
            elif a[key] == b[key]:
                pass  # same leaf value
            else:
                if not override:
                    raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a


def unique_dict(input):
    return [yaml.load(y, Loader=yaml.FullLoader) for y in {yaml.dump(d) for d in input}]


# Get around pyyaml removing leading 0s
# https://github.com/yaml/pyyaml/issues/98
def string_representer(dumper, value):
    if value.startswith("0"):
        return dumper.represent_scalar("tag:yaml.org,2002:str", value, style="'")
    return dumper.represent_scalar("tag:yaml.org,2002:str", value)


yaml.Dumper.add_representer(six.text_type, string_representer)
