from yarl import URL
from asyncio import Queue, gather, sleep, get_event_loop
import base64
import os
import distutils.util
import json
import sys
import yaml
import six
import re
from hashlib import sha256
from more_itertools import one

from netaddr import IPAddress
from aiohttp import ClientSession

from .log import AlfaLog
from .producer import AlfaProducer
from .jinja import AlfaJinja

class AlfaTemplate:
    def __init__(self, alfa_template, config, queue = None, session = None, logger = None):
        self.alfa_template = alfa_template
        self.config = config
        self.session = ClientSession() if session is None else session
        self.queue = Queue() if queue is None else queue
        self.logger = AlfaLog.get_logger(f'AlfaTemplate({alfa_template["metadata"]["name"]})', self.config.log_level) if logger is None else logger

    async def loop(self):
        self.logger.info(f'loop starting')
        self.task = gather(*self.get_coroutines())
        await self.task
        self.logger.info(f'loop completed')

    def get_coroutines(self):
        yield AlfaTemplateConsumer(
            self.alfa_template,
            session = self.session,
            queue = self.queue,
            config = self.config).loop()

        for kind in self.alfa_template["spec"]["kinds"]:
            yield AlfaProducer(
                kind = kind,
                resource_version = None,
                session = self.session,
                queue = self.queue,
                config = self.config).loop()

    def __del__(self):
        self.logger.info(f'__del__ starting')
        if not get_event_loop().is_closed() and self.task is not None:
            self.task.cancel()
        self.logger.info(f'__del__ completed')


class AlfaTemplateConsumer:
    def __init__(self, alfa_template, config, jinja = None, session = None, queue = None, logger = None):
        self.kinds = alfa_template["spec"]["kinds"]
        self.template = alfa_template["spec"]["template"]
        self.metadata = alfa_template["metadata"]
        self.update = alfa_template["spec"]["update"]
        self.config = config
        self.jinja = AlfaJinja(config) if jinja is None else jinja
        
        self.session = ClientSession() if session is None else session
        self.queue = Queue() if queue is None else queue
        self.logger = AlfaLog.get_logger(f'AlfaTemplateConsumer({alfa_template["metadata"]["name"]})', self.config.log_level) if logger is None else logger

    async def loop(self):
        while True:
            self.logger.debug(f'Sleeping until next event')
            event = await self.queue.get()
            self.logger.info(f'Consumer awaiting cooldown for {self.config.cooldown} seconds')
            await sleep(self.config.cooldown)
            while not self.queue.empty():
                self.queue.get_nowait()
            await self.consume()

    async def get_items(self):
        items = []
        for kind in self.kinds:
            async with self.session.request('get', self.config[kind]["url"]) as item_collection_response:
                item_collection = await item_collection_response.json()
                for item in item_collection["items"]:
                    async with self.session.request('get', URL(item_collection_response.url).with_path(item["metadata"]["selfLink"])) as item_response:
                        item = await item_response.json()
                        items.append(item)
        return items

    async def consume(self):
        try:
            self.logger.info(f'Getting Items')
            items = list(await self.get_items())
            self.logger.info(f' - Retrieved {len(items)} Items')
            if self.config.debug_path:
                with open(os.path.join(self.config.debug_path, "alfatemplate-" + self.metadata["name"] + "-input.json"), 'w') as outfile:
                    json.dump({'kinds': self.kinds, 'items': items, 'template': self.template, 'metadata': self.metadata}, outfile, indent=2)
        except Exception as e:
            self.logger.error(f'Error Getting Items: {repr(e)}')
            return
        
        try:
            self.logger.info(f'Rendering Template')
            j2result = self.jinja.render(
                    self.template, 
                    items=items,
                    metadata=self.metadata,
                    objects=[
                        {
                            'item':         {i:item.get('metadata',{})[i] for i in item.get('metadata',{}) if i in ['annotations','labels','name','namespace','selfLink','uid']},
                            'template': {i:self.metadata[i]                     for i in self.metadata                     if i in ['annotations','labels','name','namespace','selfLink','uid']},
                            'spec':         item.get('spec',{}),
                            'kind':         item.get('kind','')
                        } for item in items
                    ]
                )
            if j2result is None:
                return
            self.logger.info(f' - Rendered {len(j2result)} bytes')
        except Exception as e:
            self.logger.error(f'Unknown Exception Rendering Template: {repr(e)}')
            return
        
        try:
            self.logger.info(f'Loading Objects from Rendered Template')
            renders = list(yaml.load_all(j2result, Loader=yaml.FullLoader))
            self.logger.info(f' - Loaded {len(renders)} Items')
            if self.config.debug_path:
                with open(os.path.join(self.config.debug_path, "alfatemplate-" + self.metadata["name"] + "-result.yaml"), 'w') as outfile:
                    outfile.write(j2result)
        except Exception as e:
            self.logger.error(f'Unknown Exception Loading Objects from Rendered Template: {repr(e)}')
            return

        for render in renders:
            if render is None or "kind" not in render:
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
                        
                        if not self.update:
                            self.logger.info(f'Not updating as update set to false {render["kind"]} {url}')
                            continue
                        
                        # Ugly hack to avoid race condition
                        if "deployment.kubernetes.io/revision" in item_get.get("metadata",{}).get("annotations",{}):
                            if "metadata" not in render:
                                render["metadata"] = {}
                            if "annotations" not in render["metadata"]:
                                render["metadata"]["annotations"] = {}
                            render["metadata"]["annotations"]["deployment.kubernetes.io/revision"] = item_get["metadata"]["annotations"]["deployment.kubernetes.io/revision"]

                        # Ugly hack to avoid race condition
                        if "clusterIP" in item_get.get("spec",{}):
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
                                    self.logger.info(f'Updated {render["kind"]} {render["metadata"].get("namespace","cluster")}\{render["metadata"]["name"]}, from resourceVersion {item_get["metadata"]["resourceVersion"]} to {item_put["metadata"]["resourceVersion"]}')
                                else:
                                    self.logger.info(f'No change to {render["kind"]} {render["metadata"].get("namespace","cluster")}\{render["metadata"]["name"]}, resourceVersion still {item_get["metadata"]["resourceVersion"]}')
                        except Exception as e:
                            self.logger.error(f'Error Updating {render["kind"]}: {repr(e)}')
                            continue
            except Exception as e:
                self.logger.error(f'Error Getting Render: {repr(e)}')
                continue


# Get around pyyaml removing leading 0s
# https://github.com/yaml/pyyaml/issues/98
def string_representer(dumper, value):
    if value.startswith("0"):
        return dumper.represent_scalar("tag:yaml.org,2002:str", value, style="'")
    return dumper.represent_scalar("tag:yaml.org,2002:str", value)


yaml.Dumper.add_representer(six.text_type, string_representer)
