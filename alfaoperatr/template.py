from yarl import URL
from asyncio import Queue, gather, sleep, get_event_loop
import base64
import os
import distutils.util
import jinja2
import json
import sys
import yaml
import six
import jmespath
import re

from netaddr import IPAddress
from aiohttp import ClientSession

from .log import Log
from .config import Config
from .producer import Producer

COOLDOWN       = int(os.environ.get('ALFA_COOLDOWN', 5))
DEBUG_PATH     =     os.environ.get('DEBUG_PATH', None)

if DEBUG_PATH:
  os.makedirs(os.path.abspath(DEBUG_PATH), exist_ok=True)

class AlfaTemplate:
  def __init__(self, alfa_template, queue = None, session = None, config = None, logger = None):
    self.session = ClientSession() if session is None else session
    self.queue = Queue() if queue is None else queue
    self.config = Config() if config is None else config
    self.logger = Log.get_logger(f'{__name__}({alfa_template["metadata"]["selfLink"]})') if logger is None else logger
    self.alfa_template = alfa_template

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
      yield Producer(
        url = self.config[kind]["url"],
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
  def __init__(self, alfa_template, session = None, queue = None, config = None, logger = None):
    self.session = ClientSession() if session is None else session
    self.queue = Queue() if queue is None else queue
    self.config = Config() if config is None else config
    self.logger = Log.get_logger(f'{__name__}({alfa_template["metadata"]["name"]})') if logger is None else logger
    
    self.kinds = alfa_template["spec"]["kinds"]
    self.template = alfa_template["spec"]["template"]
    self.metadata = alfa_template["metadata"]

  async def loop(self):
    while True:
      self.logger.info(f'Sleeping until next event')
      event = await self.queue.get()
      self.logger.info(f'Consumer awaiting cooldown for {COOLDOWN} seconds')
      await sleep(COOLDOWN)
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
  
  def get_jinja(self):
    j2environment = jinja2.Environment(loader=jinja2.BaseLoader, extensions=["jinja2_ansible_filters.AnsibleCoreFiltersExtension"])
    # add b64decode filter to jinja2 env
    j2environment.filters["b64decode"] = base64.b64decode
    j2environment.filters["ipaddr"] = ipaddr
    j2environment.filters["json_query"] = json_query
    j2environment.filters["unique_dict"] = unique_dict
    j2environment.tests["is_subset"] = is_subset
    j2environment.tests["is_superset"] = is_superset
    return j2environment

  async def consume(self):
    j2environment = self.get_jinja()
    
    try:
      self.logger.info(f'Getting Items')
      items = list(await self.get_items())
      self.logger.info(f' - Retrieved {len(items)} Items')
      if DEBUG_PATH:
        with open(os.path.join(os.path.abspath(DEBUG_PATH), "alfatemplate-" + self.metadata["name"] + "-input.json"), 'w') as outfile:
          json.dump({'kinds': self.kinds, 'items': items, 'template': self.template, 'metadata': self.metadata}, outfile, indent=2)
    except Exception as e:
      self.logger.error(f'Error Getting Items: {repr(e)}')
      return
    
    try:
      self.logger.info(f'Rendering Template')
      j2result = j2environment.from_string(source=self.template).render(items=items, metadata=self.metadata)
      renders = list(yaml.load_all(j2result, Loader=yaml.FullLoader))
      self.logger.info(f' - Rendered {len(renders)} Items')
      if DEBUG_PATH:
        with open(os.path.join(os.path.abspath(DEBUG_PATH), "alfatemplate-" + self.metadata["name"] + "-result.yaml"), 'w') as outfile:
          outfile.write(j2result)
    except Exception as e:
      self.logger.error(f'Error Rendering Template: {repr(e)}')
      return

    for render in renders:
      try:
        url = URL(self.config[render["kind"]]["url"]).parent / URL(self.config[render["kind"]]["url"]).name / render["metadata"]["name"]
        if "namespace" in render["metadata"]:
          url = URL(self.config[render["kind"]]["url"]).parent / URL("namespaces").name / URL(render["metadata"]["namespace"]).name / URL(self.config[render["kind"]]["url"]).name / render["metadata"]["name"]
        self.logger.info(f'Getting Render {url}')
        async with self.session.request('get', url) as item_get_response:
          if item_get_response.status in [404]:
            url = url.parent
            try:
              self.logger.info(f'Creating Render {url}')
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
                if DEBUG_PATH:
                  with open(os.path.join(os.path.abspath(DEBUG_PATH), f'{item_post["metadata"].get("namespace","cluster")}-{item_post["metadata"]["name"]}-{item_post["kind"]}-{item_post["metadata"]["resourceVersion"]}.yaml'), 'w') as outfile:
                    yaml.dump(item_post, outfile)
                self.logger.info(f'Created {render["kind"]} {url / render["metadata"]["name"]}, resourceVersion {item_post["metadata"]["resourceVersion"]}')
            except Exception as e:
              self.logger.error(f'Error Creating Render: {repr(e)}')
              continue
            
          else:
            item_get = await item_get_response.json()
            if DEBUG_PATH:
              with open(os.path.join(os.path.abspath(DEBUG_PATH), f'{item_get["metadata"].get("namespace","cluster")}-{item_get["metadata"]["name"]}-{item_get["kind"]}-{item_get["metadata"]["resourceVersion"]}.yaml'), 'w') as outfile:
                yaml.dump(item_get, outfile)
            render["metadata"]["resourceVersion"] = item_get["metadata"]["resourceVersion"]
            try:
              self.logger.info(f'Updating Render {url}')
              self.logger.debug(f'HTTP PUT {url}: {json.dumps(render)}')
              async with self.session.request('put', url, json=render) as item_put_response:
                if item_put_response.status in [404]:
                  self.logger.error(f'HTTP PUT response: {item_put_response.status}')
                  continue
                item_put = await item_put_response.json()
                if item_put["metadata"]["resourceVersion"] != item_get["metadata"]["resourceVersion"]:
                  if DEBUG_PATH:
                    with open(os.path.join(os.path.abspath(DEBUG_PATH), f'{item_put["metadata"].get("namespace","cluster")}-{item_put["metadata"]["name"]}-{item_put["kind"]}-{item_put["metadata"]["resourceVersion"]}.yaml'), 'w') as outfile:
                      yaml.dump(item_put, outfile)
                  self.logger.info(f'Updated {render["kind"]} {render["metadata"].get("namespace","cluster")}\{render["metadata"]["name"]}, from resourceVersion {item_get["metadata"]["resourceVersion"]} to {item_put["metadata"]["resourceVersion"]}')
                else:
                  self.logger.info(f'No change to {render["kind"]} {render["metadata"].get("namespace","cluster")}\{render["metadata"]["name"]}, resourceVersion still {item_get["metadata"]["resourceVersion"]}')
            except Exception as e:
              self.logger.error(f'Error Updating Render: {repr(e)}')
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

def json_query(v, f):
  return jmespath.search(f, v)

def unique_dict(v):
  return list(yaml.load(y, Loader=yaml.FullLoader) for y in set(yaml.dump(d) for d in v))

def is_superset(v, subset):
  return is_subset(subset, v)

# https://stackoverflow.com/posts/18335110/timeline
# cc-by-sa 4.0
def is_subset(v, superset):
  try:
    for key, value in v.items():
      if type(value) is dict:
        result = is_subset(value, superset[key])
        assert result
      else:
        assert superset[key] == value
        result = True
  except (AssertionError, KeyError):
    result = False
  return result

def ipaddr(value, action):
  if action == "revdns":
    return IPAddress(value).reverse_dns.strip('.')
  raise NotImplementedError
