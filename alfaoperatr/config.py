from collections.abc import Mapping
from difflib import get_close_matches
from os import environ, makedirs
from requests import request
from yarl import URL
from re import compile, Pattern

from .log import Log

class Config(Mapping):
  def __init__(self,
               api_proxy = 'http://localhost:8001',
               app_filter = '.*',
               cooldown = 5,
               debug_path = None,
               log_level = 'INFO',
               logger = None,
               template_filter = '.*'):
    self.api_proxy =       api_proxy                                  if isinstance(api_proxy, URL) else URL(api_proxy)
    self.app_filter =      app_filter                                 if isinstance(app_filter, Pattern) else compile(app_filter)
    self.cooldown =        cooldown
    self.debug_path =      debug_path
    self.log_level =       log_level
    self.logger =          Log.get_logger(f'Config()', log_level) if logger is None else logger
    self.template_filter = template_filter                            if isinstance(template_filter, Pattern) else compile(template_filter)



    if self.debug_path:
      makedirs(self.debug_path, exist_ok=True)

    self._kinds = dict({item['kind']:item for item in self._get_kinds()})
    with request('get', self._kinds["AlfaTemplate"]["url"]) as r:
      for ac in r.json()["items"]:
        for kind in ac["spec"]["kinds"]:
          if kind not in self._kinds.keys():
            maybe = get_close_matches(kind, self._kinds.keys(),1)
            if len(maybe) == 0:
              self.logger.error(f'AlfaTemplate "{ac["metadata"]["name"]}" refers to Kind "{kind}" that does not exist, will not monitor this kind.')
            else:
              self.logger.error(f'AlfaTemplate "{ac["metadata"]["name"]}" refers to Kind "{kind}" that does not exist, will not monitor this kind. Did you mean "{maybe[0]}"?')
            continue
          self._kinds[kind]["templates"].append({"template": ac["spec"]["template"], "metadata": ac["metadata"]})

  def __getitem__(self, k):
    return self._kinds.__getitem__(k)

  def __iter__(self):
    return self._kinds.__iter__()

  def __len__(self):
    return self._kinds.__len__()

  def _get_versions(self):
    yield self.api_proxy / 'api/v1'

  def _get_group_versions(self):
    with request('get', self.api_proxy / 'apis') as r:
      for group in r.json()["groups"]:
        for version in group["versions"]:
          if version["groupVersion"] != group["preferredVersion"]["groupVersion"]:
            yield URL(r.url) / version["groupVersion"]
        yield URL(r.url) / group["preferredVersion"]["groupVersion"]

  def _get_kinds(self):
    for url in self._get_versions():
      with request('get', url) as r:
        for resource in r.json()["resources"]:
          if '/' not in resource["name"]:
            yield {
                'version': r.json()["groupVersion"],
                'type': resource["name"],
                'kind': resource["kind"],
                'templates': [],
                'url': URL(r.url) / resource["name"],
              }

    for url in self._get_group_versions():
      with request('get', url) as r:
        for resource in r.json()["resources"]:
          if '/' not in resource["name"]:
            yield {
                'group': r.json()["groupVersion"].split('/')[0],
                'version': r.json()["groupVersion"].split('/')[-1],
                'type': resource["name"],
                'kind': resource["kind"],
                'templates': [],
                'url': URL(r.url) / resource["name"],
              }
