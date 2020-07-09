from collections.abc import Mapping
from difflib import get_close_matches
from os import environ
from requests import request
from yarl import URL

from .log import Log

KUBE_API_PROXY = URL(environ.get('KUBE_API_PROXY', f'http://localhost:8001'))

class Config(Mapping):
  def __init__(self, logger = None):
    if logger is None: logger = Log.get_logger(f'{__name__}()')
    self._kinds = dict({item['kind']:item for item in self._get_kinds()})
    with request('get', self._kinds["AlfaTemplate"]["url"]) as r:
      for ac in r.json()["items"]:
        for kind in ac["spec"]["kinds"]:
          if kind not in self._kinds.keys():
            maybe = get_close_matches(kind, self._kinds.keys(),1)
            if len(maybe) == 0:
              logger.error(f'AlfaTemplate "{ac["metadata"]["name"]}" refers to Kind "{kind}" that does not exist, will not monitor this kind.')
            else:
              logger.error(f'AlfaTemplate "{ac["metadata"]["name"]}" refers to Kind "{kind}" that does not exist, will not monitor this kind. Did you mean "{maybe[0]}"?')
            continue
          self._kinds[kind]["templates"].append({"template": ac["spec"]["template"], "metadata": ac["metadata"]})
    
  def __getitem__(self, k):
    return self._kinds.__getitem__(k)
  
  def __iter__(self):
    return self._kinds.__iter__()
  
  def __len__(self):
    return self._kinds.__len__()
 
  def _get_versions(self):
    yield KUBE_API_PROXY / 'api/v1'
  
  def _get_group_versions(self):
    with request('get', KUBE_API_PROXY / 'apis') as r:
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
