from yarl import URL
from aiohttp import ClientSession
from asyncio import Queue, gather, sleep
from asyncio.exceptions import TimeoutError
import json

from .config import Config
from .log import Log


class Producer:
  def __init__(self, url, session = None, resource_version = None, queue = None, config = None, logger = None):
    self.url = url if isinstance(url, URL) else URL(url)
    self.resource_version = resource_version
    self.session = ClientSession() if session is None else session
    self.queue = Queue() if queue is None else queue
    self.config = Config() if config is None else config
    self.logger = Log.get_logger(f'{__name__}({self.url})') if logger is None else logger

  async def loop(self):
    while True:
      try:
        params = {'watch': 1}
        if self.resource_version != None:
          params['resourceVersion'] = self.resource_version
        async with self.session.request('get', self.url.with_query(**params)) as response:
          self.logger.info(f'Connected, {URL(response.url).query_string}')
          async for line in response.content:
            if line:
              try:
                event = json.loads(line)
              except json.decoder.JSONDecodeError as e:
                self.logger.error(f'Encountered JSONDecodeError "{repr(e)}" on "{line}", continuing.')
                continue
              await self.handle_event(event)
              if int(event["object"]["metadata"]["resourceVersion"]) > (self.resource_version or 0):
                self.resource_version = int(event["object"]["metadata"]["resourceVersion"])
            await sleep(0)
      except TimeoutError as e:
        self.logger.info(f'Timed out, restarting at resourceVersion {self.resource_version}')

  async def handle_event(self, event):
    self.logger.info(f'{event["object"]["metadata"]["name"]} {event["type"].lower()} (resourceVersion {event["object"]["metadata"]["resourceVersion"]})')
    self.logger.debug(f'Received event {json.dumps(event)}')
    await self.queue.put({'event': event})
