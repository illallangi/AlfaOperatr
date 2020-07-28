from aiohttp import ClientSession
from asyncio import Queue, get_event_loop, gather
from json import dumps
from .log import Log
from .template import AlfaTemplate
from .producer import Producer

class AlfaController:
  def __init__(self, config, queue = None, session = None, logger = None):
    self.config = config
    self.session = ClientSession() if session is None else session
    self.queue = Queue() if queue is None else queue
    self.logger = Log.get_logger(f'{__name__}()', self.config.log_level) if logger is None else logger

  async def loop(self):
    self.logger.info(f'loop starting')
    self.task = gather(*self.get_coroutines())
    await self.task
    self.logger.info(f'loop completed')

  def get_coroutines(self):
    yield AlfaControllerConsumer(
      session = self.session,
      queue = self.queue,
      config = self.config).loop()

    for kind in ["AlfaTemplate"]:
      yield Producer(
        url = self.config[kind]["url"],
        resource_version = None,
        session = self.session,
        queue = self.queue,
        config = self.config).loop()
    
  def __del__(self):
    if hasattr(self, "logger"):
      self.logger.info(f'__del__ starting')
    if hasattr(self, "task") and self.task is not None:
      self.task.cancel()
    if hasattr(self, "logger"):
      self.logger.info(f'__del__ completed')

class AlfaControllerConsumer:
  def __init__(self, config, session = None, queue = None, logger = None):
    self.config = config
    self.session = ClientSession() if session is None else session
    self.queue = Queue() if queue is None else queue
    self.logger = Log.get_logger(f'{__name__}()', self.config.log_level) if logger is None else logger
    self.controllers = {}

  async def loop(self):
    while True:
      self.logger.info(f'Sleeping until next event')
      queued = await self.queue.get()
      await self.consume_event(queued['event'])

  async def consume_event(self, event):
    self.logger.debug(f'Received event {dumps(event)}')
    if not self.config.app_filter.match(event["object"]["metadata"].get("labels",{}).get("app.kubernetes.io/name","")):
      self.logger.info(f'Ignoring {event["object"]["metadata"]["name"]} {event["type"].lower()} (resourceVersion {event["object"]["metadata"]["resourceVersion"]}) - Filtered by app filter {self.config.app_filter}')
      return

    if not self.config.template_filter.match(event["object"]["metadata"]["name"]):
      self.logger.info(f'Ignoring {event["object"]["metadata"]["name"]} {event["type"].lower()} (resourceVersion {event["object"]["metadata"]["resourceVersion"]}) - Filtered by template filter {self.config.template_filter}')
      return

    self.logger.info(f'Processing {event["object"]["metadata"]["name"]} {event["type"].lower()} (resourceVersion {event["object"]["metadata"]["resourceVersion"]})')
    
    if event["object"]["metadata"]["name"] in self.controllers.keys():
      self.logger.info(f'stopping existing AlfaTemplate({event["object"]["metadata"]["name"]})')
      self.controllers.pop(event["object"]["metadata"]["name"])

    if event["type"].lower() == "added" or event["type"].lower() == "modified":
      self.logger.info(f'creating new AlfaTemplate({event["object"]["metadata"]["name"]})')
      controller = AlfaTemplate(event["object"], session = self.session, config=self.config)
      get_event_loop().create_task(controller.loop())
      self.controllers[event["object"]["metadata"]["name"]] = controller

    self.logger.info(f'{len(self.controllers)} AlfaTemplate object(s) in memory {self.controllers.keys()}')