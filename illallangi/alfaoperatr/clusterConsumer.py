from asyncio import Queue, get_event_loop
from json import dumps

from aiohttp import ClientSession

from illallangi.k8sapi import API as K8S_API

from loguru import logger

from .config import Config
from .templateController import TemplateController


class ClusterConsumer:
    def __init__(self, config, api, session=None, queue=None):
        if not isinstance(config, Config):
            raise TypeError("Expected Config; got %s" % type(config).__name__)
        if not isinstance(api, K8S_API):
            raise TypeError("Expected API; got %s" % type(api).__name__)

        self.config = config
        self.api = api
        self.session = ClientSession() if session is None else session
        self.queue = Queue() if queue is None else queue
        self.controllers = {}

    async def loop(self):
        while True:
            logger.info(
                f'{len(self.controllers)} TemplateController(s) in memory ({", ".join(self.controllers.keys())})'
            )
            logger.debug("Sleeping until next event")
            queued = await self.queue.get()
            await self.consume(queued["event"])

    async def consume(self, event):
        logger.debug(f"Received event {dumps(event)}")
        if not self.config.parent == event["object"]["spec"]["kinds"]["parent"]["kind"]:
            logger.info(
                f'Ignoring {event["object"]["metadata"]["name"]} {event["type"].lower()} (resourceVersion {event["object"]["metadata"]["resourceVersion"]}) - Not a template for {self.config.parent}'
            )
            return

        logger.info(
            f'Processing {event["object"]["metadata"]["name"]} {event["type"].lower()} (resourceVersion {event["object"]["metadata"]["resourceVersion"]})'
        )

        if event["object"]["metadata"]["name"] in self.controllers.keys():
            logger.info(
                f'stopping existing TemplateController({event["object"]["metadata"]["name"]})'
            )
            self.controllers.pop(event["object"]["metadata"]["name"])

        if event["type"].lower() == "added" or event["type"].lower() == "modified":
            logger.info(
                f'creating new TemplateController({event["object"]["metadata"]["name"]})'
            )
            controller = TemplateController(
                event["object"], session=self.session, config=self.config, api=self.api
            )
            get_event_loop().create_task(controller.loop())
            self.controllers[event["object"]["metadata"]["name"]] = controller
