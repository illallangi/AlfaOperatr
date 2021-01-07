from asyncio import Queue, get_event_loop
from json import dumps

from aiohttp import ClientSession

from illallangi.k8sapi import API as K8S_API

from .config import Config
from .log import Log
from .templateController import TemplateController


class ClusterConsumer:
    def __init__(self, config, api, session=None, queue=None, logger=None):
        if not isinstance(config, Config):
            raise TypeError("Expected Config; got %s" % type(config).__name__)
        if not isinstance(api, K8S_API):
            raise TypeError("Expected API; got %s" % type(api).__name__)

        self.config = config
        self.api = api
        self.session = ClientSession() if session is None else session
        self.queue = Queue() if queue is None else queue
        self.logger = (
            Log.get_logger("ClusterConsumer()", self.config.log_level)
            if logger is None
            else logger
        )
        self.controllers = {}

    async def loop(self):
        while True:
            self.logger.info(
                f'{len(self.controllers)} TemplateController(s) in memory ({", ".join(self.controllers.keys())})'
            )
            self.logger.debug("Sleeping until next event")
            queued = await self.queue.get()
            await self.consume(queued["event"])

    async def consume(self, event):
        self.logger.debug(f"Received event {dumps(event)}")
        if not self.config.app_filter.match(
            event["object"]["metadata"]
            .get("labels", {})
            .get("app.kubernetes.io/name", "")
        ):
            self.logger.info(
                f'Ignoring {event["object"]["metadata"]["name"]} {event["type"].lower()} (resourceVersion {event["object"]["metadata"]["resourceVersion"]}) - Filtered by app filter {self.config.app_filter}'
            )
            return

        if not self.config.parent == event["object"]["spec"]["kinds"]["parent"]["kind"]:
            self.logger.info(
                f'Ignoring {event["object"]["metadata"]["name"]} {event["type"].lower()} (resourceVersion {event["object"]["metadata"]["resourceVersion"]}) - Not a template for {self.config.parent}'
            )
            return

        if not self.config.template_filter.match(event["object"]["metadata"]["name"]):
            self.logger.info(
                f'Ignoring {event["object"]["metadata"]["name"]} {event["type"].lower()} (resourceVersion {event["object"]["metadata"]["resourceVersion"]}) - Filtered by template filter {self.config.template_filter}'
            )
            return

        self.logger.info(
            f'Processing {event["object"]["metadata"]["name"]} {event["type"].lower()} (resourceVersion {event["object"]["metadata"]["resourceVersion"]})'
        )

        if event["object"]["metadata"]["name"] in self.controllers.keys():
            self.logger.info(
                f'stopping existing TemplateController({event["object"]["metadata"]["name"]})'
            )
            self.controllers.pop(event["object"]["metadata"]["name"])

        if event["type"].lower() == "added" or event["type"].lower() == "modified":
            self.logger.info(
                f'creating new TemplateController({event["object"]["metadata"]["name"]})'
            )
            controller = TemplateController(
                event["object"], session=self.session, config=self.config, api=self.api
            )
            get_event_loop().create_task(controller.loop())
            self.controllers[event["object"]["metadata"]["name"]] = controller
