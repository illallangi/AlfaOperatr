from asyncio import Queue, get_event_loop, sleep
from json import dumps

from aiohttp import ClientSession

from illallangi.alfa.template import Controller as TemplateController
from illallangi.k8sapi import API as K8S_API

from loguru import logger

from yarl import URL


class Consumer:
    def __init__(self, api, dump, parent, session=None, queue=None):
        self.api = (
            K8S_API(URL(api) if not isinstance(api, URL) else api)
            if not isinstance(api, K8S_API)
            else api
        )
        self.dump = dump
        self.parent = parent
        self.session = ClientSession() if session is None else session
        if not isinstance(self.session, ClientSession):
            raise TypeError(
                "Expected ClientSession; got %s" % type(self.session).__name__
            )
        self.queue = Queue() if queue is None else queue
        if not isinstance(self.queue, Queue):
            raise TypeError("Expected Queue; got %s" % type(self.queue).__name__)
        self.controllers = {}

    async def loop(self):
        while True:
            logger.debug("Sleeping until next event")
            queued = await self.queue.get()
            await self.consume(queued["event"])

    async def consume(self, event):
        logger.trace(f"Received event {dumps(event)}")
        if not self.parent == event["object"]["spec"]["kinds"]["parent"]["kind"]:
            logger.debug(
                f'Ignoring {event["object"]["metadata"]["name"]} {event["type"].lower()} (resourceVersion {event["object"]["metadata"]["resourceVersion"]}) - Not a template for {self.parent}'
            )
            return

        logger.debug(
            f'Processing {event["object"]["metadata"]["name"]} {event["type"].lower()} (resourceVersion {event["object"]["metadata"]["resourceVersion"]})'
        )

        if event["object"]["metadata"]["name"] in self.controllers.keys():
            logger.info(f'stopping {event["object"]["metadata"]["name"]} controller')
            self.controllers.pop(event["object"]["metadata"]["name"]).cancel()
            await sleep(0)

        if event["type"].lower() == "added" or event["type"].lower() == "modified":
            logger.info(f'creating {event["object"]["metadata"]["name"]} controller')
            controller = TemplateController(
                api=self.api,
                dump=self.dump,
                alfa_template=event["object"],
                session=self.session,
            )
            get_event_loop().create_task(controller.loop())
            self.controllers[event["object"]["metadata"]["name"]] = controller
