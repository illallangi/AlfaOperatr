from asyncio import Queue, gather, get_event_loop

from aiohttp import ClientSession

from illallangi.k8sapi import API as K8S_API

from loguru import logger

from .config import Config
from .functions import recursive_get
from .producer import Producer
from .templateConsumer import TemplateConsumer


class TemplateController:
    def __init__(self, alfa_template, config, api, queue=None, session=None):
        if not isinstance(config, Config):
            raise TypeError("Expected Config; got %s" % type(config).__name__)
        if not isinstance(api, K8S_API):
            raise TypeError("Expected API; got %s" % type(api).__name__)

        self.alfa_template = alfa_template
        self.config = config
        self.api = api
        self.session = ClientSession()  # if session is None else session
        self.queue = Queue() if queue is None else queue

    async def loop(self):
        logger.info("loop starting")
        self.task = gather(*self.get_coroutines())
        await self.task
        logger.info("loop completed")

    def get_coroutines(self):
        yield TemplateConsumer(
            self.alfa_template,
            session=self.session,
            queue=self.queue,
            config=self.config,
            api=self.api,
        ).loop()

        for kind in [
            *[recursive_get(self.alfa_template, "spec.kinds.parent.kind")],
            *[
                kind["kind"]
                for kind in recursive_get(self.alfa_template, "spec.kinds.monitored")
            ],
        ]:
            yield Producer(
                kind=kind,
                resource_version=None,
                session=self.session,
                queue=self.queue,
                config=self.config,
                api=self.api,
            ).loop()

    def __del__(self):
        logger.info("__del__ starting")
        if not get_event_loop().is_closed() and self.task is not None:
            self.task.cancel()
        logger.info("__del__ completed")
