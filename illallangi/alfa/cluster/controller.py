from asyncio import Queue, gather

from aiohttp import ClientSession

from illallangi.alfa import Producer
from illallangi.k8sapi import API as K8S_API

from loguru import logger

from yarl import URL

from .consumer import Consumer


class Controller:
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

    async def loop(self):
        logger.info("loop starting")
        self.task = gather(*self.get_coroutines())
        await self.task
        logger.info("loop completed")

    def get_coroutines(self):
        yield Consumer(
            api=self.api,
            dump=self.dump,
            parent=self.parent,
            session=self.session,
            queue=self.queue,
        ).loop()

        for kind in ["AlfaTemplate"]:
            yield Producer(
                api=self.api,
                kind=kind,
                resource_version=None,
                session=self.session,
                queue=self.queue,
            ).loop()

    def __del__(self):
        if hasattr(self, "logger"):
            logger.info("__del__ starting")
        if hasattr(self, "task") and self.task is not None:
            self.task.cancel()
        if hasattr(self, "logger"):
            logger.info("__del__ completed")
