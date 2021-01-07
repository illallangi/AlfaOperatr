from asyncio import Queue, gather

from aiohttp import ClientSession

from illallangi.k8sapi import API as K8S_API

from .clusterConsumer import ClusterConsumer
from .config import Config
from .log import Log
from .producer import Producer


class ClusterController:
    def __init__(self, config, api, queue=None, session=None, logger=None):
        if not isinstance(config, Config):
            raise TypeError("Expected Config; got %s" % type(config).__name__)
        if not isinstance(api, K8S_API):
            raise TypeError("Expected API; got %s" % type(api).__name__)

        self.config = config
        self.api = api
        self.session = ClientSession() if session is None else session
        self.queue = Queue() if queue is None else queue
        self.logger = (
            Log.get_logger("ClusterController()", self.config.log_level)
            if logger is None
            else logger
        )

    async def loop(self):
        self.logger.info("loop starting")
        self.task = gather(*self.get_coroutines())
        await self.task
        self.logger.info("loop completed")

    def get_coroutines(self):
        yield ClusterConsumer(
            session=self.session, queue=self.queue, config=self.config, api=self.api
        ).loop()

        for kind in ["AlfaTemplate"]:
            yield Producer(
                kind=kind,
                resource_version=None,
                session=self.session,
                queue=self.queue,
                config=self.config,
                api=self.api,
            ).loop()

    def __del__(self):
        if hasattr(self, "logger"):
            self.logger.info("__del__ starting")
        if hasattr(self, "task") and self.task is not None:
            self.task.cancel()
        if hasattr(self, "logger"):
            self.logger.info("__del__ completed")
