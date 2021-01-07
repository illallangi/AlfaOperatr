from asyncio import Queue, gather

from aiohttp import ClientSession

from .clusterConsumer import ClusterConsumer
from .log import Log
from .producer import Producer


class ClusterController:
    def __init__(self, config, queue=None, session=None, logger=None):
        self.config = config
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
            session=self.session, queue=self.queue, config=self.config
        ).loop()

        for kind in ["AlfaTemplate"]:
            yield Producer(
                kind=kind,
                resource_version=None,
                session=self.session,
                queue=self.queue,
                config=self.config,
            ).loop()

    def __del__(self):
        if hasattr(self, "logger"):
            self.logger.info("__del__ starting")
        if hasattr(self, "task") and self.task is not None:
            self.task.cancel()
        if hasattr(self, "logger"):
            self.logger.info("__del__ completed")
