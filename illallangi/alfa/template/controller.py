from asyncio import Queue, gather, get_event_loop

from aiohttp import ClientSession

from illallangi.alfa.functions import recursive_get
from illallangi.k8sapi import API as K8S_API

from loguru import logger

from yarl import URL

from .consumer import Consumer
from .producer import Producer


class Controller:
    def __init__(self, api, dump, alfa_template, session=None, queue=None):
        self.api = (
            K8S_API(URL(api) if not isinstance(api, URL) else api)
            if not isinstance(api, K8S_API)
            else api
        )
        self.dump = dump
        self.alfa_template = alfa_template
        self.session = ClientSession() if session is None else session
        if not isinstance(self.session, ClientSession):
            raise TypeError(
                "Expected ClientSession; got %s" % type(self.session).__name__
            )
        self.queue = Queue() if queue is None else queue
        if not isinstance(self.queue, Queue):
            raise TypeError("Expected Queue; got %s" % type(self.queue).__name__)

    async def loop(self):
        with logger.contextualize(
            template=recursive_get(self.alfa_template, "metadata.name"),
        ):
            logger.debug("loop starting")
            self.task = gather(*self.get_coroutines())
            await self.task
            logger.debug("loop completed")

    def get_coroutines(self):
        yield Consumer(
            api=self.api,
            dump=self.dump,
            alfa_template=self.alfa_template,
            session=self.session,
            queue=self.queue,
        ).loop()

        for kind in [
            *[recursive_get(self.alfa_template, "spec.kinds.parent.kind")],
            *[
                kind["kind"]
                for kind in recursive_get(self.alfa_template, "spec.kinds.monitored")
            ],
        ]:
            yield Producer(
                api=self.api,
                kind=kind,
                session=self.session,
                queue=self.queue,
            ).loop()

    def cancel(self):
        if (
            not get_event_loop().is_closed()
            and hasattr(self, "task")
            and self.task is not None
        ):
            logger.info("cancelling task")
            self.task.cancel()

    def __del__(self):
        logger.info("__del__ starting")
        logger.info("__del__ completed")
