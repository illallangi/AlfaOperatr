from asyncio import Queue, gather, get_event_loop

from aiohttp import ClientSession

from .functions import recursive_get
from .log import Log
from .producer import Producer
from .templateConsumer import TemplateConsumer


class TemplateController:
    def __init__(self, alfa_template, config, queue=None, session=None, logger=None):
        self.alfa_template = alfa_template
        self.config = config
        self.session = ClientSession()  # if session is None else session
        self.queue = Queue() if queue is None else queue
        self.logger = Log.get_logger(f'TemplateController({alfa_template["metadata"]["name"]})', self.config.log_level) if logger is None else logger

    async def loop(self):
        self.logger.info('loop starting')
        self.task = gather(*self.get_coroutines())
        await self.task
        self.logger.info('loop completed')

    def get_coroutines(self):
        yield TemplateConsumer(
            self.alfa_template,
            session=self.session,
            queue=self.queue,
            config=self.config).loop()

        for kind in [
            *[recursive_get(self.alfa_template, 'spec.kinds.parent.kind')],
            *[kind['kind'] for kind in recursive_get(self.alfa_template, 'spec.kinds.monitored')]
        ]:
            yield Producer(
                kind=kind,
                resource_version=None,
                session=self.session,
                queue=self.queue,
                config=self.config).loop()

    def __del__(self):
        self.logger.info('__del__ starting')
        if not get_event_loop().is_closed() and self.task is not None:
            self.task.cancel()
        self.logger.info('__del__ completed')
