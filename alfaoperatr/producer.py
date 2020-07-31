from yarl import URL
from aiohttp import ClientSession
from asyncio import Queue, sleep
from asyncio.exceptions import TimeoutError
import json

from .log import AlfaLog


class AlfaProducer:
    def __init__(self, kind, config, session = None, resource_version = None, queue = None, logger = None):
        self.kind = kind
        self.config = config
        self.resource_version = resource_version
        self.session = ClientSession() if session is None else session
        self.queue = Queue() if queue is None else queue
        self.logger = AlfaLog.get_logger(f'AlfaProducer({self.kind})', self.config.log_level) if logger is None else logger

    async def loop(self):
        while True:
            try:
                params = {'watch': 1}
                if self.resource_version != None:
                    params['resourceVersion'] = self.resource_version
                async with self.session.request('get', self.config[self.kind]["url"].with_query(**params)) as response:
                    self.logger.info(f'Connected, {URL(response.url).query_string}')
                    async for line in response.content:
                        if line:
                            try:
                                event = json.loads(line)
                            except json.decoder.JSONDecodeError as e:
                                self.logger.error(f'Encountered JSONDecodeError "{repr(e)}" on "{line}", continuing.')
                                continue
                            await self.handle_event(event)
                            if int(event["object"]["metadata"]["resourceVersion"]) > (self.resource_version or 0):
                                self.resource_version = int(event["object"]["metadata"]["resourceVersion"])
                        await sleep(0)
            except TimeoutError as e:
                self.logger.info(f'Timed out, restarting at resourceVersion {self.resource_version}')

    async def handle_event(self, event):
        if "name" not in event["object"]["metadata"].keys():
            self.logger.warn(f'Ignoring event with no object.metadata.name: {json.dumps(event)}')
            return
        if event["object"]["metadata"]["name"] in ["cert-manager-controller", "cert-manager-cainjector-leader-election-core", "cert-manager-cainjector-leader-election"]:
            self.logger.debug(f'Ignoring {event["object"]["metadata"]["name"]} {event["type"].lower()} (resourceVersion {event["object"]["metadata"]["resourceVersion"]})')
            return
        self.logger.info(f'Handling {event["object"]["metadata"]["name"]} {event["type"].lower()} (resourceVersion {event["object"]["metadata"]["resourceVersion"]})')
        self.logger.debug(f'Received event {json.dumps(event)}')
            
        await self.queue.put({'event': event})
