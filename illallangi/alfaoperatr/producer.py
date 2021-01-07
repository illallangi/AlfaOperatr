import json
from asyncio import Queue, sleep
from asyncio.exceptions import TimeoutError

from aiohttp import ClientSession

from illallangi.k8sapi import API as K8S_API

from yarl import URL

from .config import Config
from .log import Log


class Producer:
    def __init__(
        self,
        kind,
        config,
        api,
        session=None,
        resource_version=None,
        queue=None,
        logger=None,
    ):
        if not isinstance(config, Config):
            raise TypeError("Expected Config; got %s" % type(config).__name__)
        if not isinstance(api, K8S_API):
            raise TypeError("Expected API; got %s" % type(api).__name__)

        self.kind = kind
        self.config = config
        self.api = api
        self.resource_version = resource_version
        self.session = ClientSession() if session is None else session
        self.queue = Queue() if queue is None else queue
        self.logger = (
            Log.get_logger(f"Producer({self.kind})", self.config.log_level)
            if logger is None
            else logger
        )

    async def loop(self):
        while True:
            try:
                params = {"watch": 1}
                if self.resource_version is not None:
                    params["resourceVersion"] = self.resource_version
                async with self.session.request(
                    "get", self.api.kinds[self.kind].rest_path.with_query(**params)
                ) as response:
                    self.logger.info(f"Connected, {URL(response.url).query_string}")
                    async for line in response.content:
                        if line:
                            try:
                                event = json.loads(line)
                            except json.decoder.JSONDecodeError as e:
                                self.logger.error(
                                    f'Encountered JSONDecodeError "{repr(e)}" on "{line}", continuing.'
                                )
                                continue
                            await self.handle_event(event)
                            if int(event["object"]["metadata"]["resourceVersion"]) > (
                                self.resource_version or 0
                            ):
                                self.resource_version = int(
                                    event["object"]["metadata"]["resourceVersion"]
                                )
                        await sleep(0)
            except TimeoutError:
                self.logger.info(
                    f"Timed out, restarting at resourceVersion {self.resource_version}"
                )

    async def handle_event(self, event):
        if "name" not in event["object"]["metadata"].keys():
            self.logger.warn(
                f"Ignoring event with no object.metadata.name: {json.dumps(event)}"
            )
            return
        if event["object"]["metadata"]["name"] in [
            "cert-manager-controller",
            "cert-manager-cainjector-leader-election-core",
            "cert-manager-cainjector-leader-election",
        ]:
            self.logger.debug(
                f'Ignoring {event["object"]["metadata"]["name"]} {event["type"].lower()} (resourceVersion {event["object"]["metadata"]["resourceVersion"]})'
            )
            return
        self.logger.info(
            f'Handling {event["object"]["metadata"]["name"]} {event["type"].lower()} (resourceVersion {event["object"]["metadata"]["resourceVersion"]})'
        )
        self.logger.debug(f"Received event {json.dumps(event)}")

        await self.queue.put({"event": event})
