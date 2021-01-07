import json
from asyncio import Queue, sleep
from asyncio.exceptions import TimeoutError

from aiohttp import ClientSession

from illallangi.k8sapi import API as K8S_API

from loguru import logger

from yarl import URL


class Producer:
    def __init__(
        self,
        api,
        kind,
        resource_version=None,
        session=None,
        queue=None,
    ):
        self.api = (
            K8S_API(URL(api) if not isinstance(api, URL) else api)
            if not isinstance(api, K8S_API)
            else api
        )
        self.kind = kind
        self.resource_version = resource_version or 0
        self.session = ClientSession() if session is None else session
        if not isinstance(self.session, ClientSession):
            raise TypeError(
                "Expected ClientSession; got %s" % type(self.session).__name__
            )
        self.queue = Queue() if queue is None else queue
        if not isinstance(self.queue, Queue):
            raise TypeError("Expected Queue; got %s" % type(self.queue).__name__)

    async def loop(self):
        while True:
            try:
                params = {"watch": 1}
                if self.resource_version > 0:
                    params["resourceVersion"] = self.resource_version
                async with self.session.request(
                    "get", self.api.kinds[self.kind].rest_path.with_query(**params)
                ) as response:
                    logger.info(f"Connected, {URL(response.url).query_string}")
                    async for line in response.content:
                        if line:
                            try:
                                event = json.loads(line)
                            except json.decoder.JSONDecodeError as e:
                                logger.error(
                                    f'Encountered JSONDecodeError "{repr(e)}" on "{line}", continuing.'
                                )
                                continue
                            await self.handle_event(event)
                            if (
                                int(event["object"]["metadata"]["resourceVersion"])
                                > self.resource_version
                            ):
                                self.resource_version = int(
                                    event["object"]["metadata"]["resourceVersion"]
                                )
                        await sleep(0)
            except TimeoutError:
                logger.info(
                    f"Timed out, restarting at resourceVersion {self.resource_version}"
                )

    async def handle_event(self, event):
        if "name" not in event["object"]["metadata"].keys():
            logger.warning(
                f"Ignoring event with no object.metadata.name: {json.dumps(event)}"
            )
            return
        if event["object"]["metadata"]["name"] in [
            "cert-manager-controller",
            "cert-manager-cainjector-leader-election-core",
            "cert-manager-cainjector-leader-election",
        ]:
            logger.debug(
                f'Ignoring {event["object"]["metadata"]["name"]} {event["type"].lower()} (resourceVersion {event["object"]["metadata"]["resourceVersion"]})'
            )
            return
        logger.info(
            f'Handling {event["object"]["metadata"]["name"]} {event["type"].lower()} (resourceVersion {event["object"]["metadata"]["resourceVersion"]})'
        )
        logger.debug(f"Received event {json.dumps(event)}")

        await self.queue.put({"event": event})
