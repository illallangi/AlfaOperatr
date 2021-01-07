import json
from asyncio import Queue, sleep
from asyncio.exceptions import TimeoutError

from aiohttp import ClientSession

from illallangi.k8sapi import API as K8S_API

from loguru import logger

from yarl import URL

TIMEOUT = None


class Producer:
    def __init__(
        self,
        api,
        kind,
        session=None,
        queue=None,
    ):
        self.api = (
            K8S_API(URL(api) if not isinstance(api, URL) else api)
            if not isinstance(api, K8S_API)
            else api
        )
        self.kind = kind
        self.session = ClientSession() if session is None else session
        if not isinstance(self.session, ClientSession):
            raise TypeError(
                "Expected ClientSession; got %s" % type(self.session).__name__
            )
        self.queue = Queue() if queue is None else queue
        if not isinstance(self.queue, Queue):
            raise TypeError("Expected Queue; got %s" % type(self.queue).__name__)

    async def loop(self):
        with logger.contextualize():
            logger.debug("starting loop")
            resource_version = 0
            while True:
                try:
                    params = {"watch": 1}
                    if resource_version > 0:
                        params["resourceVersion"] = resource_version
                    async with self.session.request(
                        "get",
                        self.api.kinds[self.kind].rest_path.with_query(**params),
                        timeout=TIMEOUT,
                    ) as response:
                        logger.info(f"connected to {response.url}")
                        async for line in response.content:
                            if line:
                                try:
                                    event = json.loads(line)
                                except json.decoder.JSONDecodeError as e:
                                    logger.error(
                                        f'JSONDecodeError "{repr(e)}" on "{line}", continuing.'
                                    )
                                    continue
                                if (
                                    event["type"] == "ERROR"
                                    and event["object"]["reason"] == "Expired"
                                ):
                                    resource_version = 0
                                    logger.warning(
                                        f"connection expired, restarting at resourceVersion {resource_version}"
                                    )
                                    break
                                await self.handle_event(event)
                                if (
                                    int(event["object"]["metadata"]["resourceVersion"])
                                    > resource_version
                                ):
                                    resource_version = int(
                                        event["object"]["metadata"]["resourceVersion"]
                                    )
                            await sleep(0)
                        await sleep(0)
                    await sleep(0)
                except TimeoutError:
                    logger.warning(
                        f"timed out, restarting at resourceVersion {resource_version}"
                    )
            logger.debug("completed loop")

    async def handle_event(self, event):
        logger.trace(f"{json.dumps(event)}")
        if "name" not in event["object"]["metadata"].keys():
            logger.debug("ignoring event with no object.metadata.name")
            return
        if event["object"]["metadata"]["name"] in [
            "cert-manager-controller",
            "cert-manager-cainjector-leader-election-core",
            "cert-manager-cainjector-leader-election",
        ]:
            logger.debug(
                f'ignoring {event["object"]["metadata"]["name"]} {event["type"].lower()} (resourceVersion {event["object"]["metadata"]["resourceVersion"]})'
            )
            return
        logger.debug(
            f'handling {event["object"]["metadata"]["name"]} {event["type"].lower()} (resourceVersion {event["object"]["metadata"]["resourceVersion"]})'
        )

        await self.queue.put({"event": event})
