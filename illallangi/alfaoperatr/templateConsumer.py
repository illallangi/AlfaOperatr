import json
import os
from asyncio import Queue, sleep

from aiohttp import ClientSession

from illallangi.k8sapi import API as K8S_API

from loguru import logger

import yaml

from yarl import URL

from .functions import recursive_get
from .templateRenderer import TemplateRenderer

COOLDOWN = 5


class TemplateConsumer:
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
        while True:
            logger.debug("Sleeping until next event")
            await self.queue.get()
            logger.info(f"Consumer awaiting cooldown for {COOLDOWN} seconds")
            await sleep(COOLDOWN)
            while not self.queue.empty():
                self.queue.get_nowait()
            await self.consume()

    async def consume(self):
        for render in await TemplateRenderer(
            api=self.api,
            dump=self.dump,
            name=recursive_get(self.alfa_template, "metadata.name"),
            session=self.session,
        ).render():
            if render is None or "kind" not in render:
                continue
            try:
                url = URL(
                    self.api.kinds[render["kind"]].calculate_url(
                        render["metadata"].get("namespace", None),
                        render["metadata"]["name"],
                    )
                )
                logger.info(f'Getting {render["kind"]} {url}')
                async with self.session.request("get", url) as item_get_response:
                    if item_get_response.status in [404]:
                        url = url.parent
                        try:
                            logger.info(f'Creating {render["kind"]} {url}')
                            logger.debug(f"HTTP POST {url}: {json.dumps(render)}")
                            async with self.session.request(
                                "post", url, json=render
                            ) as item_post_response:
                                if item_post_response.headers.get(
                                    "content-type"
                                ) not in [
                                    "application/json"
                                ] or item_post_response.status in [
                                    404
                                ]:
                                    item_post = await item_post_response.text()
                                    logger.error(
                                        f"HTTP POST {url} {item_post_response.status} {item_post}"
                                    )
                                    continue
                                item_post = await item_post_response.json()
                                if (
                                    item_post["kind"] == "Status"
                                    and item_post["status"] == "Failure"
                                ):
                                    logger.error(
                                        f'HTTP POST {url} failed: {item_post["message"]} {json.dumps(item_post)}'
                                    )
                                    continue
                                logger.debug(
                                    f"HTTP POST {url} {item_post_response.status} {json.dumps(item_post)}"
                                )
                                if self.dump:
                                    with open(
                                        os.path.join(
                                            self.dump,
                                            f'{item_post["metadata"].get("namespace","cluster")}-{item_post["metadata"]["name"]}-{item_post["kind"]}-{item_post["metadata"]["resourceVersion"]}.yaml',
                                        ),
                                        "w",
                                    ) as outfile:
                                        yaml.dump(item_post, outfile)
                                logger.info(
                                    f'Created {render["kind"]} {url / render["metadata"]["name"]}, resourceVersion {item_post["metadata"]["resourceVersion"]}'
                                )
                        except Exception as e:
                            logger.error(f'Error Creating {render["kind"]}: {repr(e)}')
                            continue

                    else:
                        item_get = await item_get_response.json()
                        if self.dump:
                            with open(
                                os.path.join(
                                    self.dump,
                                    f'{item_get["metadata"].get("namespace","cluster")}-{item_get["metadata"]["name"]}-{item_get["kind"]}-{item_get["metadata"]["resourceVersion"]}.yaml',
                                ),
                                "w",
                            ) as outfile:
                                yaml.dump(item_get, outfile)
                        render["metadata"]["resourceVersion"] = item_get["metadata"][
                            "resourceVersion"
                        ]

                        if not recursive_get(self.alfa_template, "spec.update"):
                            logger.info(
                                f'Not updating as update set to false {render["kind"]} {url}'
                            )
                            continue

                        if "PersistentVolumeClaim" == item_get.get("kind"):
                            logger.info(
                                f'Not updating as PersistentVolumeClaim is immutable after creation {render["kind"]} {url}'
                            )
                            continue

                        # Ugly hack to avoid race condition
                        if "deployment.kubernetes.io/revision" in item_get.get(
                            "metadata", {}
                        ).get("annotations", {}):
                            if "metadata" not in render:
                                render["metadata"] = {}
                            if "annotations" not in render["metadata"]:
                                render["metadata"]["annotations"] = {}
                            render["metadata"]["annotations"][
                                "deployment.kubernetes.io/revision"
                            ] = item_get["metadata"]["annotations"][
                                "deployment.kubernetes.io/revision"
                            ]

                        # Ugly hack to avoid race condition
                        if "clusterIP" in item_get.get("spec", {}):
                            if "spec" not in render:
                                render["spec"] = {}
                            render["spec"]["clusterIP"] = item_get["spec"]["clusterIP"]

                        # Ugly hack to avoid cannot change healthCheckNodePort on loadBalancer service with externalTraffic=Local during update error
                        if (
                            "Service" == item_get.get("kind")
                            and "externalTrafficPolicy" in item_get.get("spec", {})
                            and "Local"
                            == item_get.get("spec", {}).get("externalTrafficPolicy")
                        ):
                            render["spec"]["healthCheckNodePort"] = item_get["spec"][
                                "healthCheckNodePort"
                            ]

                        try:
                            logger.info(f'Updating {render["kind"]} {url}')
                            logger.debug(f"HTTP PUT {url}: {json.dumps(render)}")
                            async with self.session.request(
                                "put", url, json=render
                            ) as item_put_response:
                                if item_put_response.headers.get(
                                    "content-type"
                                ) not in [
                                    "application/json"
                                ] or item_put_response.status in [
                                    404
                                ]:
                                    item_post = await item_put_response.text()
                                    logger.error(
                                        f"HTTP PUT {url} {item_put_response.status} {item_post}"
                                    )
                                    continue
                                item_put = await item_put_response.json()
                                if (
                                    item_put["kind"] == "Status"
                                    and item_put["status"] == "Failure"
                                ):
                                    logger.error(
                                        f'HTTP PUT {url} failed: {item_put["message"]} {json.dumps(item_put)}'
                                    )
                                    continue
                                if (
                                    "resourceVersion" not in item_put["metadata"]
                                    or "resourceVersion" not in item_get["metadata"]
                                    or item_put["metadata"].get("resourceVersion", None)
                                    != item_get["metadata"].get("resourceVersion", None)
                                ):
                                    if self.dump:
                                        with open(
                                            os.path.join(
                                                self.dump,
                                                f'{item_put["metadata"].get("namespace","cluster")}-{item_put["metadata"]["name"]}-{item_put["kind"]}-{item_put["metadata"]["resourceVersion"]}.yaml',
                                            ),
                                            "w",
                                        ) as outfile:
                                            yaml.dump(item_put, outfile)
                                    logger.info(
                                        f'Updated {render["kind"]} {render["metadata"].get("namespace","cluster")}\\{render["metadata"]["name"]}, from resourceVersion {item_get["metadata"]["resourceVersion"]} to {item_put["metadata"]["resourceVersion"]}'
                                    )
                                else:
                                    logger.info(
                                        f'No change to {render["kind"]} {render["metadata"].get("namespace","cluster")}\\{render["metadata"]["name"]}, resourceVersion still {item_get["metadata"]["resourceVersion"]}'
                                    )
                        except Exception as e:
                            logger.error(f'Error Updating {render["kind"]}: {repr(e)}')
                            continue
            except Exception as e:
                logger.error(f"Error Getting Render: {repr(e)}")
                continue
