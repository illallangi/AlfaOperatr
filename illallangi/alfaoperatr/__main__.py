from asyncio import ensure_future, get_event_loop
from sys import stderr

from click import Choice as CHOICE, Path as PATH, STRING, argument, command, option

from illallangi.k8sapi import API as K8S_API

from loguru import logger

from notifiers.logging import NotificationHandler

from .clusterController import ClusterController
from .config import Config


@command()
@option(
    "--log-level",
    type=CHOICE(
        ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "SUCCESS", "TRACE"],
        case_sensitive=False,
    ),
    default="INFO",
    envvar="LOGLEVEL",
)
@option("--slack-username", type=STRING, envvar="SLACK_USERNAME", default=__name__)
@option("--slack-webhook", type=STRING, envvar="SLACK_WEBHOOK", default=None)
@option("--slack-format", type=STRING, envvar="SLACK_FORMAT", default="{message}")
@argument("parent", required=True)
@option(
    "--debug-path",
    default=None,
    show_default=False,
    type=PATH(
        exists=False,
        file_okay=False,
        dir_okay=True,
        writable=True,
        readable=True,
        resolve_path=True,
        allow_dash=False,
    ),
)
@option(
    "--api-proxy",
    default="http://localhost:8001",
    show_default=False,
    type=STRING,
    envvar="PROXY",
)
def cli(
    log_level,
    slack_username,
    slack_webhook,
    slack_format,
    parent,
    debug_path,
    api_proxy,
):
    logger.remove()
    logger.add(stderr, level=log_level)
    if slack_webhook:
        params = {"username": slack_username, "webhook_url": slack_webhook}
        slack = NotificationHandler("slack", defaults=params)
        logger.add(slack, format=slack_format, level="SUCCESS")

    config = Config(
        parent=parent,
        debug_path=debug_path,
    )

    api = K8S_API(api_proxy)

    controller = ClusterController(config, api)

    get_event_loop().run_until_complete(ensure_future(controller.loop()))


if __name__ == "__main__":
    cli()
