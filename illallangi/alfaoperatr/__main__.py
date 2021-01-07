from asyncio import ensure_future, get_event_loop
from sys import stderr

from click import Choice as CHOICE, Path as PATH, STRING, command, option

from illallangi.alfa.cluster import Controller

from loguru import logger

from notifiers.logging import NotificationHandler


@command()
@option(
    "--log-level",
    type=CHOICE(
        ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "SUCCESS", "TRACE"],
        case_sensitive=False,
    ),
    default="INFO",
    envvar="LOG_LEVEL",
)
@option("--slack-username", type=STRING, envvar="SLACK_USERNAME", default=__name__)
@option("--slack-webhook", type=STRING, envvar="SLACK_WEBHOOK", default=None)
@option("--slack-format", type=STRING, envvar="SLACK_FORMAT", default="{message}")
@option(
    "--parent",
    required=True,
    envvar="ALFA_PARENT",
)
@option(
    "--dump",
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
    envvar="ALFA_DUMP",
)
@option(
    "--api",
    default="http://localhost:8001",
    show_default=False,
    type=STRING,
    envvar="ALFA_API",
)
def cli(
    log_level,
    slack_username,
    slack_webhook,
    slack_format,
    api,
    dump,
    parent,
):
    logger.remove()
    logger.add(stderr, level=log_level)
    if slack_webhook:
        params = {"username": slack_username, "webhook_url": slack_webhook}
        slack = NotificationHandler("slack", defaults=params)
        logger.add(slack, format=slack_format, level="SUCCESS")

    controller = Controller(api, dump, parent)

    get_event_loop().run_until_complete(ensure_future(controller.loop()))


if __name__ == "__main__":
    cli()
