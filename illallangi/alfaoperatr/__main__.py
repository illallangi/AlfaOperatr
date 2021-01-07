from asyncio import ensure_future, get_event_loop
from sys import stderr

from click import Choice as CHOICE, INT, Path as PATH, STRING, argument, group, option

from illallangi.k8sapi import API as K8S_API

from loguru import logger

from notifiers.logging import NotificationHandler

from .clusterController import ClusterController
from .config import Config


@group()
@option(
    "--log-level",
    type=CHOICE(
        ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "SUCCESS", "TRACE"],
        case_sensitive=False,
    ),
    default="INFO",
)
@option("--slack-webhook", type=STRING, envvar="SLACK_WEBHOOK", default=None)
@option("--slack-username", type=STRING, envvar="SLACK_USERNAME", default=__name__)
@option("--slack-format", type=STRING, envvar="SLACK_FORMAT", default="{message}")
def cli(log_level, slack_webhook, slack_username, slack_format):
    logger.remove()
    logger.add(stderr, level=log_level)

    if slack_webhook:
        params = {"username": slack_username, "webhook_url": slack_webhook}
        slack = NotificationHandler("slack", defaults=params)
        logger.add(slack, format=slack_format, level="SUCCESS")


@cli.command(name="process-templates")
@argument("parent", required=True)
@option(
    "--api-proxy",
    default="http://localhost:8001",
    show_default=False,
    type=STRING,
)
@option("--app-filter", default=".*", show_default=False, type=STRING)
@option("--cooldown", default=5, show_default=False, type=INT)
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
@option("--dry-run", is_flag=True)
@option("--template-filter", default=".*", show_default=False, type=STRING)
@option(
    "--template-path",
    default=None,
    show_default=False,
    type=PATH(
        exists=False,
        file_okay=False,
        dir_okay=True,
        writable=False,
        readable=True,
        resolve_path=True,
        allow_dash=False,
    ),
)
def process_templates(
    parent,
    api_proxy,
    app_filter,
    cooldown,
    debug_path,
    dry_run,
    template_filter,
    template_path,
):

    config = Config(
        parent=parent,
        app_filter=app_filter,
        cooldown=cooldown,
        debug_path=debug_path,
        dry_run=dry_run,
        template_filter=template_filter,
        template_path=template_path,
    )

    api = K8S_API(api_proxy)

    controller = ClusterController(config, api)

    get_event_loop().run_until_complete(ensure_future(controller.loop()))


if __name__ == "__main__":
    cli()
