from asyncio import ensure_future, get_event_loop
from sys import stderr

from click import Choice as CHOICE, Path as PATH, STRING, command, option

from illallangi.alfa.cluster import Controller

from loguru import logger

from slack_sdk import WebClient


class SlackHandler(object):
    def __init__(self, token, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.token = token
        self.client = WebClient(self.token)

    def write(self, message):
        result = self.client.chat_postMessage(
            channel="#general",
            text=f'Template {message.record["extra"]["template"]} processed {message.record["extra"]["render"]}, {message.record["message"]}',
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f'Template {message.record["extra"]["template"]} processed {message.record["extra"]["render"]}, {message.record["message"]}',
                    },
                }
            ],
        )

        for file in message.record["extra"].get("files", []):
            self.client.files_upload(
                channels="#general",
                content=file["yaml"],
                filename=file["filename"],
                filetype="javascript",
                title=file["title"],
                thread_ts=result["ts"],
            )


def log_format(record):
    f = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} |"
    if "template" in record["extra"]:
        f += " <e>{extra[template]}</e>"
    if "kind" in record["extra"]:
        f += " <c>{extra[kind]}</c>"
    f += " <g>{module}</g> <level>{message}</level>"
    if "render" in record["extra"]:
        f += " <y>{extra[render]}</y>"
    f += "\n"
    return f


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
@option("--slack-token", type=STRING, envvar="SLACK_TOKEN", default=None)
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
    slack_token,
    api,
    dump,
    parent,
):
    logger.remove()
    logger.add(stderr, format=log_format, level=log_level)
    if slack_token:
        slack = SlackHandler(token=slack_token)
        logger.add(slack, level="SUCCESS")

    controller = Controller(api, dump, parent)

    get_event_loop().run_until_complete(ensure_future(controller.loop()))


if __name__ == "__main__":
    cli()
