from asyncio import ensure_future, get_event_loop

import click

from .clusterController import ClusterController
from .config import Config


@click.command(context_settings={"auto_envvar_prefix": "ALFA"})
@click.argument('parent', required=True)
@click.option('--api-proxy', default='http://localhost:8001', show_default=False, type=click.STRING)
@click.option('--app-filter', default='.*', show_default=False, type=click.STRING)
@click.option('--cooldown', default=5, show_default=False, type=click.INT)
@click.option('--debug-path', default=None, show_default=False, type=click.Path(exists=False, file_okay=False, dir_okay=True, writable=True, readable=True, resolve_path=True, allow_dash=False))
@click.option('--dry-run', is_flag=True)
@click.option('--log-level', default='INFO', show_default=True, type=click.Choice(['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'], case_sensitive=False))
@click.option('--template-filter', default='.*', show_default=False, type=click.STRING)
@click.option('--template-path', default=None, show_default=False, type=click.Path(exists=False, file_okay=False, dir_okay=True, writable=False, readable=True, resolve_path=True, allow_dash=False))
def main(
        parent,
        api_proxy,
        app_filter,
        cooldown,
        debug_path,
        dry_run,
        log_level,
        template_filter,
        template_path):

    config = Config(
        parent=parent,
        api_proxy=api_proxy,
        app_filter=app_filter,
        cooldown=cooldown,
        debug_path=debug_path,
        dry_run=dry_run,
        log_level=log_level,
        template_filter=template_filter,
        template_path=template_path)

    controller = ClusterController(config)

    get_event_loop().run_until_complete(ensure_future(controller.loop()))


if __name__ == "__main__":
    main()
