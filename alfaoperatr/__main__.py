import click
from .config import AlfaConfig
from .operator import AlfaOperator

@click.command(context_settings={"auto_envvar_prefix": "ALFA"})
@click.option('--api-proxy',       default='http://localhost:8001', show_default=False, type=click.STRING)
@click.option('--app-filter',      default='.*',                    show_default=False, type=click.STRING)
@click.option('--cooldown',        default=5,                       show_default=False, type=click.INT)
@click.option('--debug-path',      default=None,                    show_default=False, type=click.Path(exists=False, file_okay=False, dir_okay=True, writable=True, readable=True, resolve_path=True, allow_dash=False))
@click.option('--log-level',       default='INFO',                  show_default=True,  type=click.Choice(['CRITICAL','ERROR','WARNING','INFO','DEBUG'], case_sensitive=False))
@click.option('--template-filter', default='.*',                    show_default=False, type=click.STRING)
@click.option('--template-path',   default=None,                    show_default=False, type=click.Path(exists=False, file_okay=False, dir_okay=True, writable=False, readable=True, resolve_path=True, allow_dash=False))
def main(
        api_proxy,
        app_filter,
        cooldown,
        debug_path,
        log_level,
        template_filter,
        template_path):
    config = AlfaConfig(
        api_proxy=api_proxy,
        app_filter=app_filter,
        cooldown=cooldown,
        debug_path=debug_path,
        log_level=log_level,
        template_filter=template_filter,
        template_path=template_path)
    operator = AlfaOperator(config)
    operator.loop()

if __name__ == "__main__":
    main()
