from os import makedirs
from re import Pattern, compile

from .log import Log


class Config(object):
    def __init__(
        self,
        parent,
        app_filter=".*",
        cooldown=5,
        debug_path=None,
        dry_run=False,
        log_level="INFO",
        logger=None,
        template_filter=".*",
        template_path=None,
    ):
        self.parent = parent
        self.app_filter = (
            app_filter if isinstance(app_filter, Pattern) else compile(app_filter)
        )
        self.cooldown = cooldown
        self.debug_path = debug_path
        self.dry_run = dry_run
        self.log_level = log_level
        self.logger = (
            Log.get_logger("Config()", log_level) if logger is None else logger
        )
        self.template_filter = (
            template_filter
            if isinstance(template_filter, Pattern)
            else compile(template_filter)
        )
        self.template_path = template_path

        if self.debug_path:
            makedirs(self.debug_path, exist_ok=True)

        self.logger.info("Config loaded:")
        self.logger.info(f"    parent: {self.parent}")
        self.logger.info(f"    app_filter: {self.app_filter}")
        self.logger.info(f"    cooldown: {self.cooldown}")
        self.logger.info(f"    debug_path: {self.debug_path}")
        self.logger.info(f"    dry_run: {self.dry_run}")
        self.logger.info(f"    log_level: {self.log_level}")
        self.logger.info(f"    template_filter: {self.template_filter}")
        self.logger.info(f"    template_path: {self.template_path}")
