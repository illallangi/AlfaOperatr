from os import makedirs
from re import Pattern, compile

from loguru import logger


class Config(object):
    def __init__(
        self,
        parent,
        app_filter=".*",
        cooldown=5,
        debug_path=None,
        dry_run=False,
        log_level="INFO",
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
        self.template_filter = (
            template_filter
            if isinstance(template_filter, Pattern)
            else compile(template_filter)
        )
        self.template_path = template_path

        if self.debug_path:
            makedirs(self.debug_path, exist_ok=True)

        logger.info("Config loaded:")
        logger.info(f"    parent: {self.parent}")
        logger.info(f"    app_filter: {self.app_filter}")
        logger.info(f"    cooldown: {self.cooldown}")
        logger.info(f"    debug_path: {self.debug_path}")
        logger.info(f"    dry_run: {self.dry_run}")
        logger.info(f"    log_level: {self.log_level}")
        logger.info(f"    template_filter: {self.template_filter}")
        logger.info(f"    template_path: {self.template_path}")
