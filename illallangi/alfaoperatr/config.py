from os import makedirs

from loguru import logger


class Config(object):
    def __init__(
        self,
        parent,
        debug_path=None,
    ):
        self.parent = parent
        self.debug_path = debug_path

        if self.debug_path:
            makedirs(self.debug_path, exist_ok=True)

        logger.info("Config loaded:")
        logger.info(f"    parent: {self.parent}")
        logger.info(f"    debug_path: {self.debug_path}")
