from asyncio import ensure_future, get_event_loop

from .log import Log
from .controller import AlfaController

class AlfaOperator:
  def __init__(self, config, controller = None, logger = None):
    self.config = config
    self.controller = controller if controller else AlfaController(config)
    self.logger = Log.get_logger(f'AlfaOperator()', self.config.log_level) if logger is None else logger

  def loop(self):
    get_event_loop().run_until_complete(ensure_future(self.controller.loop()))
