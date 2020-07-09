import logging
import os
import sys

LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()

logging.getLogger('apscheduler').setLevel(logging.ERROR)
loggers = {}

class Log:
  @staticmethod
  def get_logger(logger_name):
    if logger_name not in loggers.keys():
      formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s')
      handler = logging.StreamHandler(sys.stdout)
      handler.setFormatter(formatter)
      logger = logging.getLogger(logger_name)
      logger.setLevel(LOGLEVEL)
      logger.addHandler(handler)
      loggers[logger_name] = logger
    return loggers.get(logger_name)
