import logging
import os
import sys

BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)

#The background is set with 40 plus the number of the color, and the foreground with 30

#These are the sequences need to get colored ouput
RESET_SEQ = "\033[0m"
COLOR_SEQ = "\033[1;%dm"
BOLD_SEQ = "\033[1m"

def formatter_message(message, use_color = True):
    if use_color:
        message = message.replace("$RESET", RESET_SEQ).replace("$BOLD", BOLD_SEQ)
    else:
        message = message.replace("$RESET", "").replace("$BOLD", "")
    return message


COLORS = {
    'ERROR': RED,
    'CRITICAL': YELLOW,
    'WARNING': YELLOW,
    'INFO': WHITE,
    'DEBUG': BLUE
}

class ColoredFormatter(logging.Formatter):
    def __init__(self, msg, use_color = True):
        logging.Formatter.__init__(self, msg)
        self.use_color = use_color

    def format(self, record):
        levelname = record.levelname
        if self.use_color and levelname in COLORS:
            levelname_color = COLOR_SEQ % (30 + COLORS[levelname]) + levelname + RESET_SEQ
            record.levelname = levelname_color
        return logging.Formatter.format(self, record)


# Custom logger class with multiple destinations
class ColoredLogger(logging.Logger):
    FORMAT = "[$BOLD%(name)-50s$RESET][%(levelname)-18s]    %(message)s ($BOLD%(filename)s$RESET:%(lineno)d)"
    COLOR_FORMAT = formatter_message(FORMAT, True)
    def __init__(self, name):
        logging.Logger.__init__(self, name, logging.DEBUG)                                

        color_formatter = ColoredFormatter(self.COLOR_FORMAT)

        console = logging.StreamHandler()
        console.setFormatter(color_formatter)

        self.addHandler(console)
        return


logging.getLogger('apscheduler').setLevel(logging.ERROR)
loggers = {}
logging.setLoggerClass(ColoredLogger)

class AlfaLog:
    @staticmethod
    def get_logger(logger_name, log_level):
        if logger_name not in loggers.keys():
            #formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s')
            #handler = logging.StreamHandler(sys.stdout)
            #handler.setFormatter(formatter)
            #logger = logging.getLogger(logger_name)
            logger = ColoredLogger(logger_name)
            logger.setLevel(log_level)
            #logger.addHandler(handler)
            loggers[logger_name] = logger
        return loggers.get(logger_name)
