import os
import sys
import logging

BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)
# The background is set with 40 plus the number of the color, and the
# foreground with 30

# These are the sequences need to get colored ouput
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
    'WARNING': YELLOW,
    'INFO': WHITE,
    'DEBUG': BLUE,
    'CRITICAL': YELLOW,
    'ERROR': RED
}


class ColoredFormatter(logging.Formatter):
    def __init__(self, msg, use_color=True):
        logging.Formatter.__init__(self, msg)
        self.use_color = use_color

    def format(self, record):
        levelname = record.levelname
        if self.use_color and levelname in COLORS:
            levelname_color = COLOR_SEQ % (30 + COLORS[levelname]) + levelname + RESET_SEQ
            record.levelname = levelname_color
        return logging.Formatter.format(self, record)


# create logger
logger = logging.getLogger('nlogn')
logger.log_depth = 0

log_levels = {
    'notest': logging.NOTSET,
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}


if not len(logger.handlers):

    _log_level = os.environ.get('NLOGN_LOG_LEVEL', 'info')
    log_level = log_levels[_log_level]

    logger.setLevel(log_level)

    # create console handler and set level to debug
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(log_level)

    # create formatter
    formatter = logging.Formatter(
        ('[%(asctime)s %(funcName)s() %(filename)s'
         ':%(lineno)-7d][%(levelname)-8s] %(message)s'))

    # just like print()
    # formatter = logging.Formatter('%(message)s')

    # a color formatter
    FORMAT = ("[%(asctime)s %(levelname)-18s "
              "$BOLD%(filename)s{%(lineno)-5d}$RESET:%(funcName)s()] "
              "%(message)s")
    COLOR_FORMAT = formatter_message(FORMAT, True)
    formatter = ColoredFormatter(COLOR_FORMAT)

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)

    # DEV
    logger.info('test info message')
    logger.debug('test debug message')
    logger.warning('test warning message')
    logger.error('test error message')
    logger.critical('test critical message')
    # END DEV
