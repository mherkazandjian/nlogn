import os
import sys
import logging


BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)
# The background is set with 40 plus the number of the color, and the
# foreground with 30


logger_name = 'nlogn'
logger_level_env_var = 'NLOGN_LOG_LEVEL'


LOG_COLORS = {
    'WARNING': YELLOW,
    'INFO': WHITE,
    'DEBUG': BLUE,
    'CRITICAL': MAGENTA,
    'ERROR': RED
}

log_levels = {
    'notest': logging.NOTSET,
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}

# These are the sequences need to get colored output
RESET_SEQ = "\033[0m"
COLOR_SEQ = "\033[1;%dm"
BOLD_SEQ = "\033[1m"


def formatter_message(message, use_color=True):
    if use_color:
        message = message.replace("$RESET", RESET_SEQ).replace("$BOLD", BOLD_SEQ)
    else:
        message = message.replace("$RESET", "").replace("$BOLD", "")
    return message


class ColoredFormatter(logging.Formatter):
    def __init__(self, msg, use_color=True):
        logging.Formatter.__init__(self, msg)
        self.use_color = use_color

    def format(self, record):
        levelname = record.levelname
        if self.use_color and levelname in LOG_COLORS:
            levelname_color = COLOR_SEQ % (30 + LOG_COLORS[levelname]) + levelname + RESET_SEQ
            record.levelname = levelname_color
        return logging.Formatter.format(self, record)


class CustomFileHandler(logging.FileHandler):
    def __init__(self, *args, **kwargs):
        super(CustomFileHandler, self).__init__(*args, **kwargs)


def create_stream_handler(log_level):
    """
    Create console handler and set level to debug

    :param log_level: the logger log level
    :return: the stream handler
    """
    channel = logging.StreamHandler(sys.stdout)
    channel.setLevel(log_level)

    fmt = ("[%(asctime)s %(levelname)-18s "
           "$BOLD%(filename)s{%(lineno)-5d}$RESET:%(funcName)s()] "
           "%(message)s")

    formatter = formatter_message(fmt)
    color_formatter = ColoredFormatter(formatter)

    channel.setFormatter(color_formatter)

    return channel


def create_file_handler(log_level: str = 'debug', fpath: str = 'debug.log'):
    """
    Create console handler and set level to debug

    :param log_level: the logger log level
    :param fpath: the path to the log file
    :return: the stream handler
    """
    channel = logging.FileHandler(os.path.join(fpath), "w")
    channel.setLevel(log_levels[log_level])
    fmt = ("[%(asctime)s %(levelname)-18s "
           "%(filename)s{%(lineno)-5d}:%(funcName)s()] "
           "%(message)s")

    formatter = logging.Formatter(fmt)
    channel.setFormatter(formatter)

    return channel


def test_all_log_levels(logger):
    logger.info('test info message')
    logger.debug('test debug message')
    logger.warning('test warning message')
    logger.error('test error message')
    logger.critical('test critical message')


def create_logger():

    # create the logger instance
    logger = logging.getLogger(logger_name)
    logger.log_depth = 0
    logger.setLevel(log_levels['debug'])

    _log_level = os.environ.get(logger_level_env_var, 'info')
    log_level = log_levels[_log_level]

    # add terminal output channel to logger
    stream_handler = create_stream_handler(log_level)
    logger.addHandler(stream_handler)

    # add the file output channel to the logger
    #file_handler = create_file_handler(log_level)
    #logger.addHandler(file_handler)

    # DEV
    test_all_log_levels(logger)
    # END DEV

    return logger


logger = create_logger()


