import os
import platform
import math
import re
import time
import datetime as dt
import logging
import glob
import tarfile
from subprocess import Popen, PIPE
import shlex
from shutil import copyfile


log = logging.getLogger('')
log.setLevel(logging.DEBUG)

now = dt.datetime.fromtimestamp(time.time())


class MyFormatter(logging.Formatter):
    """
    Custom formatter for the logger
    """
    converter = dt.datetime.fromtimestamp

    def formatTime(self, record, datefmt=None):
        """
        Set the time formatter of the logger

        :param record: the record
        :param datefmt: the date format
        :return: formatter
        """
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s,%03d" % (t, record.msecs)
        return s


console = logging.StreamHandler()
formatter = MyFormatter(
    fmt='%(asctime)s-%(name)s-%(levelname)-8s] %(message)s',
    datefmt='%H:%M:%S.%f'
)
console.setFormatter(formatter)

log.addHandler(console)
