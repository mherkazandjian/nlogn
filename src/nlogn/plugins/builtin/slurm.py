import copy
import json
from subprocess import Popen
from subprocess import PIPE
import shlex
from io import StringIO
import csv
import re
import datetime
import time
import psutil

from .. import Module
from nlogn.units import bytes_units_converter
from nlogn.units import u

# .. todo:: make sure to document that in the __init__.py each .py
# .. todo:: should be imported

virtual_name = {
    'squeue_all': 'squeue_all',
    'sinfo_all': 'sinfo_all',
    'sacct_info': 'sacct_info'
    # .. todo:: by default the key should have the same name as the module.py
    # .. todo::
    # .. todo:: in case more than one class is defined in the .py file
    # .. todo:: having __virtual_name__ defined as a dict allow for the future to support
    # .. todo:: multiple classes being mapped to sub-modules, e.g:
    # .. todo:: a package that defines:
    # .. todo::
    # .. todo:: foo/__init__.py
    # .. todo::     bar.py:
    # .. todo::            claas Util1: pass
    # .. todo::            claas Util2: pass
    # .. todo::
    # .. todo:: these two utility classes would be called as:
    # .. todo::   nlogn.foo.bar.util1
    # .. todo::   nlogn.foo.bar.util2
    # .. todo::
    # .. todo:: note that in this case searching for nlogn.foo.bar.util2 will fail since
    # .. todo:: foo/bar/util1.py does not exist, so some checks needs to be done to support
    # .. todo:: this behavior
}


class Sinfo:
    """
    Current nodes info - alloc,idle,
    sinfo -N -p all  --format="%N|%t|%a|%c|%O|%z|%m|%e|%b|%E"
    """
    pass


def sinfo_all():
    pass


class Squeue:
    """
    Current jobs info by user by executing the command squeue -o %all
    """
    def __init__(self, keep_columns=None):
        self.keep_columns = keep_columns

    @staticmethod
    def convert_time_limit_to_sec(val):
        _, days, hhmmss = re.match('((.*)\-)?(.*)', val).groups()
        days = 0 if not days else int(days)
        t = time.strptime(hhmmss, '%H:%M:%S')
        t_sec = days*24*3600 + 3600*t.tm_hour + 60*t.tm_min + t.tm_sec
        return t_sec

    def run(self) -> list:
        """
        Execute the command "squeue -o %all" command and return the output
        """
        cmd = "squeue -o %all"
        process = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
        stdout, stderr = tuple(map(bytes.decode, process.communicate()))

        assert stdout.strip()
        # .. todo:: handle this assertion better
        buff = StringIO()
        buff.write(stdout)
        buff.seek(0)

        # filter out / keep only the columns of interest
        rows_output = []
        with buff:
            data = list(csv.reader(buff, delimiter='|', ))
            header, rows = list(map(str.strip, map(str.lower, data[0]))), data[1:]
            for row in rows:
                _row_out = {}
                for col_name, val in zip(header, row):
                    if col_name in self.keep_columns:
                        _row_out[col_name] = val
                rows_output.append(_row_out)

        retval = copy.copy(rows_output)

        # check field values and align them to avoid having parsing errors
        # along the way either in elasticsearch or when parsing
        for item in retval:
            # convert field value of time_limit from D-HH:MM:SS to sec
            item['time_limit'] = self.convert_time_limit_to_sec(item['time_limit'])
            if item['end_time'].strip() == 'N/A':
                item['end_time'] = '0'
            if item['start_time'].strip() == 'N/A':
                item['start_time'] = '0'

        return retval


def squeue_all(keep_columns: list = None, *args, **kwargs) -> list:
    """
    Execute the run function of an instance of the Squeue class

    :param keep_columns: see doc of Squeue.run
    :returns: see doc of Squeue.run
    """
    squeue = Squeue(keep_columns=keep_columns)
    retval = squeue.run()
    return retval


class Sacct:
    """
    Core hours usage by user, dept, group
    """
    pass


def sacct_info():
    pass
