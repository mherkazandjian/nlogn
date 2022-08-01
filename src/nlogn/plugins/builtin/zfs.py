import copy
import json
from subprocess import Popen
from subprocess import PIPE
import shlex
from io import StringIO
import csv
import re
import datetime
from dateutil import relativedelta
import time
import psutil

from .. import Module
from nlogn.units import bytes_units_converter
from nlogn.units import u

# .. todo:: make sure to document that in the __init__.py each .py
# .. todo:: should be imported

virtual_name = {
    'zfs_user_usage': 'zfs_user_usage',
}

class ZfsUserUsage:
    """
    Usage of a zfs partition by user

    the following command is used to collect the output:

        $ zfs userspace -p -o name,used,quota,objused,objquota -S used <my_fs>

    """
    def __init__(self, filesystem: str = None, keep_columns: str = None):
        """
        Constructor

        :param filesystem: the filesystem that is targete for usage info
        """
        self.keep_columns = keep_columns
        self.filesystem = filesystem
        self.cmd = f"zfs userspace -p -o name,used,quota,objused,objquota -S used {filesystem}"

    def run(self) -> list:
        """
        Execute the command, parse the output and return it
        """
        cmd = self.cmd
        process = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
        stdout, stderr = tuple(map(bytes.decode, process.communicate()))

        assert stdout.strip()
        # replace multi-whitespace with single whitespace
        # then replace that single whitespace (excluding newline) with a ,
        stdout = re.sub(r" +", " ", stdout)
        stdout = re.sub(r"[^\S\r\n]", ",", stdout)

        # .. todo:: handle this assertion better
        buff = StringIO()
        buff.write(stdout)
        buff.seek(0)

        # filter out / keep only the columns of interest (user and hours)
        # include each user once
        rows_output = []
        with buff:
            data = list(csv.reader(buff, delimiter=',', ))
            header, rows = list(map(str.strip, map(str.lower, data[0]))), data[1:]
            for row in rows:
                _row_out = {}
                for col_name, val in zip(header, row):
                    if col_name in self.keep_columns:
                        if val.strip() == '-':
                            # replace '-' with 0 for the objused and the objquota
                            # columns since if no data is found for a user these
                            # are set to '-'
                            val = 0
                        if col_name in ['used', 'quota']:
                            val = bytes_units_converter(f'{val}B')
                        _row_out[col_name] = val
                rows_output.append(_row_out)

        retval = copy.copy(rows_output)

        return retval

def zfs_user_usage(filesystem: str = None, keep_columns=None, *args, **kwargs) -> list:
    """
    Execute the run function of an instance of the ZfsUserUsage class

    :returns: see doc of ZfsUserUsage
    """
    zfs_user_usage = ZfsUserUsage(keep_columns=keep_columns, filesystem=filesystem)
    retval = zfs_user_usage.run()
    return retval
