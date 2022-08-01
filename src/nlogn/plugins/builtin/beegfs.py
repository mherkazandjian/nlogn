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
    'beegfs_user_usage': 'beegfs_user_usage',
}

class BeegfsUserUsage:
    """
    Usage of a beegfs pool by user

    the following command is used to collect the output:

        $ beegfs-ctl --getquota --uid --all --csv

    """
    def __init__(self, storagepoolid: int = None):
        """
        Constructor

        :param storagepoolid: The storage pool id to be queried
        """
        self.storagepoolid = storagepoolid
        self.cmd = f"beegfs-ctl --getquota --uid --all --csv --storagepoolid={storagepoolid}"

    def run(self) -> list:
        """
        Execute the command, parse the output and return it
        """
        cmd = self.cmd
        process = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
        stdout, stderr = tuple(map(bytes.decode, process.communicate()))

        assert stdout.strip()

        # .. todo:: handle this assertion better
        buff = StringIO()
        buff.write(stdout)
        buff.seek(0)

        # filter out / keep only the columns of interest (user and hours)
        # include each user once
        rows_output = []
        with buff:
            data = list(csv.reader(buff, delimiter=',',))
            _, rows = list(map(str.strip, map(str.lower, data[0]))), data[1:]
            header = ['name', 'uid', 'used', 'quota', 'filesused', 'filesquota']
            for row in rows:
                _row_out = {}
                for col_name, val in zip(header, row):
                    if val.strip() == 'unlimited':
                        # replace 'unlimited' with 0 for the 'hard' columns
                        val = 0
                    if col_name in ['used', 'quota']:
                        val = bytes_units_converter(f'{val}B')
                    _row_out[col_name] = val
                rows_output.append(_row_out)

        retval = copy.copy(rows_output)

        return retval

def beegfs_user_usage(storagepoolid: str = None, *args, **kwargs) -> list:
    """
    Execute the run function of an instance of the BeegfsUserUsage class

    :returns: see doc of BeegfsUserUsage
    """
    beegfs_user_usage  = BeegfsUserUsage(storagepoolid=storagepoolid)
    retval = beegfs_user_usage.run()
    return retval
