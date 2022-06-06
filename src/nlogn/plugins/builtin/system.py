import json
from subprocess import Popen
from subprocess import PIPE
import shlex
import datetime
import time
import psutil

from .. import Module
from nlogn.units import bytes_units_converter
from nlogn.units import u

# .. todo:: make sure to document that in the __init__.py each .py
# .. todo:: should be imported

virtual_name = {
    'cpu_metrics': 'cpu_metrics',
    'cpu_specs': 'cpu_specs',
    'mem_metrics': 'mem_metrics'
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


def cpu_specs():
    cmd = 'lscpu'
    process = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
    # .. todo:: recent version of lscpu support --json that make parsing the output
    #           much easier and elegant, try to see how to use it since on old
    #           systems / machines the lscpu version is old and --json is not supported
    stdout, stderr = tuple(map(bytes.decode, process.communicate()))

    retval = {}
    for line in stdout.splitlines():
        if 'thread(s) per core' in line.lower():
            retval['threads_per_core'] = int(line.split(':')[-1].strip())
        if 'core(s) per socket' in line.lower():
            retval['cores_per_socket'] = int(line.split(':')[-1].strip())
        if 'socket(s)' in line.lower():
            retval['sockets'] = int(line.split(':')[-1].strip())
        if 'model name' in line.lower():
            retval['model_name'] = line.split(':')[-1].strip()
        #if 'hypervisor vendor' in line.lower():
        #    retval['hypervisor'] = line.split(':')[-1].strip()
        if 'cpu mhz' in line.lower():
            retval['mhz'] = line.split(':')[-1].strip()

    return retval


def cpu_metrics():
    # cpu info and usage statistics
    n_cpu = psutil.cpu_count()
    cpu_percent = psutil.cpu_percent()
    cpu_current_freq = psutil.cpu_freq()
    cpu_load_ave = psutil.getloadavg()

    retval = {
        'cpu_used': cpu_percent * n_cpu / 100.0,
        'cpu_n_cores': n_cpu,
        'cpu_current_freq': cpu_current_freq.current * u.gigahertz,
        'cpu_load_ave_1m': cpu_load_ave[0],
        'cpu_load_ave_5m': cpu_load_ave[1],
        'cpu_load_ave_15m': cpu_load_ave[2],
    }

    return retval


def mem_metrics():
    # get the memory usage info
    memory_info = psutil.virtual_memory()

    retval = {
        'mem_used': bytes_units_converter(f'{memory_info.used}B'),
        'mem_total': bytes_units_converter(f'{memory_info.total}B'),
        'mem_swap': bytes_units_converter(f'{psutil.swap_memory().total}B')
    }
    return retval

"""
def main(*args, **kwargs):
    pass
.. todo:: as an option each module can be also executed from the command line as such:
    $ nlogn -m plugin.builtin.command --arg1=foo arg2=bar
"""

