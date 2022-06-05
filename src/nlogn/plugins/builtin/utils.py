import json
from subprocess import Popen
from subprocess import PIPE
import shlex

from .. import Module
from nlogn.units import bytes_units_converter

# .. todo:: make sure to document that in the __init__.py each .py
# .. todo:: should be imported

virtual_name = {
    'remove_header': 'remove_header',
    'keep_cols': 'keep_cols',
    'df_cmd_size_single_mount': 'df_cmd_size_single_mount',
    'df_cmd_inodes_single_mount': 'df_cmd_inodes_single_mount'
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

def remove_header(input=None, lines=1):
    return input


def keep_cols(input=None, cols=[]):
    return input


def df_cmd_size_single_mount(output=None, *args, **kwargs):
    _output = json.loads(output)
    record = _output['stdout'].splitlines()[-1]
    fs, sz, used, _, _, mount = map(str.strip, record.split())
    sz = bytes_units_converter(sz.replace('K', 'KB'))
    used = bytes_units_converter(used.replace('K', 'KB'))

    retval = {
        'filesystem': fs,
        'mount': mount,
        'size_total': sz,
        'size_used': used
    }

    return retval


def df_cmd_inodes_single_mount(output=None, *args, **kwargs):
    _output = json.loads(output)
    record = _output['stdout'].splitlines()[-1]
    fs, sz, used, _, _, mount = map(str.strip, record.split())

    retval = {
        'filesystem': fs,
        'mount': mount,
        'inodes_total': sz,
        'inodes_used': used
    }

    return retval

"""
def main(*args, **kwargs):
    pass
.. todo:: as an option each module can be also executed from the command line as such:
    $ nlogn -m plugin.builtin.command --arg1=foo arg2=bar
"""

