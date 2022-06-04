import json
from subprocess import Popen
from subprocess import PIPE
import shlex

from .. import Module

# .. todo:: make sure to document that in the __init__.py each .py
# .. todo:: should be imported

virtual_name = {
    'command': 'Command'
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


class Command(Module):

    def __init__(self, cmd=None, *args, **kwargs):
        super(Command, self).__init__(*args, **kwargs)
        self.cmd = cmd

    def run(self, *args, **kwargs):
        assert self.cmd is not None
        process = Popen(shlex.split(self.cmd), stdout=PIPE, stderr=PIPE)
        stdout, stderr = tuple(map(bytes.decode, process.communicate()))

        return json.dumps(
            {
                'stdout': stdout,
                'stderr': stderr,
                'retcode': None   # .. todo:: this is a placeholder
            }
        )


"""
def main(*args, **kwargs):
    pass
.. todo:: as an option each module can be also executed from the command line as such:
    $ nlogn -m plugin.builtin.command --arg1=foo arg2=bar
"""

