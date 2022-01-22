import time
from subprocess import Popen
from subprocess import PIPE
import shlex

#
#
#

class StopWatch:
    def __init__(self):
        self.name = None
        self.t_start = None
        self.t_end = None
    def tic(self):
        pass
    def toc(self):
        pass
    def __enter__(self):
        self.t_start = time.time()
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.name:
            self.t_end = time.time()
            print(f'{self.name}')
            print(f'elapsed time: {self.t_start - self.t_start}')


class ShellCommand:
    def __init__(self):
        self.process = None
    def running(self):
        # is the process still running or not? has it returned?
        pass


class ShellScript(ShellCommand):
    def __init__(self):
        # accept and executes a script
        #   - write the script to a /tmp/foo.sh location wraped in a #!/usr/bin/env bash
        #   - execute the script and keep track of the exit flag..etc..
        pass


class ShellJob:
    def __init__(self):
        self.processes = []  #: list of ShellCommand
        self.multiplier = 2  #: everytime last attempt fails next time execute
                             #  it this time later
    def start(self):
        pass
    def end(self):
        pass