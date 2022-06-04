import time


class Schedule:
    """
    A schedule that defines an interval and a cadence multiplier for that interval
    """
    def __init__(self):
        """
        Constructor
        """
        self.interval = None
        """A duration in seconds"""

        self.cadence_multiplier: int = None
        """An integer multiplier for the interval"""

    def __str__(self):
        retval = ""
        retval += (
            f'interval: {repr(self.interval)}\n'
            f'cadence_multiplier: {self.cadence_multiplier}'
        )
        return retval


class Timeout:
    """A duration after which an action is terminated"""
    def __init__(self):
        """
        Constructor
        """

        self.duration = None
        """The duration in seconds"""

        self.max_attempts = None
        """The maximum number of attempts for an arbitrary action"""

    def __str__(self):
        retval = ""
        retval += (
            f'duration: {repr(self.duration)}\n'
            f'max_attempts: {self.max_attempts}\n'
        )
        return retval


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

