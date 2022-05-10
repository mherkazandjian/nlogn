import time


class Schedule:
    def __init__(self):
        self.interval = None
        self.cadence_multiplier = None
    def __str__(self):
        retval = ""
        retval += (
            f'interval: {repr(self.interval)}\n'
            f'cadence_multiplier: {self.cadence_multiplier}'
        )
        return retval


class Timeout:
    def __init__(self):
        self.duration = None
        self.max_attempts = None
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

