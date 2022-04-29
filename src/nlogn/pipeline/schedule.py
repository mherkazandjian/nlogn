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
