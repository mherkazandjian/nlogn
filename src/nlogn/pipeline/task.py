"""

"""


class TaskDefinitionStatus:
    pass

class Defined(TaskDefinitionStatus):
    pass
class Partial(TaskDefinitionStatus):
    pass
class Undefined(TaskDefinitionStatus):
    pass


class Timeout:
    def __init__(self):
        self.duration = None
        self.max_attempts = None
        self.cadence_multiplier = None


class MetaTask:
    pass


class Columns:
    """
    .. todo:: use Pint https://github.com/hgrecco/pint
    """
    def __init__(self):
        self.names = None
        self.units = None


class Output:
    def __init__(self):
        self.columns = None


class ModuleInput:
    def __init__(self):
       pass


class ModuleOutput:
    def __init__(self):
        pass


class Module:
    def __init__(self):
        self.func = None
        self.input = None
        self.output = None


class Task:
    """
    A task that is executed in a pipeline

    The specs of the task is passed as a dictionary
    """
    def __init__(self, spec):
        pass
        self.status = Undefined
        self.schedule = None
        self.timeout = None
        self.module = None
        self.output = None

    def func(self):
        pass