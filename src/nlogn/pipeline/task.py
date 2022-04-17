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


class TaskComposer:
    """
    Process a task in a pipeline and prepare it for execution

    - replace variables
    - create multiple tasks based on parameteres (e.g task grid..etc..)
    """
    def __init__(self, pipeline=None, name=None):
        pass


class Task:
    """
    A task that is executed in a pipeline
    """
    def __init__(self, spec=None):
        self.spec = spec
        self.status = Undefined
        self.schedule = None
        self.timeout = None
        self.module = None
        self.output = None
        self.transforms = None

    def func(self):
        pass