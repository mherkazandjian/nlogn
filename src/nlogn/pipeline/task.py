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
    def __init__(self, spec=None):
        self.columns = None
    def find_transforms(self):
        """
        .. todo:: implement these below
        Find the section of transforms where each is a module as Module objects
        Find the corresponding modules and insert them into a list
        Find the input args for each
        :return: json
        """
        pass


class ModuleInput:
    def __init__(self):
       pass


class ModuleOutput:
    def __init__(self):
        pass


class Module:
    def __init__(self, spec=None):
        func = list(spec.keys()).pop()
        self.func = func
        self.input = spec[func]
        # .. todo:: if the output is a well defined builting on an extension / plugin
        # .. todo:: that can be looked up where the output can be inferred then
        # .. todo:: the output is set, otherwise the output is assumed to be plain
        # .. todo:: text that is supposed to be transformed
        self.output = None

    def find_module_in_paths(self):
        """get the actual callable method / function"""
        pass

    def __str__(self):
        retval = ''
        retval += f'func: {self.func}\n'
        retval += f'input: {repr(self.input)}\n'
        retval += f'output: {repr(self.output)}'
        return retval


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
        self.name = list(spec.keys()).pop()
        self.spec = spec[self.name]
        self.status = Undefined
        self.schedule = None
        self.timeout = None
        self.module = None
        self.output = None
        self.transforms = None

    def find_module(self):
        for key in self.spec:
            if key.startswith('nlogn.'):
                module_spec = self.spec[key]
                self.module = Module(spec=module_spec)
                break
        else:
            raise KeyError('Task does not have an execution module')

    def find_timeout(self):
        pass

    def find_output(self):
        pass

    def find_stage(self):
        pass

    def find_variables(self):
        pass

    def find_schedule(self):
        pass
