"""

"""
import importlib
import yaml

import nlogn


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
    """
    Module section handler of a 'task' in the task spec, e.g

    home_part_usage:
      nlogn.builtin.command:    # <<<< this is referred to as a module
        cmd: df -bk $MOUNT_POINT
      ...
      ...
    """
    def __init__(self, spec=None):
        """
        Constructor

        :param spec: .. todo:: ...
        """
        module = list(spec.keys()).pop()
        self.module = module
        "The partially resolved module python path e.g nlogn.builtin.command"

        self.input = spec[module]
        # .. todo:: if the output is a well defined builting on an extension / plugin
        # .. todo:: that can be looked up where the output can be inferred then
        # .. todo:: the output is set, otherwise the output is assumed to be plain
        # .. todo:: text that is supposed to be transformed
        self.output = None

        self.py_module_spec = None
        "The module spec obtaied from importlib"

        self.py_module = None
        "The actual python module that contains the class whose run method will be executed"

        self.py_class = None
        "The actual python module that contains the class whose run method will be executed"

    def find_module_in_paths(self):
        """get the actual callable method / function"""

    def fully_qualified_name(self):
        real_module_path = self.module.replace('nlogn.', 'nlogn.plugins.')
        return real_module_path

    def __str__(self):
        retval = ''
        retval += f'module: {self.module}\n'
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
        self.pipeline = pipeline
        self.task_name = name
        self.task = None

        if name is not None:
            self.find_task()
            self.check_module_sanity()
            self.find_py_module()
            self.find_py_module_class()

    def find_task(self):
        task_spec = self.pipeline.task_spec(name=self.task_name)
        print(yaml.dump(task_spec))
        self.task = Task(spec=task_spec)

    def check_module_sanity(self):
        py_module_spec = importlib.util.find_spec(self.task.module.fully_qualified_name())
        assert py_module_spec is not None
        self.task.module.py_module_spec = py_module_spec

    def find_py_module(self):
        mod = importlib.util.module_from_spec(self.task.module.py_module_spec)
        self.task.module.py_module_spec.loader.exec_module(mod)
        self.task.module.py_module = mod

    def find_py_module_class(self):
        exec_module_name_only = self.task.module.fully_qualified_name().split('.')[-1]
        for vname, cls_name in self.task.module.py_module.virtual_name.items():
            if vname == exec_module_name_only:
                exec_cls = getattr(self.task.module.py_module, cls_name)
                break
        else:
            raise ValueError(f'{exec_module_name_only} not found')
        self.task.module.py_class = exec_cls


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

        self.find_module()

    def find_module(self):
        """
        Search for the main module of the task in the task spec

        .. todo:: allow for searching in namespaces other than 'nlogn' in the python path
        """
        for key in self.spec:
            if key.startswith('nlogn.'):
                module_spec = self.spec[key]
                self.module = Module(spec={key: module_spec})
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
