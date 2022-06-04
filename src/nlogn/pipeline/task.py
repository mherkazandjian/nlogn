"""

"""
import os
import re
import importlib
import yaml
import pint

import nlogn
from nlogn.pipeline.pipeline import Pipeline
from nlogn.pipeline.exec_module import ExecModule
from nlogn.pipeline.schedule import Schedule
from nlogn.pipeline.schedule import Timeout
from nlogn.pipeline.job import Job

_ureg = pint.UnitRegistry()


class TaskDefinitionStatus:
    pass
class Defined(TaskDefinitionStatus):
    pass
class Partial(TaskDefinitionStatus):
    pass
class Undefined(TaskDefinitionStatus):
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


class TaskRenderer:
    """
    Process a task in a pipeline and prepare it for execution

    - replace variables
    - ensure that the components of the task that are pre-requisites to create
      a fully defined job are populated and checked.
    - prepare the lsit of components that are needed to create the job
       - exec module
       - schedule
       - timeout
       - output
       - transforms
    - create multiple tasks based on parameteres (e.g task grid..etc..)
    """
    def __init__(self, name: str = None, pipeline: Pipeline = None):
        """
        Constructor

        :param name: The name of the task that will be composed.
        :param pipeline: The parsed and assembled pipeline object to which the task belongs to.
        """
        self.pipeline = pipeline
        self.task_name = name
        self.task = None

        if name is not None:

            self.find_task()

            self.check_exec_module_sanity()
            self.find_exec_py_module()
            self.find_exec_py_module_class()
            self.replace_variables()

            self.check_transforms_modules_sanity()
            self.find_transforms_py_module()
            self.find_transforms_py_module_class()
            # .. todo:: replace variables not supported at the moment for transforms


    def find_task(self) -> None:
        """
        Create the task instance from the task spec

        Changes:
          - self.task
        """
        task_spec = self.pipeline.task_spec(name=self.task_name)
        self.task = Task(spec=task_spec)

    def check_exec_module_sanity(self) -> None:
        """
        Make sure that the module of the task is importable and set the module spec to the task

        Changes:
          - self.task.module.py_module_spec
        """
        py_module_spec = importlib.util.find_spec(self.task.exec_module.fully_qualified_name())
        assert py_module_spec is not None
        self.task.exec_module.py_module_spec = py_module_spec

    def check_transforms_modules_sanity(self) -> None:
        """
        Make sure that the module of the transform is importable and set the module spec to the transforms

        Changes:
          - self.task.transforms[:].py_module_spec

        .. todo:: this method is a copy of self.check_exec_module_sanity but it is done like this
                  for quick testinf and prototyping, .. todo:: reuse the code and cleanup
        """
        for transform in self.task.transforms:
            module_path = transform.fully_qualified_name()
            if not os.path.exists(module_path):
                module_path = module_path.rsplit('.', 1)[0]
            py_module_spec = importlib.util.find_spec(module_path)
            assert py_module_spec is not None
            transform.py_module_spec = py_module_spec

    def find_exec_py_module(self) -> None:
        """
        Find the python execution module (.py file) specified in the task and set it

        .. todo:: this method is a copy of self.check_exec_py_module but it is done like this
                  for quick testinf and prototyping, .. todo:: reuse the code and cleanup
        Changes:
          - self.task.transforms[:].py_module
        """
        mod = importlib.util.module_from_spec(self.task.exec_module.py_module_spec)
        self.task.exec_module.py_module_spec.loader.exec_module(mod)
        self.task.exec_module.py_module = mod

    def find_transforms_py_module(self) -> None:
        """
        Find the python execution module (.py file) specified in the transforms and set it

        Changes:
          - self.task.transforms[:].py_module
        """
        for transform in self.task.transforms:
            mod = importlib.util.module_from_spec(transform.py_module_spec)
            transform.py_module_spec.loader.exec_module(mod)
            transform.py_module = mod

    def find_exec_py_module_class(self) -> None:
        """
        Find the actual execution class name from the virtual class name in the module

        For example, if the execution module is 'nlogn.builtin.command', then in this case
        the class 'Command' in the file 'ROOT/nlogn/plugins/builtin/command.py:Command' is
        set to 'self.task.module.py_class'

        Changes:
          - self.task.module.py_class
        """
        exec_module_name_only = self.task.exec_module.fully_qualified_name().split('.')[-1]
        for vname, cls_name in self.task.exec_module.py_module.virtual_name.items():
            if vname == exec_module_name_only:
                cls = getattr(self.task.exec_module.py_module, cls_name)
                break
        else:
            raise ValueError(f'{exec_module_name_only} not found')
        self.task.exec_module.py_class = cls

    def find_transforms_py_module_class(self) -> None:
        """
        Find the actual execution class name or function from the virtual class name in the module

        For example, if the execution module is 'nlogn.builtin.command', then in this case
        the class 'Command' in the file 'ROOT/nlogn/plugins/builtin/command.py:Command' is
        set to 'self.task.module.py_class'

        .. note:: for transforms the module should be defined to have either the virtual_name
                  or not have it and use the callables only.
        Changes:
          - self.task.transforms.py_class
        """
        for transform in self.task.transforms:
            transform_module_name_only = transform.fully_qualified_name().split('.')[-1]
            if hasattr(transform.py_module, 'virtual_name'):
                for vname, cls_name in transform.py_module.virtual_name.items():
                    if vname == transform_module_name_only:
                        cls = getattr(transform.py_module, cls_name)
                        break
                else:
                    raise ValueError(f'{transform_module_name_only} not found')
            elif transform_module_name_only in dir(transform.py_module):
                cls = getattr(transform.py_module, transform_module_name_only)
            else:
                raise ValueError(f'{transform_module_name_only} not found')

            # set the proper attribute callable or class
            if type(cls) == type:
                transform.py_class = cls
            elif callable(cls):
                transform.py_func = cls
            else:
                raise ValueError(f'transform should be a class type or callable, got {type(cls)}')


    def replace_variables(self):
        """
        Replace all variables defined in a task wherever the variables are referenced

        .. todo:: maybe it is a good idea and it makes the logic simpler to
         use the yaml output of the task spec treated as a string to replace
         the variable vaules by doing string like substitution
        """
        """
        replace variables in:
          - self.task.exec_module (make a copy of it)
          - self.task.schedule
          - self.task.timeout
        """
        self.task.exec_module.replace_variables(self.task.variables)

    def create_job(self):
        """
        Create a job from the composed task.

        This method should be executed after variables have been replaced
        :return:
        """
        job = Job()

        job.task_name = self.task_name
        job.exec_cls = self.task.exec_module.py_class
        job.input = self.task.exec_module.input
        job.schedule = self.task.schedule
        job.timeout = self.task.timeout
        job.history = []
        job.output = {}

        return job

    def summary(self):
        """
        Print a summary of the task
        """
        print('---------')
        print('task name')
        print('---------')
        print(f'  {self.task_name}')
        print()

        print('-----------------------------------------------------')
        print('task as yaml before rendering and replacing variables')
        print('-----------------------------------------------------')
        print(self.task)

        print('----------------')
        print('execution module')
        print('----------------')
        print(self.task.exec_module)



class Task:
    """
    A single task that definition that is defined in a pipeline

    The following components are expected to be found in a task definition:

      - task name
      - execution module (one [supported]) .. todo:: implement multiple/nested/chained exec modules
      - output (this is the final output that is exported [ingested or commited to a destination])
      - stage: the name of the stage to which the task belongs to
      - timeout: the timeout of the task
      - variables: variables that are defined for this task
      - when: define how often or when the task is executed
    """
    def __init__(self, spec: dict = None):
        """
        Constructor

        :param spec: the spec of the task from the parsed raw pipeline, e.g

          .. code-block:: python

             {'home_part_usage': {'stage': 'nfs',
              'timeout': {'duration': '5s', 'max_attempts': 10},
              'when': {'interval': '10s', 'cadence_multiplier': 2},
              'nlogn.builtin.command': {'cmd': 'df -bk $MOUNT_POINT'},
              'output': {'transform': [{'nlogn.builtin.remove_header': ['first_line']}],
               'columns': ['filesystem[str]',
                'mount_point[str]',
                'bytes_total[long:kB]',
                'bytes_used[long:kB]'],
               'target': 'home_block'},
              'variables': {'MOUNT_POINT': '/home'}}}
        """
        self.name: str = list(spec.keys()).pop()
        """The name of the task"""

        self.spec: dict = spec[self.name]
        """The spec of the task that is copied from the passed arg"""

        self.status = Undefined
        """???"""

        self.schedule: Schedule = None
        """The schedule configuration of the task"""

        self.timeout: Timeout = None
        """The timeout  configuration of the task"""

        self.exec_module: ExecModule = None
        """The main execution module of the task"""

        self.output = None
        """The output configuration of the task"""

        self.transforms = None
        """The chain of transformation on the raw output of the execution module"""

        self.variables = None
        """The key value pair of the variables used in the task"""

        self.find_exec_module()
        self.find_variables()
        self.find_schedule()
        self.find_transforms()

    def find_exec_module(self):
        """
        Search for the main module of the task in the task spec, e.g nlogn.builtin.command

        .. todo:: allow for searching in namespaces other than 'nlogn' in the python path
                  for example nlogn.plugins.foo.bar

        changes:
          - self.exec_module
        """
        # .. todo:: use a filter pattern here on the key value pairs
        for key in self.spec:
            if key.startswith('nlogn.'):
                module_spec = self.spec[key]
                self.exec_module = ExecModule(spec={key: module_spec})
                break
        else:
            raise KeyError('Task does not have an execution module')

    def find_schedule(self):
        """
        Search for the schedule information and the cadence

        The following are searched for and set:

           - timeout:
               - duration
               - max_attempts
           - when:
              interval:
              cadence_multiplier
           - on_status:
               - fail:
               - success:
               - no_response: (hang)
               - nlogn.foo.bar

        changes:
          - self.schedule
          - self.timeout
        """
        # instantiate and set the values of the schedule object
        schedule = Schedule()
        interval = self.spec['when']['interval']
        if isinstance(interval, int):
            schedule.interval = float(interval) * _ureg.Quantity('s')
        elif isinstance(interval, str):
            match = re.match(r"([0-9]+)([a-z]+)", interval, re.I)
            value, unit = match.groups()
            schedule.interval = float(value) * _ureg.Quantity(unit)

        cadence_multiplier = self.spec['when']['cadence_multiplier']
        schedule.cadence_multiplier = cadence_multiplier

        self.schedule = schedule

        # instantiate and set the values of the timeout object
        timeout = Timeout()
        duration = self.spec['timeout']['duration']
        if isinstance(duration, int):
            timeout.interval = duration * _ureg.Quantity('s')
        elif isinstance(duration, str):
            match = re.match(r"([0-9]+)([a-z]+)", duration, re.I)
            value, unit = match.groups()
            timeout.duration = float(value) * _ureg.Quantity(unit)

        max_attempts = self.spec['timeout']['max_attempts']
        timeout.max_attempts = max_attempts

        self.timeout = timeout

    def find_variables(self):
        """
        Find the variables in the task definition.

        The key value pairs are set as strings

        changes:
          - self.variables
        """
        attr = 'variables'
        variables = {}
        for key, value in self.spec[attr].items():
            variables[f'{key}'] = f'{value}'
        self.variables = variables

    def find_transforms(self):
        """
        Search for the transforms section in the output section

        .. todo:: allow for searching in namespaces other than 'nlogn' in the python path
                  for example nlogn.plugins.foo.bar

        changes:
          - self.transforms
        """
        output = self.spec.get('output')
        # no output is specified hence no transforms are specified / needed
        if not output:
            self.transforms = []
            return

        transforms = output.get('transform')
        # no transforms is specified
        if not transforms:
            self.transforms = []
        else:
            # one or more transform is specified, pre-process them and
            # add them to the list of transforms. Each transform should
            # be a python exec module
            transforms_list = []

            # .. todo:: use a filter pattern here on the key value pairs
            for transform in transforms:
                exec_module = ExecModule(spec=transform)
                transforms_list.append(exec_module)

            self.transforms = transforms_list

    def find_output(self):
        pass

    def find_stage(self):
        pass

    def __str__(self):
        retval = yaml.dump(self.spec)
        return retval
