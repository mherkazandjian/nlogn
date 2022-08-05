import copy


class ExecModuleInput:
    def __init__(self):
       pass


class ExecModuleOutput:
    def __init__(self):
        pass


class ExecModule:
    """
    Module section handler of a 'task' in the task spec, e.g

    home_part_usage:
      nlogn.builtin.command:    # <<<< this is referred to as a module
        cmd: df -bk $MOUNT_POINT
      ...
      ...
    """
    def __init__(self, spec: dict = None) -> None:
        """
        Constructor

        :param spec: The spec of the module in the task spec for example
         ``{'nlogn.builtin.command': {'cmd': 'df -bk $MOUNT_POINT'}}``
        """
        module = list(spec.keys()).pop()
        self.module = module
        "The partially resolved module python path e.g nlogn.builtin.command"

        inputs = spec[module]
        if not inputs:
            inputs = {}

        self.input = inputs
        # .. todo:: if the output is a well defined builting on an extension / plugin
        # .. todo:: that can be looked up where the output can be inferred then
        # .. todo:: the output is set, otherwise the output is assumed to be plain
        # .. todo:: text that is supposed to be transformed

        self.output = None
        "The output of the execution module"

        self.py_module_spec = None
        "The module spec obtaied from importlib"

        self.py_module = None
        "The actual python module that contains the class whose run method will be executed"

        self.py_class = None
        "The actual class whose run method will be executed (if it is set)"

        self.py_func = None
        "The actual function that will be executed (if it is set)"

    def find_module_in_paths(self):
        """get the actual callable method / function"""
        pass

    def fully_qualified_name(self) -> str:
        """
        Replace the shortened module path with the fully quialified path
        """
        # appending 'plugins' after 'nlogn.' is needed only for the builtin
        # plugins. i.e nlogn.plugins.builtin.xyz can be refered as
        # nlogn.builtin.xyz instead of repeating 'plugins.' everytime
        real_module_path = self.module.replace('nlogn.', 'nlogn.plugins.')
        return real_module_path

    @staticmethod
    def replace_variable(str_in, variable: dict = None) -> str:
        """
        replace the value of 'variable' in the input string 'str_in'

        :param variable: the variable name and its value to be replced
        :param str_in: the string that might contain the 'variable' by name
        :return: The string with the variable replaced (if variable exists)
        """
        # only one variable value can be replaced at a time and it must be a dict
        assert len(variable) == 1
        assert isinstance(variable, dict)

        key, value = '$'+list(variable.keys())[0], list(variable.values())[0]
        str_in = str_in.replace(key, value)
        return str_in

    def find_referenced_variables(self, attr=None) -> tuple[str]:
        """
        Find the list of referenced var names in the specified attr, e.g in self.input

        :return: referenced var names
        """
        def find_vars(value):
            """
            Keep and return tokens in value that start with $
                - non string value are ignored (since they can not be split)
            """
            if isinstance(value, str):
                retval = list(filter(lambda x: x.startswith('$'), value.split(' ')))
            else:
                retval = list()
            return retval

        # .. todo:: keys are ignoed and no var (token staring with $) is looked
        #           up for. Think if that might come in handy in some situations
        variables = []
        attr_v = getattr(self, attr)
        for key, value in attr_v.items():

            if isinstance(value, (list, tuple)):
                _value = value
            else:
                _value = [value]

            for item in _value:
                variables.extend(find_vars(item))

        return tuple(variables)

    def replace_variables(self, variables: dict = None):
        """
        Replace the value of the variables in 'input', 'output' (output not supported yet)

        changes:
          - self.input (overwrite self.input with the replaced variables)

        :param variables: the key value pair of the variables
        """
        # find the list of variables that need to be replaced
        _required_variables = self.find_referenced_variables(attr='input')
        required_variables = []
        for var in _required_variables:
            required_variables.append(var.replace('$', '', 1))

        # replace the variables in the allowed/specified sections. In this case
        # the input can have variables also the output but that is not supported yet
        _exec_mod = copy.copy(self)
        input = {}
        for key, value in _exec_mod.input.items():
            for var_name in required_variables:
                key = self.replace_variable(key, {var_name: variables[var_name]})
                value = self.replace_variable(value, {var_name: variables[var_name]})
            input[key] = value
        self.input = input

    def __str__(self):
        retval = ''
        retval += f'module: {self.module}\n'
        retval += f'class: {self.py_class}\n'
        retval += f'input: {repr(self.input)}\n'
        retval += f'output: {repr(self.output)}'
        return retval


