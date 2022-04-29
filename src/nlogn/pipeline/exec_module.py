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

        self.input = spec[module]
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
        "The actual python module that contains the class whose run method will be executed"

    def find_module_in_paths(self):
        """get the actual callable method / function"""

    def fully_qualified_name(self) -> str:
        """
        Replace the shortened module path with the fully quialified path
        """
        real_module_path = self.module.replace('nlogn.', 'nlogn.plugins.')
        return real_module_path

    @staticmethod
    def replace_variable(str_in, variable: dict = None) -> str:
        """
        return true if 'variable' exists in 'value'
        :param variable: the variable name and its value to be replced
        :param str_in: the string that might contain the 'variable' by name
        :return: The string with the variable replaced (if variable exists)
        """
        # only one variable value can be replaced at a time
        assert len(variable) == 1

        # the variable must be a dict
        assert isinstance(variable, dict)

        key, value = '$'+list(variable.keys())[0], list(variable.values())[0]
        str_in = str_in.replace(key, value)
        return str_in

    def find_referenced_variables(self) -> tuple[str]:
        """
        Find the list of referenced variables in the input(s) or the output(s)
        :return: tuple of referenced variables
        """
        variables = []
        for key, value in self.input.items():
            variables.extend(list(filter(lambda x: x.startswith('$'), value.split(' '))))
        return tuple(variables)

    def replace_variables(self, variables: dict = None):
        """
        Replace the value of the variable in 'input', 'output' (output not supported yet)
        :param variables: the key value pair of the variables
        :return: A copy of the object with the variables replaced wherever they occure
        """
        # find the list of variables that need to be replaced
        _required_variables = self.find_referenced_variables()
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


