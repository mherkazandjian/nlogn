from . import Module


class Cmd(Module):
    """
    Execute a command using Popen
    """
    default_output = bytes
    def __init__(self):
        pass