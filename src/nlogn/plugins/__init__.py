class Module:
    output_spec = None
    virtual_name = None
    input_spec = None
    def __init__(self, *args, **kwargs):
        self.func = None
        self.output_spec = None
        self.output = None
    def __check__(self):
        pass
    def __before__(self):
        pass
    def __after__(self):
        pass
    def run(self):
        pass
