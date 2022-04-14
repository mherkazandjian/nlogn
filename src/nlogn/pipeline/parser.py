import yaml


class PipelineParser:
    def __init__(self, path=None):
        self.path = path

    @staticmethod
    def read(fpath):
        with open(fpath) as fobj:
            return yaml.safe_load(fobj.read())


class PipelineParserSingleFile:
    pass


class PipelineParserDirectory:
    pass


class PipelineParserProject:
    pass

