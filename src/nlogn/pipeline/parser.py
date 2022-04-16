import os
import yaml

from nlogn import log


class PipelineParser:
    def __init__(self):
        pass

    @staticmethod
    def read(fpath):
        with open(fpath) as fobj:
            return yaml.safe_load(fobj.read())

    def parse(self, path=None):
        if os.path.isfile(path):
            log.debug(f'parse pipline file {path}')
            raw = self.read(path)
            raw_pipeline = PipelineParserSingleFile(spec=raw)
            return raw_pipeline


class PipelineParserSingleFile:
    def __init__(self, spec=None):
        self.spec = spec
        self.stages = None
        self.tasks = None
        self.find_stages()
        self.find_tasks()

    def check_component(self, name):
        if name not in self.spec:
            log.debug(f'attribute {name} not found.')
            return False
        else:
            return True

    def find_stages(self):
        log.debug('find the stages')
        attribute = 'stages'
        self.check_component(attribute)
        self.stages = [stage.strip() for stage in self.spec['stages']]
        log.debug('stages found:')
        for stage in self.stages:
            log.debug(f'\t{stage}')

    def find_tasks(self):
        log.debug('find the tasks')
        self.tasks = [key.strip() for key in self.spec.keys() if key.strip() != 'stages']
        log.debug('tasks found:')
        for task in self.tasks:
            log.debug(f'\t{task}')


class PipelineParserDirectory:
    pass


class PipelineParserProject:
    pass

