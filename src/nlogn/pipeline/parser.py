import os
import yaml

from nlogn import log


class PipelineParserSingleFile:
    """
    Helper class for accessing attributes of a raw pipeline
    """
    def __init__(self, spec: dict = None):
        """
        Constructor
        :param spec: The raw data of the pipeline as read e.g from the yaml file
        """
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

    def find_stages(self) -> None:
        """
        Find the stages of the pipeline

        set the value of the attribute: stages
        """
        attribute = 'stages'
        log.debug(f'find the {attribute}')

        self.check_component(attribute)
        self.stages = [stage.strip() for stage in self.spec['stages']]
        log.debug('stages found:')
        for stage in self.stages:
            log.debug(f'\t{stage}')

    def find_tasks(self) -> None:
        """
        Find the tasks of the pipeline.

        Everything other than 'stages' is assumed to be a task
        """
        log.debug('find the tasks')
        self.tasks = [key.strip() for key in self.spec.keys() if key.strip() != 'stages']
        log.debug('tasks found:')
        for task in self.tasks:
            log.debug(f'\t{task}')


class PipelineParserDirectory:
    pass


class PipelineParserProject:
    pass


class PipelineParser:
    """
    Parse a single file that defines a pipeline
    """
    def __init__(self):
        """
        Constructor
        """
        pass

    @staticmethod
    def read(fpath: str) -> dict:
        """
        Read a yaml file and return its content as parsed yaml
        :param fpath: the path to the yaml file that defines the pipeline
        :return: the parsed pipeline
        """
        with open(fpath) as fobj:
            return yaml.safe_load(fobj.read())

    def parse(self, path: str = None) -> PipelineParserSingleFile:
        """
        Read the yaml pipeline file and parse it and return parsed object of the raw pipeline

        :param path: the path to the yaml file that defines the pipeline
        :return: The raw parsed pipeline
        """
        if os.path.isfile(path):
            log.debug(f'parse pipline file {path}')
            raw = self.read(path)
            raw_pipeline = PipelineParserSingleFile(spec=raw)
            return raw_pipeline
