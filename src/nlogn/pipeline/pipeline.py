import yaml

from .parser import PipelineParser
from nlogn import log


class Pipelines:
    pass


class Pipeline:
    """
    Read a pipeline file and parse it and produce the list of tasks and stages

    This class does not handle replacing variable values.
    """
    def __init__(self, path: str = None):
        """
        Constructor

        :param path: The path to the pipeline
        """
        self.stages_specs = {}
        self.tasks_specs = {}
        self.parsed = PipelineParser().parse(path)

        self.assemble_tasks_specs()
        self.assemble_stages()

    def find_extends_chain(self, name: str = None, chain: list[str] = None) -> list[str]:
        """
        Find the tasks that extent the specified task "name" recursively

                              returned chain
        /---------------------------------------------------\
                 chain
        /---------------------\             /---------------\
        ... parent1 -> parent2 -> "name" -> child1 -> child2 ...

        :param name: The name of the task
        :param chain: The parent list of tasks of  the current task "name"
        :return: The list of that extend the current task
        """
        log.debug(f'find extension chain "{name}"')
        if not chain:
            chain = []
        chain.append(name)
        task = self.parsed.spec[name]
        if 'extends' in task:
            extends = task['extends']
            self.find_extends_chain(name=extends, chain=chain)
        chain.reverse()
        return chain

    def assemble_task_spec(self, name: str = None) -> dict:
        """
        Assemble a task by replacing recursively all the '.extends' sections

        :param name: the name of the task to be assembled
        :return: the assembled task
        """
        log.debug(f'assmble the task "{name}"')
        chain = self.find_extends_chain(name=name)
        root_name = chain.pop(0)
        assembled = self.parsed.spec[root_name]
        for name in chain:
            extension = self.parsed.spec[name]
            assembled = assembled | extension
        assembled.pop('extends')
        return assembled

    def assemble_tasks_specs(self) -> None:
        """
        Assembe all specs of all the tasks in the pipeline

        a task that has an 'extends' section is crawled recursively and a
        single task spec is assembled.

        changes:
          - self.tasks_specs
        """
        self.tasks_specs = {}
        for name in self.parsed.tasks:
            log.debug(f'assemble task {name}')
            if 'extends' in self.parsed.spec[name]:
                task_spec = self.assemble_task_spec(name)
            else:
                task_spec = self.parsed.spec[name]
            self.tasks_specs[name] = task_spec

    def assemble_stages(self) -> None:
        """
        Construct the list of tasks for each stage

        changes:
          - self.stages_specs
        """
        self.stages_specs = {}
        for name in self.tasks_specs:
            stage = self.tasks_specs[name].get('stage')
            if stage:
                if stage not in self.stages_specs:
                    self.stages_specs[stage] = []
                self.stages_specs[stage].append(name)

    def show_specs(self) -> None:
        """
        Pretty print the pipeline as yaml

        stages:
          stage1:
          - task1
          - task2
          ...
          stage2:
          - task3
          - task4
          ...

        task1:
        ...

        task2:
        ...
        """
        print(yaml.dump({'stages': self.stages_specs}, explicit_start=True).replace('---', '').strip())
        print()
        for task in self.tasks_specs:
            spec = self.task_spec(name=task)
            print(yaml.dump(spec).replace('---', '').strip())
            print()

    def task_spec(self, name: str = None) -> dict:
        """
        Get the spec of a certain task

        :return: the task spec
        """
        return {name: self.tasks_specs[name]}


# (done) get the stages
# (done) find all the jobs for a certain stage
# identify the job that outputs out of the pipeline
# (done)find the hidden stages that are defined to be used as 'extends'
# crawl the directory and see who includes who
# assembel the full pipelines
#   - find the unique pipelines
# for each pipeline:
#   - assembel and execution flow
#   - configure the parameters of the jobs
#       - which function gets called...etc..
#   - configure the objects that gets called by the agent in the main
#     loop or as the schedule function
