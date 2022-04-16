import yaml

from .parser import PipelineParser
from nlogn import log


class Pipelines:
    pass


class Pipeline:
    def __init__(self, path=None):
        self.stages_specs = {}
        self.tasks_specs = {}
        self.parsed = PipelineParser().parse(path)
        self.assemble_tasks_specs()
        self.assemble_stages()

    def find_extends_chain(self, name=None, chain=None):
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

    def assemble_task(self, name=None):
        log.debug(f'assmble the task "{name}"')
        chain = self.find_extends_chain(name=name)
        root_name = chain.pop(0)
        assembled = self.parsed.spec[root_name]
        for name in chain:
            extension = self.parsed.spec[name]
            assembled = assembled | extension
        assembled.pop('extends')
        return assembled

    def assemble_tasks_specs(self):
        self.tasks_specs = {}
        for name in self.parsed.tasks:
            log.debug(f'assemble task {name}')
            if 'extends' in self.parsed.spec[name]:
                task_spec = self.assemble_task(name)
            else:
                task_spec = self.parsed.spec[name]
            self.tasks_specs[name] = task_spec

    def assemble_stages(self):
        self.stages_specs = {}
        for name in self.tasks_specs:
            stage = self.tasks_specs[name]['stage']
            if stage not in self.stages_specs:
                self.stages_specs[stage] = []
            else:
                self.stages_specs[stage].append(name)

    def show_specs(self):
        print(yaml.dump({'stages': self.stages_specs}, explicit_start=True).replace('---', '').strip())
        print()
        for task in self.tasks_specs:
            print(yaml.dump({task: self.tasks_specs[task]}, explicit_start=True).replace('---', '').strip())
            print()

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