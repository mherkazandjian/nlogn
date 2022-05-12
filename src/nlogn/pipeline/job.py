"""

"""
import asyncio
import time
from nlogn import log


class Job:
    """

    """
    def __init__(self):
        """
        Constructor
        """
        self.task_name = None
        self.exec_cls = None
        self.input = None
        self.output = None
        self.schedule = None
        self.timeout = None
        self.history = None
        self.status = 'unset'
        self.instances = []

    def wrap_output(self) -> None:
        """
        add a timestamp and the host identifier to the output dict
        """
        pass

    def current_timestamp(self):
        pass

    async def _run(self, **kwargs):
        instance = self.exec_cls(**self.input)
        self.instances.append(instance)
        #await asyncio.sleep(5)
        log.info('_run await sleep finished')

        # run the exec class in a safe manner
        try:
            output = 'success', instance.run()
        except:
            output = 'failed', None

        return output

    async def run(self, *args, **kwargs):

        # .. todo:: -loop here max_attempt times instead of having max_instances set for
        # .. todo::  the scheduler
        # .. todo:: - (timeout duration * max_attempts) should be larger than the task interval
        log.info(f'[{self.task_name}] enter run func, num instances {len(self.instances)}...')
        log.info(f'[{self.task_name}] \t execute job "{self.task_name}", # existing instances {len(self.instances)}')
        atask = asyncio.create_task(self._run(**kwargs))
        log.info('run exec class in async mode with a timeout')
        await asyncio.wait([atask], timeout=kwargs['exec_class_timeout'])
        log.info(f'async task {atask}')

        # handle the job timeout
        if atask.done():
            status, result = atask.result()
            log.info(f'{result}')
        else:
            log.warn('task timed out')
            status = 'timeout'
            result = None

        if status == 'failed':
            # retry again max attempt times (see above)
            pass
            # not handeled yet ...
        self.status = 'terminated'
        self.instances.pop()
