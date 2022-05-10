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

    async def run(self, *args, **kwargs):
        log.info(f'[{self.task_name}] enter run func...')
        if len(self.instances) > self.timeout.max_attempts:
            log.warn(f'[{self.task_name}] \t max attempts reached {len(self.instances)}, not running more instances')
            return
        elif self.status in ['unset', 'terminated']:
            log.info(f'[{self.task_name}] \t execute job "{self.task_name}", # existing instances {len(self.instances)}')
            instance = self.exec_cls(**self.input)
            self.status = 'dispatched'
            self.instances.append(instance)
            output = instance.run()
            log.debug(f'[{self.task_name}] \t' + output)
            #time.sleep(20)
            await asyncio.sleep(kwargs['task_to_finish'])
            self.status = 'terminated'
            self.instances.pop()
            return output
