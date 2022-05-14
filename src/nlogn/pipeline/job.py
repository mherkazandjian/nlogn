"""

"""
import copy
import asyncio
import time
from datetime import datetime

import apscheduler.schedulers.asyncio

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
        self.outputs = []
        self.original_trigger = None

    def wrap_output(self) -> None:
        """
        add a timestamp and the host identifier to the output dict
        """
        pass

    def current_timestamp(self):
        pass

    async def _run(self):
        instance = self.exec_cls(**self.input)
        #await asyncio.sleep(10)
        # run the exec class in a safe manner
        try:
            output = 'success', instance.run()
        except:
            output = 'failed', None

        return output

    def reschedule_job(self,
                       scheduler_job: apscheduler.job.Job = None,
                       scheduler: apscheduler.schedulers.asyncio.AsyncIOScheduler = None,
                       seconds=None):
        """
        Rescheduler a job by removing it and putting back a the modified job in the scheduler

        :param scheduler_job: The scheduler job that will be modified
        :param scheduler: The scheduler object
        :param seconds: The updated 'seconds' argument of the schedule job
        :return: The modified scheduler job
        """
        scheduler_job.remove()
        scheduler.add_job(
            self.run,
            "interval",
            seconds=seconds,
            id=self.task_name,
            max_instances=self.timeout.max_attempts,
            kwargs={
                'scheduler': scheduler,
            }
        )
        return scheduler_job

    async def run(self, scheduler: apscheduler.schedulers.asyncio.AsyncIOScheduler = None) -> None:
        """
        Run the job and handle its interaction with the shceduler

        :param scheduler: The scheduler that manages jobs
        """
        log.info(f'[{self.task_name}] enter run func, instance # = {len(self.instances)}')

        # fetch the job instance in the scheduler and create a copy of the original job trigger
        # in the scheduler
        scheduler_job = scheduler.get_job(self.task_name)
        if not self.original_trigger:
            self.original_trigger = copy.copy(scheduler_job.trigger)

        # do not execute the exec class if the number of failed attempts
        # has reached the max attempts specified by the task
        if len(self.instances) >= self.timeout.max_attempts:
            log.warn(f'[{self.task_name}] skip execution since max attempts is reached')
            return

        log.info(f'[{self.task_name}] \t execute job')

        # run the exec class asyncroneously
        atask = asyncio.create_task(self._run())
        self.instances.append(atask)
        log.info(f'[{self.task_name}] run exec class in async mode with a timeout')
        await asyncio.wait([atask], timeout=self.timeout.duration.to('sec').magnitude)
        log.info(f'[{self.task_name}] async task {atask} either finished or timed out')
        log.info(f'[{self.task_name}] # instances {len(self.instances)}')

        if atask.done():
            # handle the sucessful execution of the exec class (i.e no timeout, not necessarily
            # a successful execution as intended, i.e an exception could have been raised or
            # in case of executing an un-expected flag could have been returned

            #   - exec class success (assumed yes)
            #   - exec class failed  (not implemented yet)
            status, result = atask.result()
            log.info(f'[{self.task_name}] output = {result}')

            # if exec class success
            # .. todo:: implement exec class failed
            # elif result.error
        else:
            # exec class timed out, set the flags and result and update the next run time
            log.warn(f'[{self.task_name}] task timed out')
            status = 'timeout'
            result = None

        # in order to restore the jobs trigger scheduler or to modify it
        # the trigger is modified accordingly and the scheduler job is
        # removed and a new job with the modified trigger is created and
        # added back to the scheduler
        if status in ['failed', 'timeout']:
            # exec class failed or timed out
            scheduler_job = self.reschedule_job(
                scheduler_job,
                scheduler,
                seconds=scheduler_job.trigger.interval.seconds * self.schedule.cadence_multiplier,
            )
            log.warn(f'[{self.task_name}] next execution time extended')
            log.warn(f'[{self.task_name}] \t {scheduler_job.trigger.interval}')
        elif status == 'success':

            # time stamp and store the result asap before overheads creep
            timestamp_str = datetime.utcnow().isoformat()
            self.outputs.append([timestamp_str, result])

            # restore the original trigger interval
            scheduler_job = self.reschedule_job(
                scheduler_job,
                scheduler,
                seconds=self.schedule.interval.to('sec').magnitude,
            )
            log.info(f'[{self.task_name}] reset the execution interval')
            log.info(f'[{self.task_name}] \t {scheduler_job.trigger.interval}')

            # clear the atasks that were in the list of instances (if any)
            # any successful execution clears all prior instances
            # .. todo:: need to make sure that stalled process are cleared, maybe do this
            #           by periodically checking for records in "ps aux" and not executing
            #           in case a maximum is reached
            if len(self.instances) > 0:
                self.instances = []
                log.warn(f'[{self.task_name}] instances list cleared')

    def transform(self, outputs):
        # ... do the transformations here
        transformed_outputs = outputs  # just a placeholder
        return transformed_outputs

    def send_to_relay(self, outputs):
        # send the outputs to the relay
        pass

    async def dispatch(self, scheduler: apscheduler.schedulers.asyncio.AsyncIOScheduler = None) -> None:
        """

        :param scheduler:
        """
        log.info(f'[{self.task_name}_transform] enter run func, instance # = {len(self.instances)}')

        # pop the collected outputs/results and print them
        outputs = []
        while len(self.outputs) > 0:
            outputs.append(self.outputs.pop())
        for timestamp, result in outputs:
            log.debug(f'[{self.task_name}_transform] {timestamp}:{result}')

        # .. todo:: continue from here
        transformed_outputs = self.transform(outputs)
        self.send_to_relay(transformed_outputs)