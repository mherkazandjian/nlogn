"""

"""
import json
import copy
import asyncio
import socket
from datetime import datetime

import warnings
warnings.filterwarnings('ignore', message='Certificate for localhost has no')
import requests

import apscheduler.schedulers.asyncio

from nlogn import log
from nlogn import units


class Job:
    """

    """
    def __init__(self):
        """
        Constructor
        """
        self.task_name = None
        self.exec_cls = None
        self.exec_func = None
        self.input = None
        self.schedule = None
        self.timeout = None
        self.transforms = None
        self.output = None
        self.outputs = []

        self.instances = []
        self.original_trigger = None

        self.history = None   #: not used yet
        self.status = 'unset'  #: not used yet

    def wrap_output(self) -> None:
        """
        add a timestamp and the host identifier to the output dict
        """
        pass

    def current_timestamp(self):
        pass

    async def _run(self):
        if self.exec_cls:
            instance = self.exec_cls(**self.input)
            _callable = instance.run
        elif self.exec_func:
            _callable = self.exec_func
        else:
            raise ValueError('either a exec class of a callable should be set')

        # run the exec class in a safe manner
        try:
            output = 'success', _callable()
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
            #   assumed yes ...
            # else
            # .. todo:: implement handling exec class failed
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

            # get the hostname
            hostname = socket.gethostname()

            # run the transforms stack
            transformed_output = self.transform(result)

            # check that the transformed data is in line with the expected columns
            # and do the unit conversion and prepare the type info to be appended to the ouput
            prepared_outputs, columns_info = self.prepare_outputs(transformed_output)

            # store the output that is ready to be dispatched
            self.outputs.append(
                {
                    'timestamp': timestamp_str,
                    'hostname': hostname,
                    'output': prepared_outputs,
                    'columns': columns_info,
                    'name': self.task_name
                }
            )

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

    def transform(self, output):
        """
        Apply the stack of the transforms to the output

        The output should be compatible with the 'columns' spec of the task defined in the pipeline

        :param output:
        :return: dict
        """
        transformed_output = copy.copy(output)
        for transform in self.transforms:
            if transform.py_class:
                raise NotImplementedError('classess not supported for transforms yet')
            func = transform.py_func
            transformed_output = func(output=transformed_output, **transform.input)

        return transformed_output

    def prepare_outputs(self, outputs):
        """

        :return:
        """
        # check that the column names are the same as specified in the output spec
        output_col_names = set(outputs.keys())
        expected_col_names = {column.name for column in self.output.columns}
        diff = expected_col_names - output_col_names
        if diff:
            for name in diff:
                log.error(f'the following column name is inconsistent {name}')
            raise ValueError('there is an inconsistency between the transformed data and the expected columns')

        columns_info = {}
        prepared_output = {}
        for col in self.output.columns:

            col_name_with_unit = col.name
            if col.unit:
                col_name_with_unit += f'_{col.unit}'

            if col.unit:
                converted_value = outputs[col.name].to(col.unit).magnitude
                converted_casted_value = units.types_map[col.type](converted_value)
                prepared_output[col_name_with_unit] = converted_casted_value
            else:
                prepared_output[col_name_with_unit] = outputs[col.name]

            columns_info[col_name_with_unit] = col.type

        return prepared_output, columns_info

    async def _post_data(self, connection=None, outputs=None):
        """

        :param connection:
        :param outputs:
        :return:
        """
        try:
            response = requests.post(
                connection['url'],
                auth=connection['auth'],
                headers=connection['headers'],
                data=json.dumps(outputs),
                verify=connection['cert'],
            )
            retval = 'success', response
        except:
            retval = 'failed', None

        return retval

    async def dispatch(self, connection=None) -> None:
        """

        :param connection:
        :return:
        """
        task_name = self.task_name + '_relay'
        POST_TIMEOUT = 10  # timeout for the post request

        log.info(f'[{task_name}] enter dispatch method')

        # pop the collected outputs/results and print them
        outputs_send = []
        while len(self.outputs) > 0:
            outputs_send.append(self.outputs.pop(0))

        atask = asyncio.create_task(self._post_data(connection, outputs_send))
        log.info(f'[{task_name}] async dispatch post outputs to relay server')
        await asyncio.wait([atask], timeout=POST_TIMEOUT)

        if atask.done():
            # handle the sucessful dispatch of the outputs

            status, result = atask.result()
            log.info(f'[{task_name}] post output = {result}')
        else:
            # dispatch post request timed out
            log.warn(f'[{task_name}] task timed out')
