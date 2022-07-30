"""

"""
import json
import copy
import asyncio
import socket
import time
from datetime import datetime

import warnings
warnings.filterwarnings('ignore', message='Certificate for ')
import requests
import urllib3
urllib3.disable_warnings()
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()
# .. todo:: clean up these warning ignore mechanisms

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
            _kwargs = {}
        elif self.exec_func:
            _callable = self.exec_func
            _kwargs = self.input
        else:
            raise ValueError('either a exec class of a callable should be set')

        # run the exec class in a safe manner
        try:
            t0 = time.time()
            output = 'success', _callable(**_kwargs)
            dt = time.time() - t0
            log.debug(f'[{self.task_name}] callable for task finished in {dt:5.2}s')
        except:
            output = 'failed', None

        return output

    def reschedule_job(self,
                       scheduler_job: apscheduler.job.Job = None,
                       scheduler: apscheduler.schedulers.asyncio.AsyncIOScheduler = None,
                       cluster: str = '',
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
                'cluster': cluster
            }
        )

        return scheduler_job

    async def run(self,
                  cluster: str = '',
                  scheduler: apscheduler.schedulers.asyncio.AsyncIOScheduler = None) -> None:
        """
        Run the job and handle its interaction with the shceduler

        :param cluster:
        :param scheduler: The scheduler that manages jobs
        """
        _t0 = time.time()
        log.debug(f'[{self.task_name}] enter run func, instance # = {len(self.instances)}')

        # fetch the job instance in the scheduler and create a copy of the original job trigger
        # in the scheduler
        scheduler_job = scheduler.get_job(self.task_name)
        if not self.original_trigger:
            self.original_trigger = copy.copy(scheduler_job.trigger)

        # do not execute the exec class if the number of failed attempts
        # has reached the max attempts specified by the task
        if len(self.instances) >= self.timeout.max_attempts:
            log.warning(f'[{self.task_name}] skip execution since max attempts is reached')
            return

        log.info(f'[{self.task_name}] \t execute job')

        # run the exec class asyncroneously
        t0 = time.time()
        atask = asyncio.create_task(self._run())
        self.instances.append(atask)
        log.debug(f'[{self.task_name}] run exec class in async mode with a timeout')
        await asyncio.wait([atask], timeout=self.timeout.duration.to('sec').magnitude)
        dt = time.time() - t0
        log.debug(f'[{self.task_name}] {atask} either finished in {dt:5.2f}s or timed out')
        log.debug(f'[{self.task_name}] # instances {len(self.instances)}')

        if atask.done():
            # handle the sucessful execution of the exec class (i.e no timeout, not necessarily
            # a successful execution as intended, i.e an exception could have been raised or
            # in case of executing an un-expected flag could have been returned

            #   - exec class success (assumed yes)
            #   - exec class failed  (not implemented yet)
            status, result = atask.result()
            log.debug(f'[{self.task_name}] output = {result}')

            # if exec class success
            #   assumed yes ...
            # else
            # .. todo:: implement handling exec class failed
            # elif result.error
        else:
            # exec class timed out, set the flags and result and update the next run time
            log.warning(f'[{self.task_name}] task timed out')
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
            log.warning(f'[{self.task_name}] next execution time extended')
            log.warning(f'[{self.task_name}] \t {scheduler_job.trigger.interval}')
        elif status == 'success':

            # time stamp and store the result asap before overheads creep
            timestamp_str = datetime.utcnow().isoformat()

            # get the hostname
            hostname = socket.gethostname()

            # check the returned type of the result and put it in a list
            # if it is a dict and assign it to a variable expected to be
            # used later on
            if isinstance(result, dict):
                results = [result]
            elif isinstance(result, list):
                results = result
            else:
                raise ValueError(
                    f'unsupported result: expected "dict" or "list" got "{type(result)}"'
                )

            for _result in results:
                # run the transforms stack
                transformed_output = self.transform(_result)

                # check that the transformed data is in line with the expected columns
                # and do the unit conversion and prepare the type info to be appended to the ouput
                prepared_outputs, columns_info = self.prepare_outputs(transformed_output)

                # store the output that is ready to be dispatched
                output = {
                    'timestamp': timestamp_str,
                    'hostname': hostname,
                    'output': prepared_outputs,
                    'columns': columns_info,
                    'name': self.task_name,
                    'cluster': cluster
                }

                self.outputs.append(output)

            # restore the original trigger interval if its interval has been modified
            original_trigger_dt = self.schedule.interval.to('sec').magnitude
            current_trigger_dt = scheduler_job.trigger.interval.seconds
            trigger_dt_rel_diff = abs(0.0 - current_trigger_dt / original_trigger_dt)
            # consider a different greater than 1% a difference and reset it
            # .. todo:: this means that a cadance multiplier minimum is 1% (probably most cadance
            #           multipliers will be larger than 2 anyway so this is quite safe compared
            #           to using original_trigger_dt - current_trigger_dt == 0.0
            if trigger_dt_rel_diff > 1e-1:
                scheduler_job = self.reschedule_job(
                    scheduler_job=scheduler_job,
                    scheduler=scheduler,
                    cluster=cluster,
                    seconds=self.schedule.interval.to('sec').magnitude,
                )
                log.debug(f'[{self.task_name}] reset the execution interval')
                log.debug(f'[{self.task_name}] \t {scheduler_job.trigger.interval}')

            # clear the atasks that were in the list of instances (if any)
            # any successful execution clears all prior instances
            # .. todo:: need to make sure that stalled process are cleared, maybe do this
            #           by periodically checking for records in "ps aux" and not executing
            #           in case a maximum is reached
            if len(self.instances) > 0:
                self.instances = []
                log.debug(f'[{self.task_name}] instances list cleared')

            _dt = time.time() - _t0
            log.debug(f'[{self.task_name}] total time running {_dt:5.2f}s')

        #for task in asyncio.tasks.all_tasks():
        #    if task.done():
        #        tas

        log.debug(f'[{self.task_name}] {len(asyncio.tasks.all_tasks())} task(s) in the asyncio loop ')

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
        Execute the actual rest api call to dispatch the collectedd output to the relay server

        :param connection:
        :param outputs:
        :return:
        """
        task_name = self.task_name + '_dispatch'
        try:
            log.debug(f'[{task_name}]: try post data using requests')
            response = requests.post(
                connection['url'],
                auth=connection['auth'],
                headers=connection['headers'],
                data=json.dumps(outputs),
                verify=connection['cert'],
            )
            retval = 'success', response
            log.debug(f'[{task_name}]: post succeeded')
            # .. todo:: a success is considered a success if the data on the server side
            #           is ingested into the database, i.e ensure that no data is lost
        except:
            log.error(f'[{task_name}]: post failed')
            retval = 'failed', None

        return retval

    async def dispatch(self, connection=None) -> None:
        """
        Handle collected the buffered outputs and dispatch them to the relay server

        :param connection:
        :return:
        """
        _t0 = time.time()
        task_name = self.task_name + '_dispatch'
        post_timeout = 10  # timeout for the post request

        log.debug(f'[{task_name}] enter dispatch method')

        # pop the collected outputs/results and print them
        t0 = time.time()
        outputs_send = []
        while len(self.outputs) > 0:
            outputs_send.append(self.outputs.pop(0))
        dt = time.time() - t0
        log.debug(f'[{task_name}]: {len(outputs_send)} outputs collected in {dt:5.2f} seconds')

        if len(outputs_send) > 0:
            atask = asyncio.create_task(self._post_data(connection, outputs_send))
            log.debug(f'[{task_name}] async dispatch post outputs to relay server')
            t0 = time.time()
            await asyncio.wait([atask], timeout=post_timeout)
            dt = time.time() - t0
            log.debug(f'[{task_name}]: async task to dispath data finished in {dt:5.2f} seconds')

            if atask.done():
                # handle the sucessful dispatch of the outputs
                status, result = atask.result()
                log.debug(f'[{task_name}] post output = {result}')
                if not result or (result and result.status_code != 200):
                    msg = (
                        f'[{task_name}] either the request that was sent was malformed or \n'
                        f'[{task_name}] something went wrong on the server side (e.g dead server)'
                    )
                    log.error(msg)
                    # .. todo:: in the case of a failiur make sure that the collected data is
                    #           put back into the buffer
            else:
                # dispatch post request timed out
                log.warning(f'[{task_name}] task timed out')
                # .. todo:: in the case of a failiur make sure that the collected data is
                #           put back into the buffer
        else:
            log.debug('no buffered data to be dispatched')
        _dt = time.time() - _t0
        log.debug(f'[{task_name}]: async dispatch method finished in {_dt:5.2f} seconds')
