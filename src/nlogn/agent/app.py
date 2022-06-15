import os
import logging
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from requests.auth import HTTPBasicAuth

from nlogn.conf import Config
from nlogn.agent.argparser import parse_args
from nlogn.pipeline.pipeline import Pipeline
from nlogn.pipeline.task import TaskRenderer


def main():
    args = parse_args()

    conf = None
    if args.conf:
        conf = Config(os.path.expanduser(args.conf))

    url = f'{args.relay_host}/relay_data'
    auth = HTTPBasicAuth(args.username, args.password)

    headers = {
        'Content-type': 'application/json',
        'Accept': 'text/plain',
    }

    conn = {
        'url': url,
        'auth': auth,
        'headers': headers,
        'cert': args.trusted_certificate
    }

    #logging.getLogger('apscheduler').setLevel(logging.DEBUG)
    loop = asyncio.new_event_loop()
    scheduler = AsyncIOScheduler(event_loop=loop)
    misfire_grace_time = 30  # .. todo:: put this in the conf file

    for pipeline_path in args.pipelines.split(','):
        pipeline = Pipeline(pipeline_path)
        pipeline.show_specs()

        complete_tasks = [task for task in pipeline.tasks_specs if not task.startswith('.')]
        for task in complete_tasks:
            task_renderer = TaskRenderer(name=task, pipeline=pipeline)
            task_renderer.summary()
            pipeline_job = task_renderer.create_job()

            scheduler.add_job(
                pipeline_job.run,
                "interval",
                seconds=pipeline_job.schedule.interval.to('sec').magnitude,
                id=pipeline_job.task_name,
                max_instances=pipeline_job.timeout.max_attempts,
                misfire_grace_time=misfire_grace_time,
                kwargs={
                    'scheduler': scheduler,
                    'cluster': args.cluster
                }
            )

            # .. todo:: probably to avoid congestion only one such job can be used to
            #           dispatch all the collected data to the relay instead of doing
            #           it like this and having one dispatch job per pipeline task
            scheduler.add_job(
                pipeline_job.dispatch,
                "interval",
                seconds=pipeline_job.schedule.interval.to('sec').magnitude,
                id=pipeline_job.task_name + '_dispatch',
                max_instances=1,
                misfire_grace_time=misfire_grace_time,
                kwargs={
                    'connection': conn,
                }
            )

    scheduler.start()
    loop.run_forever()

    if conf:
        conf.stop_observer()



if __name__ == '__main__':
    main()
