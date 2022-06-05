import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from requests.auth import HTTPBasicAuth

from nlogn.agent.argparser import parse_args
from nlogn.pipeline.pipeline import Pipeline
from nlogn.pipeline.task import TaskRenderer


def main():
    args = parse_args()

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

    pipeline = Pipeline(args.pipelines)
    pipeline.show_specs()

    loop = asyncio.new_event_loop()
    scheduler = AsyncIOScheduler(event_loop=loop)

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
            kwargs={
                'scheduler': scheduler,
            }
        )

        scheduler.add_job(
            pipeline_job.dispatch,
            "interval",
            seconds=pipeline_job.schedule.interval.to('sec').magnitude,
            id=pipeline_job.task_name + '_dispatch',
            max_instances=1,
            kwargs={
                'connection': conn,
            }
        )

    scheduler.start()
    loop.run_forever()


if __name__ == '__main__':
    main()
