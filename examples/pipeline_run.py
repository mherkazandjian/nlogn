import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from nlogn.pipeline.pipeline import Pipeline
from nlogn.pipeline.task import TaskRenderer

heartbeat_pipeline_path = '../../samples/sample_project/heartbeat.yml'
storage_pipeline_path = '../../samples/sample_project/storage.yml'

pipeline = Pipeline(storage_pipeline_path)
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

scheduler.start()
loop.run_forever()

print('done')

