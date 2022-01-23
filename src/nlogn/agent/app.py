import sys
import time
import json
import time

import warnings
warnings.filterwarnings('ignore', message='Certificate for localhost has no')

import requests
from requests.auth import HTTPBasicAuth
import schedule
from schedule import repeat
import psutil
import datetime
import dateutil
from dateutil.tz import tzutc
from nlogn.agent.argparser import parse_args




if __name__ == '__main__':

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

    def pipeline_1(stats=None, connection=None, *args, **kwargs):
        stats['a'] += 1
        pct_cpu = psutil.cpu_percent()
        print(f'job2 {stats["a"]}')

        t = datetime.datetime.utcnow()
        timestamp = t.strftime('%Y-%m-%dT%H:%M:%S')
        print(timestamp)

        # the last job in a pipeline publishes the data
        # the data is a dict with the key value is the job name
        # in the case of the last job in the pipeline the name of the
        # key is the name of the pipeline.
        # a pipeline with a single job in that case that job is the pipeline
        # the last job in the pipeline is by default called the same as the
        # pipeline name
        data = {
            'pipeline_1': {
                'timestamp': timestamp,
                'host': 'my_host',
                'duration': 0.1,
                'pct_cpu': pct_cpu
            }
        }

        requests.post(
            connection['url'],
            auth=connection['auth'],
            headers=connection['headers'],
            data=json.dumps(data),
            verify=connection['cert']
        )

        stats['a'] += 1
        print(stats['a'])

    stats_2 = {}
    stats_2['a'] = 0
    schedule.every(2).seconds.do(pipeline_1, stats=stats_2, connection=conn)

    while True:
        schedule.run_pending()
        time.sleep(1)
        print('done')
