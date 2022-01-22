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

    def job2(stats=None, connection=None, *args, **kwargs):
        stats['a'] += 1
        pct_cpu = psutil.cpu_percent()
        print(f'job2 {stats["a"]}')

        t = datetime.datetime.utcnow()
        timestamp = t.strftime('%Y-%m-%dT%H:%M:%S')
        print(timestamp)

        data = {
            'job_name': 'job2',
            'pipeline': 'pipelinex',
            'hostname': 'my_host',
            't_start': timestamp,
            't_end': timestamp,
            'pct_cpu': pct_cpu
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
    schedule.every(2).seconds.do(job2, stats=stats_2, connection=conn)

    while True:
        schedule.run_pending()
        time.sleep(1)
        print('done')
