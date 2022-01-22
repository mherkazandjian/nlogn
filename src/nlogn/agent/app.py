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


    def job1(stats=None, connection=None, *args, **kwargs):
        # if the previous job has completed (if applicable)
        # then collect the data by e.g executing the command
        # otherwise skip and wait for next execution
        #  - if a stuck sitation is detected, log an alarm
        #
        stats['a'] += 1
        print(f'job1 {stats["a"]}')
        # job1_count += 1
        # print(job1_count)
        data = {
            'hostname': 'my_host',
            't_start': 'YYYY:MM:DD-HH:mm:ss',  # utc time
            't_end': 'YYYY:MM:DD-HH:mm:ss',  # utc time
            'fs_mount': '/',
            'bytes_total': 11111111,
            'types_free': 22222222
        }

    stats_1 = {}
    stats_1['a'] = 0
    schedule.every(2).seconds.do(job1, stats=stats_1, connection=conn)


    def job2(stats=None, connection=None, *args, **kwargs):
        stats['a'] += 1
        pct_cpu = psutil.cpu_percent()
        print(f'job2 {stats["a"]}')

        data = {
            'hostname': 'my_host',
            't_start': 'YYYY:MM:DD-HH:mm:ss',  # utc time
            't_end': 'YYYY:MM:DD-HH:mm:ss',  # utc time
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
