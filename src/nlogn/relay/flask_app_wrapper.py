import os
from subprocess import Popen
import shlex

from .flask_app_wrapper_utils import parse_args


def main():
    args = parse_args()

    cmd = (
        f'flask run '
        f'--host={args.host} '
        f'--port={args.port} '
        f'--cert={args.cert} '
        f'--key={args.key} '
    )

    env = os.environ.copy()
    env['USERS'] = args.users
    env['DATABASE'] = args.database

    process = Popen(
        shlex.split(cmd),
        env=env
    )
    process.wait()


if __name__ == '__main__':
    main()