import os
from subprocess import Popen
import shlex

from nlogn import relay
from nlogn.relay.flask_app_wrapper_utils import parse_args

# .. todo:: use app.run(**flask_only_args) instead of running it as a
# subprocess

def main():
    args = parse_args()

    cmd = (
        f'flask run '
        f'--host={args.host} '
        f'--port={args.port} '
        f'--cert={args.cert} '
        f'--key={args.key} '
    )

    print(f'>>> {cmd}')

    env = os.environ.copy()
    env['USERS'] = args.users
    env['DATABASE'] = args.database
    env['CONF'] = args.conf
    env['FLASK_APP'] = os.path.join(os.path.dirname(relay.__file__), 'app.py')

    process = Popen(
        shlex.split(cmd),
        env=env
    )
    process.wait()


if __name__ == '__main__':
    main()
