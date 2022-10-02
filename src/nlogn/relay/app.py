import os

from flask import Flask
from flask import request
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash
from werkzeug.security import check_password_hash

from nlogn import log
from nlogn.conf import Config
from nlogn.database.database import Database

# write the flags passed on the cmd line to the wrapper and passed
# here as env vars
users_auth = os.environ.get('USERS')

assert 'DATABASE' in os.environ
url = os.environ.get('DATABASE')  # localhost:9200'
conf = os.environ.get('CONF', None)

log.info('arguments passed on the cmd line and env vars:')
log.info('----------------------------------------------')
log.info(f'users: {users_auth}')
log.info(f'database: {url}')
log.info(f'conf: {conf}')
log.info('----------------------------------------------')
# .. todo:: write the content of the config file too

config = None
if conf:
    config = Config(os.path.expanduser(conf))

esdb = Database(url=url)

log.info('indices in the database:')
for _index_name in esdb.indices():
    log.info(f'\t{_index_name}')

app = Flask(__name__)
auth = HTTPBasicAuth()

_users_auth_file = os.path.expanduser(users_auth)

users_credentials = {}
if os.path.isfile(_users_auth_file):
    with open(_users_auth_file) as fobj:
        for line in fobj.readlines():
            _username, _password = line.split(':')
            users_credentials[_username.strip()] = generate_password_hash(_password.strip())
users = users_credentials

# registered users
log.info('list of registered users')
for user, pass_hash in users.items():
    log.info(f'\t{user}:{pass_hash}')


@auth.verify_password
def verify_password(username, password):
    if username in users and \
            check_password_hash(users.get(username), password):
        return username


@app.route('/', methods=['GET'])
@auth.login_required
def hello_world():
    return f'Hello, From Flask! for user {auth.current_user()}'


@app.route('/relay_data', methods=['POST'])
@auth.login_required
def relay_data():
    content_type = request.headers.get('Content-Type')
    if content_type == 'application/json':
        data = request.json
        log.debug('got the following json data')
        log.debug(f'\t{data}')

        for item in data:

            job_name = item['name']
            cluster_name = item['cluster']
            if cluster_name:
                index_prefix = f'{cluster_name}@'
            else:
                index_prefix = ''
            index_name = f'{index_prefix}{job_name}'

            if index_name not in esdb.indices():
                # esdb.delete_index(index_name, verbose=True)
                index_mapping = {
                    "timestamp": {
                        "type": "date",
                        "format": "strict_date_optional_time||epoch_millis"
                    },
                    "hostname": {"type": "keyword"}
                }

                for field_name in item['output']:
                    field_type = item['columns'][field_name]
                    index_mapping[field_name] = {"type": field_type}

                # create the empyt index with the mapping
                response = esdb.create_index(name=index_name, mapping=index_mapping)
                log.debug(f'create new index {index_name}')
                log.debug(f'{response}')
                # .. todo:: if this fails with an error the index is created
                #           without a mapping. the index should be deleted
                #           it is empty anyway and this would avoid this if
                #           statement next time around since the index would
                #           be created that is not what we need since next
                #           time data will be put in it and a default mapping
                #           will be set

            document = item['output']
            document['timestamp'] = item['timestamp']
            document['hostname'] = item['hostname']

            ### DEV: WARNING!!!
            #####esdb.delete_index(index_name, verbose=True)
            ### END DEV
            log.debug(f'document to be indexed')
            log.debug(f'\t{document}')
            response = esdb.db.index(index=index_name, document=document)
            log.debug(f'{response}')

        return 'success'
    else:
        msg = 'Content-Type not supported!'
        log.error(msg)
        return msg


if __name__ == '__main__':
    app.run()
    if config:
        config.stop_observer()

