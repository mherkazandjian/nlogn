import os

from flask import Flask
from flask import request
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash
from werkzeug.security import check_password_hash

from nlogn.database.database import Database

assert 'DATABASE' in os.environ
url = os.environ.get('DATABASE')  # localhost:9200'
esdb = Database(url=url)

pipelines = {
    'pipeline_1': {
        'mapping': {
            "timestamp": {
                "type": "date",
                "format": "strict_date_optional_time||epoch_millis"
            },
            "host": {"type": "keyword"},
            "duration": {"type": "float"},
            "pct_cpu": {"type": "float"}
        }
    }
}

cluster_name = 'my_cluster'
index_prefix = f'{cluster_name}@'
for pipeline_name in pipelines:
    index_name = f'{index_prefix}{pipeline_name}'
    mapping = pipelines[pipeline_name]['mapping']
    esdb.delete_index(index_name, verbose=True)
    esdb.create_index(
        name=index_name,
        mapping=mapping
    )

print('indices in the database:')
for index_name in esdb.indices():
    print(f'\t{index_name}')

app = Flask(__name__)
auth = HTTPBasicAuth()

users_auth = os.environ.get('USERS')
_users_auth_file = os.path.expanduser(users_auth)

users_credentials = {}
if os.path.isfile(_users_auth_file):
    with open(_users_auth_file) as fobj:
        for line in fobj.readlines():
            _username, _password = line.split(':')
            users_credentials[_username.strip()] = generate_password_hash(_password.strip())
users = users_credentials

# registered users
print('list of registered users')
for user, pass_hash in users.items():
    print(f'\t{user}:{pass_hash}')


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
        json = request.json
        print('got the following json data')
        print('\t', json)

        data = json
        index_prefix = f'{cluster_name}@'
        pipeline_name = list(data.keys())[0]
        record = data[pipeline_name]
        index_name = f'{index_prefix}{pipeline_name}'
        # .. todo:: implement a safe ingest method similar to ingest_dataframe
        # in the Database class
        print(esdb.db.index(index=index_name, document=record))

        # add the receive timestamp
        # right before ingestion add the ingest time (it seems it can be
        # done at the elsticsearch level?? check it out
        return json
    else:
        return 'Content-Type not supported!'


if __name__ == '__main__':
    app.run()
