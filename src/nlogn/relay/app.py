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

print('indices in the database:')
for _index_name in esdb.indices():
    print(f'\t{_index_name}')

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
        data = request.json
        print('got the following json data')
        print('\t', data)

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
                esdb.create_index(name=index_name, mapping=index_mapping)
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

            #esdb.delete_index(index_name, verbose=True)
            print(esdb.db.index(index=index_name, document=document))

        return 'success'
    else:
        return 'Content-Type not supported!'


if __name__ == '__main__':
    app.run()
