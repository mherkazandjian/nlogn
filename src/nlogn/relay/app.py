import os
import sys

from flask import Flask
from flask import request
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash
from werkzeug.security import check_password_hash

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
        return json
    else:
        return 'Content-Type not supported!'


if __name__ == '__main__':
    app.run()
