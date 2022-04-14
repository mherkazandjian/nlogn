import argparse
from argparse import RawTextHelpFormatter


def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Wrapper script that runs the relay flask app\n"
            "\n"
            "Usage example\n"
            "\n"
            "   $ nlogn-relay --users=/path/to/users.txt --database=foo.example.com:9200 --cert=/path/to/public/cert.pem --key=/path/to/private/key.pem"
        ),
        formatter_class=RawTextHelpFormatter
    )

    parser.add_argument(
        "--host",
        type=str,
        default=None,
        dest="host",
        help="the hostname to which the listening socket the flask app will run"
    )

    parser.add_argument(
        "--port",
        type=str,
        default=None,
        dest="port",
        help="the port on which the listening socket the flask app will run"
    )

    parser.add_argument(
        "--users",
        type=str,
        default=None,
        dest="users",
        help="path to the file that has the user credentials"
    )

    parser.add_argument(
        "--database",
        type=str,
        default=None,
        dest="database",
        help="the hostname:port of the database"
    )

    parser.add_argument(
        "--cert",
        type=str,
        default=None,
        dest="cert",
        help="path the public certificate file"
    )

    parser.add_argument(
        "--key",
        type=str,
        default=None,
        dest="key",
        help="path to the private key file"
    )

    parser.add_argument(
        "-v",
        "--verbosity",
        action='count',
        default=0
    )

    return parser.parse_args()
