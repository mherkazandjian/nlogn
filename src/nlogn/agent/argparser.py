import argparse
from argparse import RawTextHelpFormatter


def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "my description\n"
            "goes here\n"
            "\n"
            "usage example\n"
            "\n"
            "   $ bla bla 3 --foo=1  "
        ),
        formatter_class=RawTextHelpFormatter
    )

    parser.add_argument(
        "-r",
        "--relay-host",
        type=str,
        default=None,
        dest="relay_host",
        help='the url to the relay host'
    )

    parser.add_argument(
        "--trust-cert",
        type=str,
        default=None,
        dest="trusted_certificate",
        help='the certificate of the relay host that is cosidered trusted'
    )

    parser.add_argument(
        "-c",
        "--conf",
        type=str,
        default=None,
        dest="config",
        help='the path to the configuration file'
    )

    parser.add_argument(
        "--user",
        type=str,
        default=None,
        dest="username",
        help='the username to authenticate on the relay'
    )

    parser.add_argument(
        "--pass",
        type=str,
        default=None,
        dest="password",
        help='the password to be used for the autnetication on the relay'
    )

    # a count option. the attribute verbosity is set to the number of times this
    # option is specified in the command line.
    parser.add_argument(
        "-v",
        "--verbosity",
        action='count',
        default=0
    )

    return parser.parse_args()
