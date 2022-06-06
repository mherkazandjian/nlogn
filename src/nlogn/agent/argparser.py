import argparse
from argparse import RawTextHelpFormatter


def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Agent that runs the pipelines for collecting data"
            "\n"
            "Usage example\n"
            "\n"
            "   $ nlogn-agent --relay-host=https://localhost:18080 --user=john --pass=foosecret --trust-cert=/path/to/public/cert.pem --pipelines=/path/to/pipelines/pipelines.yml\n"
        ),
        formatter_class=RawTextHelpFormatter
    )

    parser.add_argument(
        "--relay-host",
        type=str,
        default=None,
        dest="relay_host",
        help='the url to the relay host'
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

    parser.add_argument(
        "--trust-cert",
        type=str,
        default=None,
        dest="trusted_certificate",
        help='the certificate of the relay host that is cosidered trusted'
    )

    parser.add_argument(
        "--pipelines",
        type=str,
        default=None,
        dest="pipelines",
        help="the path to the configuration file that defines the pipelines"
    )

    parser.add_argument(
        "--cluster",
        type=str,
        default='',
        dest="cluster",
        help="the name of the cluster to which this host belongs"
    )

    return parser.parse_args()
