import argparse
from argparse import RawTextHelpFormatter

from .parser import PipelinePrser


def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Utility script for parsing and checking pipelines\n"
            "\n"
            "Usage example\n"
            "\n"
            "   $ nlogn-pipeline /path/to/pipeline.yml --parse --run --verbose\n"
            "   $ nlogn-pipeline /path/to/pipeline/dir"
        ),
        formatter_class=RawTextHelpFormatter
    )

    parser.add_argument(
        "--parse",
        default=0,
        dest="parse",
        action="count",
        help="if present shows the parsed pipeline"
    )

    parser.add_argument(
        "--run",
        default=0,
        dest="run",
        action="count",
        help="if present perform a local pipeline run"
    )

    parser.add_argument(
        "--verbose",
        default=0,
        dest="verbose",
        action="count",
        help="if present run in verbose mode"
    )

    parser.add_argument(
        "pipelines",
        type=str,
        default=None,
        help="the path to the pipeline yml file or pipeline dir"
    )

    return parser.parse_args()


def main():
    args = parse_args()
    parser = PipelinePrser()


if __name__ == '__main__':
    main()