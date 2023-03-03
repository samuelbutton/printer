# System imports
import argparse
import logging
from typing import Callable, List

from .commands import some_command

_logger = logging.getLogger(__name__)

VERSION = "1.0.0"
PROGRAM_DESCRIPTION = """
A CLI for backtesting.
"""
USAGE = "tbd"


class CLI:
    SUBCOMMANDS: List[Callable[argparse._SubParsersAction], None] = []

    def __init__(self) -> None:
        self.parser = self._create_parser()

    def _create_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(usage=USAGE, description=description)
        parser.add_argument("-v", "--version", action="version", version=VERSION)

        subparser = parser.add_subparsers(
            title="sub-commands", metavar="", dest="sub_cmd"
        )

        for subcommand in self.SUBCOMMANDS:
            subcommand(subparser)
        return parser

    def parse_args(self, args: List[str]) -> None:
        parsed_args = self.parser.parse_args(args)
        parsed_args.func(parsed_args)
        return


def run(args: List[str]) -> None:
    cli = CLI()
    try:
        cli.parse_args(args)
    except Exception as e:
        _logger.exception(e)
        raise e
    return
