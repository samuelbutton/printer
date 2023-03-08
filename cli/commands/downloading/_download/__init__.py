import logging
import argparse

from .configs.download import CONFIG_CHOICES
from .downloader.build_downloader import build_downloader

__all__ = ["download_cmd"]

_logger = logging.getLogger(__name__)


def download_cmd(parent: argparse._SubParsersAction) -> None:
    download_cmd = parent.add_parser(
        "download",
        help="Download dataset",
        formatter_class=argparse.RawTextHelpFormatter,
        description="download a data set from a source",
    )
    download_cmd.add_argument(
        "--config",
        choices=CONFIG_CHOICES,
        help="the configuration you would like to use for the download",
        required=True,
    )
    download_cmd.add_argument(
        "--debug",
        action="store_true",
        help="sets the logger to level DEBUG for just _download module loggers",
    )

    download_cmd.set_defaults(func=_download_func)
    return


def _download_func(args: argparse.Namespace) -> None:
    if args.debug is True:
        _logger.setLevel(logging.DEBUG)

    downloader = build_downloader(CONFIG_CHOICES[args.config])

    missing_data = downloader.find_missing_data()

    for symbol, missing_dts in missing_data:
        missing_data = downloader.pull_missing_data(symbol, missing_dts)
        downloader.save_to_database(symbol, missing_dts, missing_data)
