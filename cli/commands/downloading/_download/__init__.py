import logging
import argparse

from core.models import CandidateGroup

from .config import CONFIG_CHOICES

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
    download_cmd.set_defaults(func=_download_func)
    return


def _download_func(args: argparse.Namespace) -> None:
    print(f"downloading {args.config}!")
