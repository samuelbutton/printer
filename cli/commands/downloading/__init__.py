import argparse

from ._download import download_cmd


def downloading_cmd(parent: argparse._SubParsersAction) -> None:
    downloading_parser = parent.add_parser(
        "downloading",
        help="Command for downloading data",
    )
    downloading_subparser = downloading_parser.add_subparsers(
        title="downloading-sub-commands",
        metavar="",
        dest="downloading_sub_cmd",
    )

    cmds = [
        download_cmd,
    ]

    for cmd in cmds:
        cmd(downloading_subparser)
