from enum import Enum

from ..configs.download import DownloadConfig, DownloaderEnum
from .downloader import Downloader
from .prices import PricesDownloader


def build_downloader(cfg: DownloadConfig) -> Downloader:
    if cfg.downloader_enum == DownloaderEnum.PricesDownloader:
        return PricesDownloader(cfg)
    raise NotImplementedError
