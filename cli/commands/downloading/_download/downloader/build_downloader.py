from enum import Enum

from ..configs.download import DownloadConfig, DownloaderEnum
from .downloader import Downloader
from .prices import PricesDownloader

from .profiles import ProfilesDownloader


def build_downloader(cfg: DownloadConfig) -> Downloader:
    if cfg.downloader_enum == DownloaderEnum.PricesDownloader:
        return PricesDownloader(cfg)
    elif cfg.downloader_enum == DownloaderEnum.ProfilesDownloader:
        return ProfilesDownloader(cfg)
    raise NotImplementedError
