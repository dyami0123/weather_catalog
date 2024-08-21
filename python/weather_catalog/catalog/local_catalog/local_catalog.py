from weather_catalog.catalog.abstract_catalog import AbstractCatalog
from weather_catalog.query import Query

from .local_data_downloader import LocalDataDownloader
from .local_data_uploader import LocalDataUploader


class LocalCatalog(AbstractCatalog):
    """A Catalog for local data

    Used mainly for testing and development

    """

    catalog_id: str = "local"

    downloader: LocalDataDownloader
    uploader: LocalDataUploader

    def __init__(self, base_path: str):
        downloader = LocalDataDownloader(base_path=base_path)
        uploader = LocalDataUploader(base_path=base_path)
        super().__init__(downloader=downloader, uploader=uploader)  # type: ignore

    def can_source(self, query: Query) -> bool:
        return True
