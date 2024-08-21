from weather_catalog.query import Query

from .abstract_catalog import AbstractCatalog
from .all_catalogs import all_catalogs


class CatalogSelectorClass:
    """
    Class to select a catalog based on a query
    """

    def __init__(self, catalogs: list[AbstractCatalog]):
        self.catalogs = catalogs

    def select_catalog(self, query: Query) -> AbstractCatalog:
        return self.catalogs[0]  # for testing

    def get_catalog_by_id(self, catalog_id: str) -> AbstractCatalog:
        for catalog in self.catalogs:
            if catalog.catalog_id == catalog_id:
                return catalog
        raise ValueError(f"Catalog with id {catalog_id} not found")


CatalogSelectorSingleton = CatalogSelectorClass(catalogs=all_catalogs)
