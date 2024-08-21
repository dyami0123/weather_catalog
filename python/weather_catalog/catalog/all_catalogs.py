import os

from .abstract_catalog import AbstractCatalog
from .local_catalog.local_catalog import LocalCatalog

all_catalogs: list[AbstractCatalog] = [
    LocalCatalog(
        base_path=os.path.join(os.path.dirname(__file__), "local_catalog_data")
    )
]
