import pandas as pd

from weather_catalog.data import DataCube
from weather_catalog.query import PointDateRangeQuery, Query


class QueryResolver:

    def resolve(self, query: Query, data: DataCube) -> pd.DataFrame:

        # if not isinstance(query, PointDateRangeQuery):
        #     raise ValueError(f"Query type {type(query)} not supported, current MVP only supports PointDateRangeQuery")

        return self._resolve_point_daterange_query(query, data)  # type: ignore

    def _resolve_point_daterange_query(self, query: PointDateRangeQuery, data: DataCube) -> pd.DataFrame:

        # TODO: add middleman handling converting the lat/lon and time inputs into indicies for the data cube

        return data.get_data(
            latitude=query.location.latitude,
            longitude=query.location.longitude,
            start_date=query.start_date,
            end_date=query.end_date,
            variables=[query.variable],
        )
