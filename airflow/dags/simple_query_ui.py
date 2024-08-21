from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import ParamsDict
from weather_catalog.catalog.catalog_selector import CatalogSelectorSingleton
from weather_catalog.query.point_daterange_query import PointDateRangeQuery
from weather_catalog.query_resolution.query_resolver import QueryResolver

##
with DAG(
    dag_id=Path(__file__).stem,
    dag_display_name="Query UI",
    description="tmp",
    doc_md=__doc__,
    schedule=None,
    catchup=False,
    tags=["query", "ui"],
    params=PointDateRangeQuery.create_params(),
) as dag:

    @task(task_id="create_query_object", task_display_name="Create query object")
    def create_query_object(params: ParamsDict) -> dict:
        query = PointDateRangeQuery.from_params(params)
        return query.model_dump(mode="json")

    @task(task_id="determine_catalog", task_display_name="Determine catalog")
    def determine_catalog(query: dict) -> str:
        query = PointDateRangeQuery(**query)
        catalog = CatalogSelectorSingleton.select_catalog(query)
        return catalog.catalog_id

    @task(task_id="get_data_source", task_display_name="Get data source")
    def get_data_source_and_resolve(catalog_id: str, query: dict) -> str:
        query = PointDateRangeQuery(**query)
        catalog = CatalogSelectorSingleton.get_catalog_by_id(catalog_id)
        data = catalog.get_data(query)
        resolved = QueryResolver().resolve(query, data)
        return resolved

    query = create_query_object()  # type: ignore

    catalog_id = determine_catalog(query=query)  # type: ignore

    resolved_data = get_data_source_and_resolve(query=query, catalog_id=catalog_id)  # type: ignore
