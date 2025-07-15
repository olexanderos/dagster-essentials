import os
from datetime import datetime, timedelta

import dagster as dg
import pandas as pd
import requests
from dagster_duckdb import DuckDBResource

from dagster_essentials.assets import constants
from dagster_essentials.partitions import monthly_partition, weekly_partition


@dg.asset(partitions_def=monthly_partition)
def taxi_trips_file(context: dg.AssetExecutionContext) -> None:
    """
    The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:7]
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(
        constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb"
    ) as output_file:
        output_file.write(raw_trips.content)


@dg.asset(deps=["taxi_trips_file"], partitions_def=monthly_partition)
def taxi_trips(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
    """
    The raw taxi trips dataset, loaded into a DuckDB database
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:7]
    query = f"""
        create or replace table trips as (
          select
            VendorID as vendor_id,
            PULocationID as pickup_zone_id,
            DOLocationID as dropoff_zone_id,
            RatecodeID as rate_code_id,
            payment_type as payment_type,
            tpep_dropoff_datetime as dropoff_datetime,
            tpep_pickup_datetime as pickup_datetime,
            trip_distance as trip_distance,
            passenger_count as passenger_count,
            total_amount as total_amount
          from '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}'
        );
    """

    with database.get_connection() as conn:
        conn.execute(query)


@dg.asset(deps=["taxi_trips"], partitions_def=weekly_partition)
def trips_by_week(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
    """
    The number of trips by week, loaded into a DuckDB database
    """
    period_to_fetch = context.partition_key
    result = pd.DataFrame()

    # get all trips for the week
    query = f"""
        select
            vendor_id, total_amount, trip_distance, passenger_count
        from trips
        where pickup_datetime >= '{period_to_fetch}'
            and pickup_datetime < '{period_to_fetch}'::date + interval '1 week'
    """

    with database.get_connection() as conn:
        data_for_week = conn.execute(query).fetch_df()

    aggregate = (
        data_for_week.agg(
            {
                "vendor_id": "count",
                "total_amount": "sum",
                "trip_distance": "sum",
                "passenger_count": "sum",
            }
        )
        .rename({"vendor_id": "num_trips"})
        .to_frame()
        .T
    )

    # clean up the formatting of the dataframe
    aggregate["period"] = period_to_fetch
    aggregate["num_trips"] = aggregate["num_trips"].astype(int)
    aggregate["passenger_count"] = aggregate["passenger_count"].astype(int)
    aggregate["total_amount"] = aggregate["total_amount"].round(2).astype(float)
    aggregate["trip_distance"] = aggregate["trip_distance"].round(2).astype(float)
    aggregate = aggregate[
        ["period", "num_trips", "total_amount", "trip_distance", "passenger_count"]
    ]
    try:
        existing = pd.read_csv(constants.TRIPS_BY_WEEK_FILE_PATH)
        existing = existing[existing["period"] != period_to_fetch]
        result = pd.concat([existing, aggregate])
        existing.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)
    except FileNotFoundError:
        aggregate.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)


@dg.asset
def taxi_zones_file() -> None:
    """
    The raw CSV file for the taxi zones dataset. Sourced from the NYC Open Data portal.
    """
    raw_zones = requests.get(
        "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
    )

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_zones.content)


@dg.asset(deps=["taxi_zones_file"])
def taxi_zones(database: DuckDBResource) -> None:
    """
    The raw taxi zones dataset, loaded into a DuckDB database
    """
    query = f"""
        create or replace table zones as (
            select
                LocationID as zone_id,
                zone,
                borough,
                the_geom as geometry
          from '{constants.TAXI_ZONES_FILE_PATH}'
        )
    """
    with database.get_connection() as conn:
        conn.execute(query)
