import dagster as dg

from dagster_essentials.partitions import monthly_partition, weekly_partition

trips_by_week = dg.AssetSelection.assets("trips_by_week")

trip_update_job = dg.define_asset_job(
    name="trip_update_job",
    partitions_def=monthly_partition,
    # We’ll isolate this specifically because we won’t want to
    # run it with the rest of our pipeline and it should be run more frequently.
    selection=dg.AssetSelection.all() - trips_by_week,
)

weekly_update_job = dg.define_asset_job(
    name="weekly_update_job",
    partitions_def=weekly_partition,
    selection=trips_by_week,
)
