from concurrent.futures import ThreadPoolExecutor, wait
from math import ceil
from pathlib import Path

import pandas as pd
from click import echo
from google.cloud import storage

from arret.plan import get_gs_urls
from arret.terra import TerraWorkspace


def do_clean(
    workspace_namespace: str, workspace_name: str, plan_file: Path, gcp_project_id: str
) -> None:
    # get the referenced gs:// URLs again in case anything has changed
    tw = TerraWorkspace(workspace_namespace, workspace_name)
    bucket_name = tw.get_bucket_name()
    gs_urls = get_gs_urls(tw, bucket_name)

    # read in the cleanup plan
    plan = pd.read_parquet(plan_file)

    plan["in_data_table"] = plan["gs_url"].isin(gs_urls)
    plan["to_delete"] = plan["to_delete"] & ~plan["in_data_table"]

    to_delete = plan.loc[
        plan["to_delete"] & plan["is_old"],
        ["name", "updated", "size", "gs_url"],
    ].sort_values("size", ascending=False)

    # create evenly-sized batches of URLs to delete (there's a max of 1000 operations
    # for a `storage.Client` batch context so do this outer layer of batching, too)
    n_blobs = len(to_delete)
    max_batch_size = 1000
    n_batches = 1 + n_blobs // max_batch_size
    batch_size = ceil(n_blobs / n_batches)
    echo(f"Deleting {n_blobs} blobs in {n_batches} batches of {batch_size} each")

    storage_client = storage.Client(project=gcp_project_id)
    bucket = storage_client.bucket(bucket_name, user_project=gcp_project_id)

    with ThreadPoolExecutor() as executor:
        futures = []

        for i in range(0, n_batches):
            batch = list(
                to_delete["name"].iloc[(i * batch_size) : (i * batch_size + batch_size)]
            )

            futures.append(
                executor.submit(
                    delete_batch,
                    batch=batch,
                    i=i,
                    n_batches=n_batches,
                    storage_client=storage_client,
                    bucket=bucket,
                )
            )

        wait(futures)


def delete_batch(
    batch: list[str],
    i: int,
    n_batches: int,
    storage_client: storage.Client,
    bucket=storage.Bucket,
) -> None:
    echo(f"Deleting batch {i+1} of {n_batches}")

    with storage_client.batch(raise_exception=False):
        for blob in batch:
            bucket.delete_blob(blob)
