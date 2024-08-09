from concurrent.futures import ThreadPoolExecutor, as_completed
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
    tw = TerraWorkspace(workspace_namespace, workspace_name)
    bucket_name = tw.get_bucket_name()
    gs_urls = get_gs_urls(tw, bucket_name)

    storage_client = storage.Client(project=gcp_project_id)
    bucket = storage_client.bucket(bucket_name, user_project=gcp_project_id)

    plan = pd.read_parquet(plan_file)

    # create evenly-sized batches of URLs to delete
    blobs = plan["blobs"].explode().to_frame().rename(columns={"blobs": "blob"})
    blobs["url"] = "gs://" + bucket_name + "/" + blobs["blob"]
    assert ~blobs["url"].isin(gs_urls).any()

    n_blobs = len(blobs)
    max_batch_size = 500
    n_batches = 1 + n_blobs // max_batch_size
    batch_size = ceil(n_blobs / n_batches)

    with ThreadPoolExecutor() as executor:
        futures = []

        for i in range(0, n_batches):
            batch = list(blobs[(i * batch_size) : (i * batch_size + batch_size)])

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

        for _ in as_completed(futures):
            pass


def delete_batch(
    batch: list[str],
    i: int,
    n_batches: int,
    storage_client: storage.Client,
    bucket: storage.Bucket,
) -> None:
    echo(f"Deleting batch {i+1} of {n_batches}")

    with storage_client.batch(raise_exception=False):
        for blob in batch:
            bucket.delete_blob(blob)
