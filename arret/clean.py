import logging
from concurrent.futures import ThreadPoolExecutor, wait
from math import ceil
from pathlib import Path

import pandas as pd
from google.cloud import storage

from arret.terra import TerraWorkspace
from arret.utils import extract_unique_values


def do_clean(
    workspace_namespace: str,
    workspace_name: str,
    plan_file: Path,
    gcp_project_id: str,
    other_workspaces: list[dict[str, str]],
) -> None:
    # get the gs:// URLs referenced in relevant workspaces
    workspaces_to_check = [
        {"workspace_namespace": workspace_namespace, "workspace_name": workspace_name},
        *other_workspaces,
    ]

    gs_urls = set().union(
        *[
            get_gs_urls(x["workspace_namespace"], x["workspace_name"])
            for x in workspaces_to_check
        ]
    )

    # read in the cleanup plan
    plan = pd.read_parquet(plan_file)

    # prevent deleting files in any of the workspaces
    plan["in_data_table"] = plan["gs_url"].isin(list(gs_urls))

    plan["to_delete"] = (
        ~plan["in_data_table"]
        & ~plan["force_keep"]
        & (plan["is_pipeline_logs"] | plan["is_old"] | plan["is_large"])
    )

    to_delete = plan.loc[
        plan["to_delete"], ["name", "updated", "size", "gs_url"]
    ].sort_values("size", ascending=False)

    # create evenly-sized batches of URLs to delete (there's a max of 1000 operations
    # for a `storage.Client` batch context so do this outer layer of batching, too)
    n_blobs = len(to_delete)
    max_batch_size = 1000
    n_batches = 1 + n_blobs // max_batch_size
    batch_size = ceil(n_blobs / n_batches)
    logging.info(
        f"Deleting {n_blobs} blobs in {n_batches} batches of {batch_size} each"
    )

    terra_workspace = TerraWorkspace(workspace_namespace, workspace_name)
    bucket_name = terra_workspace.get_bucket_name()
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
                    bucket=bucket,  # pyright: ignore
                )
            )

        wait(futures)


def get_gs_urls(workspace_namespace: str, workspace_name: str) -> set[str]:
    logging.info(
        f"Getting GCS URLs referenced in {workspace_namespace}/{workspace_name} data tables"
    )
    terra_workspace = TerraWorkspace(workspace_namespace, workspace_name)

    entity_types = terra_workspace.get_entity_types()
    entities = {k: terra_workspace.get_entities(k) for k in entity_types}

    gs_urls = set()

    for _, df in entities.items():
        unique_values = extract_unique_values(df)

        gs_urls.update(
            {x for x in unique_values if isinstance(x, str) and x.startswith(f"gs://")}
        )

    return gs_urls


def delete_batch(
    batch: list[str],
    i: int,
    n_batches: int,
    storage_client: storage.Client,
    bucket=storage.Bucket,
) -> None:
    logging.info(f"Deleting batch {i+1} of {n_batches}")

    with storage_client.batch(raise_exception=False):
        for blob in batch:
            bucket.delete_blob(blob_name=blob)  # pyright: ignore
