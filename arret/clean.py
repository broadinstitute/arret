import logging
import random
from math import ceil
from time import sleep

import duckdb
import pandas as pd
import psutil
from google.cloud import storage

from arret.terra import TerraWorkspace
from arret.utils import BoundedThreadPoolExecutor, collect_gs_urls, human_readable_size


def do_clean(
    workspace_namespace: str,
    workspace_name: str,
    plan_path: str,
    gcp_project_id: str,
    other_workspaces: list[dict[str, str]],
    to_delete_sql: str,
) -> None:
    """
    Clean the Terra workspace's bucket using previously generated plan database.

    :param workspace_namespace: the namespace of the Terra workspace
    :param workspace_name: the name of the Terra workspace
    :param plan_path: path to existing plan .duckdb file
    :param gcp_project_id: a GCP project ID
    :param other_workspaces: a list of dictionaries containing workspace namespaces and
    names for other Terra workspaces to check for blob usage
    :param to_delete_sql: the SQL string to use for assigning `to_delete`
    """

    n_cpus = psutil.cpu_count()

    if n_cpus is None:
        n_cpus = 1

    # get the gs:// URLs referenced in relevant workspaces
    workspaces_to_check = [
        {"workspace_namespace": workspace_namespace, "workspace_name": workspace_name},
        *other_workspaces,
    ]

    gs_urls = pd.concat(
        [
            get_gs_urls(
                workspace_namespace=x["workspace_namespace"],
                workspace_name=x["workspace_name"],
                n_workers=min(n_cpus, 10),
            )
            for x in workspaces_to_check
        ]
    )

    # apply deletion logic
    with duckdb.connect(plan_path) as db:
        apply_delete_logic(db, gs_urls, to_delete_sql)
        to_delete_rows = db.table("blobs").filter("to_delete")
        blobs_to_delete = to_delete_rows["name"].fetchdf()["name"].tolist()
        delete_size = to_delete_rows.sum("size").fetchone()[0]  # pyright: ignore

    if len(blobs_to_delete) == 0:
        logging.info("No blobs to delete")
        return

    # make a GCS client and get the bucket we're deleting from
    terra_workspace = TerraWorkspace(workspace_namespace, workspace_name)
    bucket_name = terra_workspace.get_bucket_name()
    storage_client = storage.Client(project=gcp_project_id)
    bucket = storage_client.bucket(bucket_name, user_project=gcp_project_id)

    # create evenly-sized batches of URLs to delete (there's a max of 1000 operations
    # for a `storage.Client` batch context so do this outer layer of batching, too)
    n_blobs = len(blobs_to_delete)
    max_batch_size = 1000
    n_batches = 1 + n_blobs // max_batch_size
    batch_size = ceil(n_blobs / n_batches)
    logging.info(
        f"Deleting {n_blobs} blobs totaling {human_readable_size(delete_size)} "
        f"in {n_batches} batches of {batch_size} each"
    )

    # delete batches of blobs
    with BoundedThreadPoolExecutor(queue_size=n_cpus * 2) as executor:
        for i in range(0, n_batches):
            batch = list(
                blobs_to_delete[(i * batch_size) : (i * batch_size + batch_size)]
            )

            executor.submit(
                delete_batch,
                batch=batch,
                i=i,
                n_batches=n_batches,
                storage_client=storage_client,
                bucket=bucket,  # pyright: ignore
            )

            sleep(random.uniform(0.01, 0.03))  # calm the thundering herd


def get_gs_urls(
    workspace_namespace: str, workspace_name: str, n_workers: int
) -> pd.DataFrame:
    """
    Retrieve Google Cloud Storage (gs://) URLs from data tables in a Terra workspace.

    :param workspace_namespace: namespace of the Terra workspace
    :param workspace_name: name of the Terra workspace
    :param n_workers: number of worker threads
    :return: a data frame of unique URLs the workspace's data tables
    """

    logging.info(
        f"Getting GCS URLs referenced in {workspace_namespace}/{workspace_name} "
        "data tables"
    )
    terra_workspace = TerraWorkspace(workspace_namespace, workspace_name)
    entity_types = terra_workspace.get_entity_types()

    # collect entity data frames in threads
    with BoundedThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = {
            k: executor.submit(terra_workspace.get_entities, entity_type=k)
            for k in entity_types
        }

    # collect gs:// URLs across returned data frames
    gs_urls_dfs = []

    for k, f in futures.items():
        df = collect_gs_urls(f.result())
        df["data_table"] = k
        gs_urls_dfs.append(df)

    gs_urls = pd.concat(gs_urls_dfs)
    gs_urls["workspace_namespace"] = workspace_namespace
    gs_urls["workspace_name"] = workspace_name

    return gs_urls


def apply_delete_logic(
    db: duckdb.DuckDBPyConnection, gs_urls: pd.DataFrame, to_delete_sql: str
) -> None:
    """
    Apply deletion logic to a plan data frame: Delete a blob if any of the following is
    true (for default logic):

        - blob is old (based on `days_considered_old`)
        - blob is large (based on `bytes_considered_large`)
        - blob is inside a `/pipelines-logs/` "folder"

    ...except when:
        - blob is referenced in a Terra data table in the workspace of interest or any
        of the `other_workspaces`

    :param db: the DuckDB database
    :param gs_urls: a data frame of unique URLs the workspace's data tables
    :param to_delete_sql: the SQL string to use for assigning `to_delete`
    """

    # register set of data table gs:// URLs as a DuckDB table
    db.register("gs_urls", gs_urls)

    # log the top 10 columns by total size of their referenced blobs
    top10 = db.sql("""
       SELECT
           gs_urls.workspace_namespace || '/' || gs_urls.workspace_name AS workspace,
           gs_urls.data_table AS data_table,
           gs_urls.col AS col,
           SUM(blobs.size) AS total_size
       FROM
           gs_urls
       INNER JOIN
           blobs
       ON
           gs_urls.url = blobs.url
       GROUP BY
           workspace,
           data_table,
           col
       ORDER BY
           total_size DESC
       LIMIT 10
       ;
   """).df()

    top10["total_size"] = top10["total_size"].apply(human_readable_size)

    logging.info("\n".join(["Top columns by total size:", top10.to_string()]))

    db.sql("""
        UPDATE
            blobs
        SET
            in_data_table = url IN (SELECT url FROM gs_urls);
    """)

    db.sql(f"""
        UPDATE
            blobs
        SET
            to_delete = ({to_delete_sql}) AND NOT in_data_table;
    """)

    # confirm we're not deleting any active blobs
    assert (
        db.sql(  # pyright: ignore
            "SELECT COUNT(*) AS n FROM blobs WHERE in_data_table AND to_delete"
        ).fetchone()[0]
        == 0
    )


def delete_batch(
    batch: list[str],
    i: int,
    n_batches: int,
    storage_client: storage.Client,
    bucket=storage.Bucket,
) -> None:
    """
    Delete a batch of blobs from a GCS bucket.

    :param batch: list of blob names to delete
    :param i: current batch index
    :param n_batches: total number of batches
    :param storage_client: GCS client for storage operations
    :param bucket: bucket containing the blobs
    """

    logging.info(f"Deleting batch {i + 1} of {n_batches}")

    with storage_client.batch(raise_exception=False):
        for blob in batch:
            try:
                bucket.delete_blob(blob_name=blob)  # pyright: ignore
            except Exception as e:
                logging.error(f"Error deleting blob {blob}: {e}")
