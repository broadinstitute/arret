import logging
import os

import duckdb
import pandas as pd

from arret.terra import TerraWorkspace
from arret.utils import human_readable_size


def write_plan(
    workspace_namespace: str,
    workspace_name: str,
    inventory_path: str,
    plan_path: str,
    days_considered_old: int,
    bytes_considered_large: int,
) -> None:
    """
    Write a cleanup plan for a Terra workspace based on its inventory.

    :param workspace_namespace: the namespace of the Terra workspace
    :param workspace_name: the name of the Terra workspace
    :param inventory_path: path to the inventory .ndjson file
    :param plan_path: path to write plan .duckdb file
    :param days_considered_old: number of days after which a file is considered old
    :param bytes_considered_large: size threshold (in bytes) above which a file is
    considered large
    """

    # get the Terra workspace's bucket name
    terra_workspace = TerraWorkspace(workspace_namespace, workspace_name)
    bucket_name = terra_workspace.get_bucket_name()

    # recreate DB
    try:
        os.remove(plan_path)
    except OSError:
        pass

    with duckdb.connect(plan_path) as db:
        set_up_db(db)

        # load the bucket inventory into the DB and make a cleanup plan
        read_inventory(db, inventory_path, bucket_name)
        make_plan(db, days_considered_old, bytes_considered_large)

        total_size = db.table("blobs").sum("size").fetchone()[0]  # pyright: ignore
        logging.info(f"Total size: {human_readable_size(total_size)}")


def set_up_db(db: duckdb.DuckDBPyConnection) -> None:
    """
    Set up a new DuckDB database for the plan.

    :param db: the DuckDB database
    """

    db.sql("SET preserve_insertion_order = false;")

    db.sql("""
        CREATE TABLE blobs (
            url VARCHAR,
            name VARCHAR NOT NULL,
            size UBIGINT NOT NULL,
            updated TIMESTAMPTZ NOT NULL,
            is_large BOOLEAN NOT NULL DEFAULT FALSE,
            is_old BOOLEAN NOT NULL DEFAULT FALSE,
            is_pipeline_logs BOOLEAN NOT NULL DEFAULT FALSE,
            force_keep BOOLEAN NOT NULL DEFAULT FALSE,
            in_data_table BOOLEAN NOT NULL DEFAULT FALSE,
            to_delete BOOLEAN NOT NULL DEFAULT FALSE
        );
    """)


def read_inventory(
    db: duckdb.DuckDBPyConnection, inventory_path: str, bucket_name: str
) -> None:
    """
    Reads inventory data from a JSON file and constructs full URLs for each item.

    :param db: the DuckDB database
    :param inventory_path: path to the inventory JSON file
    :param bucket_name: name of the storage bucket
    """

    logging.info(f"Reading inventory from {inventory_path}")

    db.sql(f"""
        COPY
            blobs(name, size, updated)
        FROM
            '{inventory_path}' (FORMAT JSON);
    """)

    # populate the `url` column, which will be used to check against data table cell
    # values later
    db.execute(
        """
        UPDATE
            blobs
        SET
            url = 'gs://' || $bucket_name || '/' || name;
    """,
        {"bucket_name": bucket_name},
    )

    # can now add the proper column constraint for `url`
    db.sql("""
        ALTER TABLE
            blobs
        ALTER COLUMN
            url
        SET
            NOT NULL;
    """)


def make_plan(
    db: duckdb.DuckDBPyConnection,
    days_considered_old: int,
    bytes_considered_large: int,
) -> None:
    """
    Generates a cleanup plan for a given inventory dataframe based on age and size
    criteria.

    :param db: the DuckDB database
    :param days_considered_old: number of days after which a file is considered old
    :param bytes_considered_large: size threshold (in bytes) above which a file is
    considered large
    """

    logging.info("Making cleanup plan")

    db.execute(
        """
        UPDATE
            blobs
        SET
            -- indicate large files
            is_large = size > $bytes_considered_large,
            -- indicate old files
            is_old = updated < $date_considered_new,
            -- indicate paths pipeline-logs folder, which are redundant with task logs
            is_pipeline_logs = name LIKE '%/pipelines-logs/%',
            -- indicate deletable files we'll make an exception for (task scripts and 
            -- logs) even if they're old
            force_keep = name LIKE '%.log' OR name LIKE '%/script';
        """,
        {
            "bytes_considered_large": bytes_considered_large,
            "date_considered_new": pd.Timestamp.now()
            - pd.Timedelta(days=days_considered_old),
        },
    )
