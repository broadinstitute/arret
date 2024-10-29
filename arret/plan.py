import datetime
import logging
import os

import pandas as pd

from arret.terra import TerraWorkspace
from arret.utils import human_readable_size


def write_plan(
    workspace_namespace: str,
    workspace_name: str,
    inventory_path: os.PathLike,
    days_considered_old: int,
    size_considered_large: int,
) -> None:
    """
    Write a Parquet cleanup plan for a Terra workspace based on its inventory.

    :param workspace_namespace: the namespace of the Terra workspace
    :param workspace_name: the name of the Terra workspace
    :param inventory_path: path to the inventory file
    :param days_considered_old: number of days after which a file is considered old
    :param size_considered_large: size threshold (in bytes) above which a file is
    considered large
    """

    # get set of all gs:// URLs referenced in the workspace's data tables
    tw = TerraWorkspace(workspace_namespace, workspace_name)
    bucket_name = tw.get_bucket_name()

    # read the bucket inventory and make a cleanup plan
    inv = read_inventory(inventory_path, bucket_name)
    plan = make_plan(inv, days_considered_old, size_considered_large)

    total_size = human_readable_size(plan["size"].sum())
    logging.info(f"Total size: {total_size}")

    # write plan as parquet
    plan_dir = os.path.join(".", "data", "plans", workspace_namespace, workspace_name)
    os.makedirs(plan_dir, exist_ok=True)
    plan_file = os.path.join(plan_dir, "plan.parquet")

    logging.info(f"Writing plan to {plan_file}")
    plan.to_parquet(plan_file)


def read_inventory(inventory_path: os.PathLike, bucket_name: str) -> pd.DataFrame:
    """
    Reads inventory data from a JSON file and constructs full URLs for each item.

    :param inventory_path: Path to the inventory JSON file.
    :param bucket_name: Name of the storage bucket.
    """

    logging.info(f"Reading inventory from {inventory_path}")
    inv = pd.read_json(
        inventory_path, lines=True, dtype="string", encoding="utf-8", engine="pyarrow"
    )
    inv = inv.loc[:, ["name", "updated", "size"]]

    # construct full URLs for blob names
    inv["gs_url"] = "gs://" + bucket_name + "/" + inv["name"]

    logging.info("Setting inventory dtypes")
    inv = inv.astype(
        {
            "name": "string",
            "updated": "datetime64[ns, UTC]",
            "size": "int64",
            "gs_url": "string",
        }
    )

    return inv


def make_plan(
    inv: pd.DataFrame,
    days_considered_old: int,
    size_considered_large: int,
) -> pd.DataFrame:
    """
    Generates a cleanup plan for a given inventory dataframe based on age and size
    criteria.

    :param inv: input inventory dataframe
    :param days_considered_old: number of days after which a file is considered old
    :param size_considered_large: size threshold (in bytes) above which a file is
    considered large
    """

    logging.info("Making cleanup plan")
    # indicate large files
    inv["is_large"] = inv["size"].gt(size_considered_large)

    # indicate GCS paths for pipeline-logs folder, which are redundant with task logs
    inv["is_pipeline_logs"] = inv["name"].str.contains("/pipelines-logs/", regex=False)

    # indicate files that are "old"
    inv["is_old"] = inv["updated"].dt.date.lt(
        (pd.Timestamp.now() - pd.Timedelta(days=days_considered_old)).date()
    )

    # indicate deletable files we'll make an exception for (task scripts and logs) even
    # if they're old
    inv["force_keep"] = inv["name"].str.endswith(".log") | inv["name"].str.endswith(
        "/script"
    )

    return inv
