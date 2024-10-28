import datetime
import logging
import os
from pathlib import Path

import pandas as pd

from arret.terra import TerraWorkspace
from arret.utils import human_readable_size


def write_plan(
    workspace_namespace: str,
    workspace_name: str,
    inventory_path: Path,
    days_considered_old: int,
    size_considered_large: int,
    timestamp_plan_file: bool,
) -> None:
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

    if timestamp_plan_file:
        now = datetime.datetime.now().isoformat(timespec="seconds").replace(":", "-")
        plan_file = os.path.join(plan_dir, f"{now}.parquet")
    else:
        plan_file = os.path.join(plan_dir, "plan.parquet")

    logging.info(f"Writing plan to {plan_file}")
    plan.to_parquet(plan_file)


def read_inventory(inventory_path: Path | str, bucket_name: str) -> pd.DataFrame:
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
