import datetime
import os
from pathlib import Path

import pandas as pd
from click import echo

from arret.terra import TerraWorkspace
from arret.utils import extract_unique_values, human_readable_size


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
    gs_urls = get_gs_urls(tw, bucket_name)

    # read the bucket inventory and make a cleaning plan
    inv = read_inventory(inventory_path, bucket_name)
    plan = make_plan(inv, gs_urls, days_considered_old, size_considered_large)

    stats = {
        "n_ops": len(plan),
        "n_objs": int(plan["n_objs"].sum()),
        "total_size": human_readable_size(plan["size"].sum()),
    }

    echo(f"Plan stats: {stats}")

    # write plan as parquet
    plan_dir = os.path.join(".", "data", "plans", workspace_namespace, workspace_name)
    os.makedirs(plan_dir, exist_ok=True)

    if timestamp_plan_file:
        now = datetime.datetime.now().isoformat(timespec="seconds").replace(":", "-")
        plan_file = os.path.join(plan_dir, f"{now}.parquet")
    else:
        plan_file = os.path.join(plan_dir, "plan.parquet")

    plan.to_parquet(plan_file)
    echo(f"Wrote plan to {plan_file}")


def get_gs_urls(tw: TerraWorkspace, bucket_name: str) -> set[str]:
    entity_types = tw.get_entity_types()
    entities = {k: tw.get_entities(k) for k in entity_types}

    gs_urls = set()

    for _, df in entities.items():
        unique_values = extract_unique_values(df)

        gs_urls.update(
            {
                x
                for x in unique_values
                if isinstance(x, str) and x.startswith(f"gs://{bucket_name}/")
            }
        )

    return gs_urls


def read_inventory(inventory_path: Path, bucket_name: str) -> pd.DataFrame:
    inv = pd.read_json(inventory_path, lines=True)
    inv = inv.loc[:, ["name", "updated", "size"]]

    # construct full URLs for blob names
    inv["gs_url"] = "gs://" + bucket_name + "/" + inv["name"]

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
    gs_urls: set[str],
    days_considered_old: int,
    size_considered_large: int,
) -> pd.DataFrame:
    # file is generally deletable if it's not referenced in a data table
    inv["deletable"] = ~inv["gs_url"].isin(gs_urls)

    # indicate files that are "old"
    inv["deletable_age"] = inv["deletable"] & inv["updated"].dt.date.lt(
        (pd.Timestamp.now() - pd.Timedelta(days=days_considered_old)).date()
    )

    # indicate empty and "large" files
    inv["deletable_empty"] = inv["deletable"] & inv["size"].eq(0)
    inv["deletable_large"] = inv["deletable"] & inv["size"].gt(size_considered_large)

    # indicate deletable files we'll make an exception for (task scripts and logs)
    inv["deletable_not_kept"] = inv["deletable"] & ~(
        inv["name"].str.endswith(".log") | inv["name"].str.endswith("/script")
    )

    # indicate GCS paths representing the pipeline-logs folder, which are redundant with
    # task logs
    inv["deletable_pipeline_logs"] = inv["deletable"] & inv["name"].str.contains(
        r"/pipelines-logs/[^/]", regex=True
    )

    inv["to_delete"] = (
        inv["deletable_empty"]
        | inv["deletable_pipeline_logs"]
        | inv["deletable_large"]
        | (inv["deletable_age"] & inv["deletable_not_kept"])
    )

    return inv.loc[inv["to_delete"], ["name", "updated", "size", "gs_url"]].sort_values(
        "size", ascending=False
    )
