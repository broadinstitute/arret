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

    # split blob names by `/` to columns
    max_name_depth = inv["name"].str.count("/").max()
    name_parts = inv["name"].str.split("/", expand=True, regex=False)
    name_parts.columns = ["name" + str(x) for x in name_parts.columns]
    name_part_cols = list(name_parts.columns)
    inv = pd.concat([inv, name_parts], axis=1)

    ops = []  # start collecting delete/keep operations

    # iterate over `name0`...`nameN` columns
    for cix in range(max_name_depth + 1):
        # for each blob, keep track of the path we're considering this iteration (path
        # could be a GCS folder or a file)
        if cix == 0:
            inv["path"] = inv[name_part_cols[cix]]
        else:
            inv["path"] = inv["path"] + "/" + inv[name_part_cols[cix]]

        # group by current paths
        inv = inv.set_index(name_part_cols[cix], append=cix > 0)
        inv_g = inv.groupby(inv.index.names)

        # for each group, check are all blobs in that path deletable (for different
        # kinds of deletability)
        all_deletable = inv_g["deletable"].all()
        all_deletable_age = inv_g["deletable_age"].all()
        all_deletable_empty = inv_g["deletable_empty"].all()
        all_deletable_large = inv_g["deletable_large"].all()
        all_deletable_not_kept = inv_g["deletable_not_kept"].all()
        all_deletable_pipeline_logs = inv_g["deletable_pipeline_logs"].all()

        # collect stats/deletability for each group
        total_size = inv_g["size"].sum()
        last_updated = inv_g["updated"].max()
        n_objs = inv_g.size()
        n_objs.name = "n_objs"

        stats = pd.concat(
            [
                all_deletable,
                all_deletable_age,
                all_deletable_empty,
                all_deletable_large,
                all_deletable_not_kept,
                all_deletable_pipeline_logs,
                total_size,
                last_updated,
                n_objs,
            ],
            axis=1,
        )

        stats["depth"] = cix

        # finally mark whether each path should be deleted
        stats["to_delete"] = (
            stats["deletable_empty"]
            | stats["deletable_large"]
            | stats["deletable_pipeline_logs"]
            | (stats["deletable_age"] & stats["deletable_not_kept"])
        )

        # also indicate whether a path represents a single file and we're keeping it
        stats["to_keep"] = stats["n_objs"].eq(1) & ~stats["to_delete"]

        if stats["to_delete"].any():
            # collect all the blob names under each deletable path
            blobs = (
                (
                    inv.loc[stats.loc[stats["to_delete"]].index]
                    .groupby(inv.index.names)["name"]
                    .agg(list)
                )
                .to_frame()
                .rename(columns={"name": "blobs"})
            )

            stats = stats.join(blobs, how="left")

        # can remove all blobs in inventory if we just marked them as deleted or kept
        inv = inv.loc[stats.loc[~(stats["to_delete"] | stats["to_keep"])].index]

        ops.append(stats.reset_index(drop=True))

    # return data frame of deletion operations
    ops = pd.concat(ops)
    delete_ops = ops.loc[ops["to_delete"]].sort_values("size", ascending=False)
    return delete_ops.drop(columns=["to_delete", "to_keep"])
