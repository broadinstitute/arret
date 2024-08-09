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
    tw = TerraWorkspace(workspace_namespace, workspace_name)
    bucket_name = tw.get_bucket_name()
    gs_urls = get_gs_urls(tw, bucket_name)

    inv = read_inventory(inventory_path)
    plan = make_plan(inv, gs_urls, days_considered_old, size_considered_large)

    stats = {
        "n_ops": len(plan),
        "n_objs": int(plan["n_objs"].sum()),
        "total_size": human_readable_size(plan["size"].sum()),
    }

    echo(f"Plan stats: {stats}")

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


def read_inventory(inventory_path: Path) -> pd.DataFrame:
    inv = pd.read_json(inventory_path, lines=True)
    inv = inv.loc[:, ["bucket", "name", "updated", "size", "crc32c"]]
    inv["gs_url"] = "gs://" + inv["bucket"] + "/" + inv["name"]

    inv = inv.astype(
        {
            "bucket": "string",
            "name": "string",
            "updated": "datetime64[ns, UTC]",
            "size": "int64",
            "crc32c": "string",
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
    inv["in_data_table"] = inv["gs_url"].isin(gs_urls)
    inv["deletable"] = ~inv["in_data_table"]

    max_last_updated = (
        pd.Timestamp.now() - pd.Timedelta(days=days_considered_old)
    ).date()
    inv["deletable_age"] = inv["deletable"] & inv["updated"].dt.date.lt(
        max_last_updated
    )

    inv["deletable_empty"] = inv["deletable"] & inv["size"].eq(0)
    inv["deletable_large"] = inv["deletable"] & inv["size"].gt(size_considered_large)

    inv["deletable_not_kept"] = inv["deletable"] & ~(
        inv["name"].str.endswith(".log") | inv["name"].str.endswith("/script")
    )
    inv["deletable_pipeline_logs"] = inv["deletable"] & inv["name"].str.contains(
        r"/pipelines-logs/[^/]", regex=True
    )

    max_name_depth = inv["name"].str.count("/").max()
    name_parts = inv["name"].str.split("/", expand=True, regex=False)
    name_parts.columns = ["name" + str(x) for x in name_parts.columns]
    name_part_cols = list(name_parts.columns)
    inv = pd.concat([inv, name_parts], axis=1)

    inv_orig = inv.copy()

    inv = inv_orig.copy()
    ops = []

    for cix in range(max_name_depth + 1):
        if cix == 0:
            inv["path"] = inv[name_part_cols[cix]]
        else:
            inv["path"] = inv["path"] + "/" + inv[name_part_cols[cix]]

        inv = inv.set_index(name_part_cols[cix], append=cix > 0)
        inv_g = inv.groupby(inv.index.names)

        all_deletable = inv_g["deletable"].all()
        all_deletable_age = inv_g["deletable_age"].all()
        all_deletable_empty = inv_g["deletable_empty"].all()
        all_deletable_large = inv_g["deletable_large"].all()
        all_deletable_not_kept = inv_g["deletable_not_kept"].all()
        all_deletable_pipeline_logs = inv_g["deletable_pipeline_logs"].all()

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

        stats["to_delete"] = (
            # delete folders whose files are all
            # - empty, or
            # - pipeline logs, or
            # - old (unless there's a forcibly-kept file somewhere)
            stats["n_objs"].gt(1)
            & (
                stats["deletable_empty"]
                | stats["deletable_pipeline_logs"]
                | (stats["deletable_age"] & stats["deletable_not_kept"])
            )
        ) | (
            # delete a file if it is
            # - large, or
            # - old and not forcibly kept
            stats["n_objs"].eq(1)
            & (
                stats["deletable_large"]
                | (stats["deletable_age"] & stats["deletable_not_kept"])
            )
        )

        stats["kept"] = stats["n_objs"].eq(1) & ~stats["to_delete"]

        if stats["to_delete"].any():
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

        inv = inv.loc[stats.loc[~(stats["to_delete"] | stats["kept"])].index]

        ops.append(stats.reset_index(drop=True))

    ops = pd.concat(ops)
    delete_ops = ops.loc[ops["to_delete"]].sort_values("size", ascending=False)
    return delete_ops.drop(columns=["to_delete", "kept"])
