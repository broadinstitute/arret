import logging
from pathlib import Path
from typing import Annotated, Any

import pandas as pd
import tomllib
import typer

from arret.batch import do_submit_to_gcp_batch
from arret.clean import do_clean
from arret.inventory import InventoryGenerator
from arret.plan import write_plan

pd.set_option("display.max_columns", 30)
pd.set_option("display.max_colwidth", 50)
pd.set_option("display.max_info_columns", 30)
pd.set_option("display.max_info_rows", 20)
pd.set_option("display.max_rows", 20)
pd.set_option("display.max_seq_items", None)
pd.set_option("display.width", 200)
pd.set_option("expand_frame_repr", True)
pd.set_option("mode.chained_assignment", "warn")

app = typer.Typer()

local_app = typer.Typer()
app.add_typer(local_app, name="local")

remote_app = typer.Typer()
app.add_typer(remote_app, name="remote")

config: dict[str, Any] = {}


# noinspection PyUnusedLocal
def done(*args, **kwargs):
    logging.info("Done.")


@app.callback(result_callback=done)
def main():
    logger = logging.getLogger()
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)


@local_app.callback()
def main(
    ctx: typer.Context,
    config_path: Annotated[Path, typer.Option(exists=True)],
):
    with open(config_path, "rb") as f:
        config.update(tomllib.load(f))

    ctx.obj = config


@local_app.command()
def inventory(ctx: typer.Context) -> None:
    """
    Generate a .ndjson file of blob metadata for a Terra workspace's GCS bucket.
    """

    ig = InventoryGenerator(
        workspace_namespace=ctx.obj["terra"]["workspace_namespace"],
        workspace_name=ctx.obj["terra"]["workspace_name"],
        gcp_project_id=ctx.obj["gcp_project_id"],
        inventory_path=ctx.obj["plan"]["inventory_path"],
    )

    ig.write_inventory()


@local_app.command()
def plan(ctx: typer.Context) -> None:
    """
    Read the .ndjson inventory file into a DuckDB database and indicate blobs that are
    old, large, forcibly kept, etc.
    """

    write_plan(
        workspace_namespace=ctx.obj["terra"]["workspace_namespace"],
        workspace_name=ctx.obj["terra"]["workspace_name"],
        inventory_path=ctx.obj["plan"]["inventory_path"],
        plan_path=ctx.obj["plan"]["plan_path"],
        days_considered_old=int(ctx.obj["plan"]["days_considered_old"]),
        bytes_considered_large=int(ctx.obj["plan"]["bytes_considered_large"]),
    )


@local_app.command()
def clean(ctx: typer.Context) -> None:
    """
    Read the DuckDB plan database and delete all files indicated as deletable.
    """

    do_clean(
        workspace_namespace=ctx.obj["terra"]["workspace_namespace"],
        workspace_name=ctx.obj["terra"]["workspace_name"],
        plan_path=ctx.obj["plan"]["plan_path"],
        gcp_project_id=ctx.obj["gcp_project_id"],
        other_workspaces=ctx.obj["terra"]["other_workspaces"]
        if "other_workspaces" in ctx.obj["terra"]
        else [],
    )


@local_app.command()
def run_all(ctx: typer.Context) -> None:
    """
    Run all arret steps (inventory, plan, clean) in sequence.
    """

    inventory(ctx)
    plan(ctx)
    clean(ctx)


@local_app.command()
def submit_to_gcp_batch(ctx: typer.Context) -> None:
    """
    Submit a job to GCP Batch to run all of the arret steps.
    """

    do_submit_to_gcp_batch(
        workspace_namespace=ctx.obj["terra"]["workspace_namespace"],
        workspace_name=ctx.obj["terra"]["workspace_name"],
        days_considered_old=int(ctx.obj["plan"]["days_considered_old"]),
        bytes_considered_large=int(ctx.obj["plan"]["bytes_considered_large"]),
        other_workspaces=ctx.obj["terra"]["other_workspaces"]
        if "other_workspaces" in ctx.obj["terra"]
        else [],
        gcp_project_id=ctx.obj["gcp_project_id"],
        region=ctx.obj["batch"]["region"],
        zone=ctx.obj["batch"]["zone"],
        machine_type=ctx.obj["batch"]["machine_type"],
        boot_disk_mib=ctx.obj["batch"]["boot_disk_mib"],
        max_run_seconds=ctx.obj["batch"]["max_run_seconds"],
        provisioning_model=ctx.obj["batch"]["provisioning_model"],
        service_account_email=ctx.obj["batch"]["service_account_email"],
        container_image_uri=ctx.obj["batch"]["container_image_uri"],
    )


@remote_app.command()
def run_all(
    workspace_namespace: Annotated[str, typer.Option()],
    workspace_name: Annotated[str, typer.Option()],
    gcp_project_id: Annotated[str, typer.Option()],
    inventory_path: Annotated[Path, typer.Option()],
    plan_path: Annotated[Path, typer.Option()],
    days_considered_old: Annotated[int, typer.Option()],
    bytes_considered_large: Annotated[int, typer.Option()],
    other_workspaces: Annotated[list[str], typer.Option()],
) -> None:
    """
    Run all arret steps (inventory, plan, clean) in sequence, without the need for a
    config TOML file. This can be used as a GCP Batch task.
    """

    ig = InventoryGenerator(
        workspace_namespace=workspace_namespace,
        workspace_name=workspace_name,
        gcp_project_id=gcp_project_id,
        inventory_path=inventory_path,
    )

    ig.write_inventory()

    write_plan(
        workspace_namespace=workspace_namespace,
        workspace_name=workspace_name,
        inventory_path=str(inventory_path),
        plan_path=str(plan_path),
        days_considered_old=days_considered_old,
        bytes_considered_large=bytes_considered_large,
    )

    # split condensed workspace names into list of dicts
    other_workspaces_dicts = [
        {"workspace_namespace": x1, "workspace_name": x2}
        for x in other_workspaces
        for x1, x2 in [x.split("/")]
    ]

    do_clean(
        workspace_namespace=workspace_namespace,
        workspace_name=workspace_name,
        plan_path=str(plan_path),
        gcp_project_id=gcp_project_id,
        other_workspaces=other_workspaces_dicts,
    )


if __name__ == "__main__":
    app()
