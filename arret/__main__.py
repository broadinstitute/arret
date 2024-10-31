import logging
from pathlib import Path
from typing import Annotated

import pandas as pd
import tomllib
import typer

from arret.batch import do_submit_to_gcp_batch
from arret.clean import do_clean
from arret.inventory import InventoryGenerator
from arret.plan import write_plan
from arret.utils import split_workspace_names

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

config = {}


# noinspection PyUnusedLocal
def done(*args, **kwargs):
    logging.info("Done.")


@app.callback(result_callback=done)
def main(
    ctx: typer.Context,
    config_path: Annotated[
        Path | None, typer.Option(help="path to an arret TOML config file")
    ] = None,
):
    logger = logging.getLogger()
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    if config_path is not None:
        with open(config_path, "rb") as f:
            config.update(tomllib.load(f))

        ctx.obj = config


@app.command()
def inventory(
    ctx: typer.Context,
    workspace_namespace: Annotated[
        str | None, typer.Option(help="the namespace of the Terra workspace")
    ] = None,
    workspace_name: Annotated[
        str | None, typer.Option(help="the name of the Terra workspace")
    ] = None,
    gcp_project_id: Annotated[str | None, typer.Option(help="a GCP project ID")] = None,
    inventory_path: Annotated[
        Path | None, typer.Option(help="path to the inventory .ndjson file")
    ] = None,
) -> None:
    """
    Generate a .ndjson file of blob metadata for a Terra workspace's GCS bucket.
    """

    if ctx.obj is None:
        if workspace_namespace is None:
            raise typer.BadParameter("--workspace-namespace is required")
        if workspace_name is None:
            raise typer.BadParameter("--workspace-name is required")
        if gcp_project_id is None:
            raise typer.BadParameter("--gcp-project-id is required")
        if inventory_path is None:
            raise typer.BadParameter("--inventory-path is required")

        ig = InventoryGenerator(
            workspace_namespace=workspace_namespace,
            workspace_name=workspace_name,
            gcp_project_id=gcp_project_id,
            inventory_path=inventory_path,
        )

    else:
        ig = InventoryGenerator(
            workspace_namespace=ctx.obj["terra"]["workspace_namespace"],
            workspace_name=ctx.obj["terra"]["workspace_name"],
            gcp_project_id=ctx.obj["gcp_project_id"],
            inventory_path=ctx.obj["plan"]["inventory_path"],
        )

    ig.write_inventory()


@app.command()
def plan(
    ctx: typer.Context,
    workspace_namespace: Annotated[
        str | None, typer.Option(help="the namespace of the Terra workspace")
    ] = None,
    workspace_name: Annotated[
        str | None, typer.Option(help="the name of the Terra workspace")
    ] = None,
    inventory_path: Annotated[
        Path | None, typer.Option(help="path to the inventory .ndjson file")
    ] = None,
    plan_path: Annotated[
        Path | None, typer.Option(help="path to write plan .duckdb file")
    ] = None,
    days_considered_old: Annotated[
        int | None,
        typer.Option(help="number of days after which a file is considered old"),
    ] = None,
    bytes_considered_large: Annotated[
        int | None,
        typer.Option(
            help="size threshold (in bytes) above which a file is considered large"
        ),
    ] = None,
) -> None:
    """
    Read the .ndjson inventory file into a DuckDB database and indicate blobs that are
    old, large, forcibly kept, etc.
    """

    if ctx.obj is None:
        if workspace_namespace is None:
            raise typer.BadParameter("--workspace-namespace is required")
        if workspace_name is None:
            raise typer.BadParameter("--workspace-name is required")
        if inventory_path is None:
            raise typer.BadParameter("--inventory-path is required")
        if plan_path is None:
            raise typer.BadParameter("--plan-path is required")
        if days_considered_old is None:
            raise typer.BadParameter("--days-considered-old is required")
        if bytes_considered_large is None:
            raise typer.BadParameter("--bytes-considered-large is required")

        write_plan(
            workspace_namespace=workspace_namespace,
            workspace_name=workspace_name,
            inventory_path=str(inventory_path),
            plan_path=str(plan_path),
            days_considered_old=days_considered_old,
            bytes_considered_large=bytes_considered_large,
        )

    else:
        write_plan(
            workspace_namespace=ctx.obj["terra"]["workspace_namespace"],
            workspace_name=ctx.obj["terra"]["workspace_name"],
            inventory_path=ctx.obj["plan"]["inventory_path"],
            plan_path=ctx.obj["plan"]["plan_path"],
            days_considered_old=int(ctx.obj["plan"]["days_considered_old"]),
            bytes_considered_large=int(ctx.obj["plan"]["bytes_considered_large"]),
        )


@app.command()
def clean(
    ctx: typer.Context,
    workspace_namespace: Annotated[
        str | None, typer.Option(help="the namespace of the Terra workspace")
    ] = None,
    workspace_name: Annotated[
        str | None, typer.Option(help="the name of the Terra workspace")
    ] = None,
    plan_path: Annotated[
        Path | None, typer.Option(help="path to write plan .duckdb file")
    ] = None,
    gcp_project_id: Annotated[str | None, typer.Option(help="a GCP project ID")] = None,
    other_workspaces: Annotated[
        list[str] | None,
        typer.Option(
            help="another Terra workspace to check for blob usage against, formatted as"
            ' "namespace/name" (can be specified multiple times)'
        ),
    ] = None,
) -> None:
    """
    Read the DuckDB plan database and delete all files indicated as deletable.
    """

    if ctx.obj is None:
        if workspace_namespace is None:
            raise typer.BadParameter("--workspace-namespace is required")
        if workspace_name is None:
            raise typer.BadParameter("--workspace-name is required")
        if plan_path is None:
            raise typer.BadParameter("--plan-path is required")
        if gcp_project_id is None:
            raise typer.BadParameter("--gcp-project-id is required")
        if other_workspaces is None:
            other_workspaces = []

        do_clean(
            workspace_namespace=workspace_namespace,
            workspace_name=workspace_name,
            plan_path=str(plan_path),
            gcp_project_id=gcp_project_id,
            other_workspaces=split_workspace_names(other_workspaces),
        )
    else:
        do_clean(
            workspace_namespace=ctx.obj["terra"]["workspace_namespace"],
            workspace_name=ctx.obj["terra"]["workspace_name"],
            plan_path=ctx.obj["plan"]["plan_path"],
            gcp_project_id=ctx.obj["gcp_project_id"],
            other_workspaces=ctx.obj["terra"]["other_workspaces"]
            if "other_workspaces" in ctx.obj["terra"]
            else [],
        )


@app.command()
def run_all(
    ctx: typer.Context,
    workspace_namespace: Annotated[
        str | None, typer.Option(help="the namespace of the Terra workspace")
    ] = None,
    workspace_name: Annotated[
        str | None, typer.Option(help="the name of the Terra workspace")
    ] = None,
    gcp_project_id: Annotated[str | None, typer.Option(help="a GCP project ID")] = None,
    inventory_path: Annotated[
        Path | None, typer.Option(help="path to the inventory .ndjson file")
    ] = None,
    plan_path: Annotated[
        Path | None, typer.Option(help="path to write plan .duckdb file")
    ] = None,
    days_considered_old: Annotated[
        int | None,
        typer.Option(help="number of days after which a file is considered old"),
    ] = None,
    bytes_considered_large: Annotated[
        int | None,
        typer.Option(
            help="size threshold (in bytes) above which a file is considered large"
        ),
    ] = None,
    other_workspaces: Annotated[
        list[str] | None,
        typer.Option(
            help="another Terra workspace to check for blob usage against, formatted as"
            ' "namespace/name" (can be specified multiple times)'
        ),
    ] = None,
) -> None:
    """
    Run all arret steps (inventory, plan, clean) in sequence.
    """

    inventory(
        ctx,
        workspace_namespace,
        workspace_name,
        gcp_project_id,
        inventory_path,
    )

    plan(
        ctx,
        workspace_namespace,
        workspace_name,
        inventory_path,
        plan_path,
        days_considered_old,
        bytes_considered_large,
    )

    clean(
        ctx,
        workspace_namespace,
        workspace_name,
        plan_path,
        gcp_project_id,
        other_workspaces,
    )


@app.command()
def submit_to_gcp_batch(
    ctx: typer.Context,
    workspace_namespace: Annotated[
        str | None, typer.Option(help="the namespace of the Terra workspace")
    ] = None,
    workspace_name: Annotated[
        str | None, typer.Option(help="the name of the Terra workspace")
    ] = None,
    gcp_project_id: Annotated[str | None, typer.Option(help="a GCP project ID")] = None,
    days_considered_old: Annotated[
        int | None,
        typer.Option(help="number of days after which a file is considered old"),
    ] = None,
    bytes_considered_large: Annotated[
        int | None,
        typer.Option(
            help="size threshold (in bytes) above which a file is considered large"
        ),
    ] = None,
    other_workspaces: Annotated[
        list[str] | None,
        typer.Option(
            help="another Terra workspace to check for blob usage against, formatted as"
            ' "namespace/name" (can be specified multiple times)'
        ),
    ] = None,
    region: Annotated[
        str | None, typer.Option(help="the GCP region name to run the batch job in")
    ] = None,
    zone: Annotated[
        str | None,
        typer.Option(
            help='the GCP zone (e.g. "us-central1-a") to query for machine type info'
        ),
    ] = None,
    machine_type: Annotated[
        str | None,
        typer.Option(help='the name of the machine type (e.g. "n2-highcpu-4")'),
    ] = None,
    boot_disk_mib: Annotated[
        int | None, typer.Option(help="the size of the boot disk in MiB")
    ] = None,
    max_run_seconds: Annotated[
        int | None, typer.Option(help="maximum runtime of the job in seconds")
    ] = None,
    provisioning_model: Annotated[
        str, typer.Option(help='VM provisioning model (e.g. "SPOT", "STANDARD", etc.)')
    ] = "STANDARD",
    service_account_email: Annotated[
        str | None, typer.Option(help="an email address for a service account")
    ] = None,
    container_image_uri: Annotated[
        str, typer.Option(help="a URL for the arret Docker image")
    ] = "docker.io/dmccabe606/arret:latest",
) -> None:
    """
    Submit a job to GCP Batch to run all of the arret steps.
    """

    if ctx.obj is None:
        if workspace_namespace is None:
            raise typer.BadParameter("--workspace-namespace is required")
        if workspace_name is None:
            raise typer.BadParameter("--workspace-name is required")
        if gcp_project_id is None:
            raise typer.BadParameter("--gcp-project-id is required")
        if days_considered_old is None:
            raise typer.BadParameter("--days-considered-old is required")
        if bytes_considered_large is None:
            raise typer.BadParameter("--bytes-considered-large is required")
        if other_workspaces is None:
            other_workspaces = []
        if region is None:
            raise typer.BadParameter("--region is required")
        if zone is None:
            raise typer.BadParameter("--zone is required")
        if machine_type is None:
            raise typer.BadParameter("--machine-type is required")
        if boot_disk_mib is None:
            raise typer.BadParameter("--boot-disk-mib is required")
        if max_run_seconds is None:
            raise typer.BadParameter("--max-run-seconds is required")
        if service_account_email is None:
            raise typer.BadParameter("--service-account-email is required")

        do_submit_to_gcp_batch(
            workspace_namespace=workspace_namespace,
            workspace_name=workspace_name,
            days_considered_old=days_considered_old,
            bytes_considered_large=bytes_considered_large,
            other_workspaces=split_workspace_names(other_workspaces),
            gcp_project_id=gcp_project_id,
            region=region,
            zone=zone,
            machine_type=machine_type,
            boot_disk_mib=boot_disk_mib,
            max_run_seconds=max_run_seconds,
            provisioning_model=provisioning_model,
            service_account_email=service_account_email,
            container_image_uri=container_image_uri,
        )
    else:
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


if __name__ == "__main__":
    app()
