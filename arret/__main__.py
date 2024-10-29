import logging
from pathlib import Path
from typing import Annotated, Any

import pandas as pd
import psutil
import tomllib
import typer

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

config: dict[str, Any] = {}


# noinspection PyUnusedLocal
def done(*args, **kwargs):
    logging.info("Done.")


@app.callback(result_callback=done)
def main(
    ctx: typer.Context,
    config_path: Annotated[Path, typer.Option(exists=True)],
):
    logger = logging.getLogger()
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    with open(config_path, "rb") as f:
        config.update(tomllib.load(f))

    ctx.obj = config


@app.command()
def inventory(
    ctx: typer.Context,
    n_workers: Annotated[int, typer.Option()] = psutil.cpu_count(),
) -> None:
    ig = InventoryGenerator(
        workspace_namespace=ctx.obj["terra"]["workspace_namespace"],
        workspace_name=ctx.obj["terra"]["workspace_name"],
        gcp_project_id=ctx.obj["gcp_project_id"],
        out_file=ctx.obj["plan"]["inventory_path"],
        n_workers=n_workers,
    )

    ig.write_inventory()


@app.command()
def plan(ctx: typer.Context) -> None:
    write_plan(
        workspace_namespace=ctx.obj["terra"]["workspace_namespace"],
        workspace_name=ctx.obj["terra"]["workspace_name"],
        inventory_path=ctx.obj["plan"]["inventory_path"],
        plan_path=ctx.obj["plan"]["plan_path"],
        days_considered_old=ctx.obj["plan"]["days_considered_old"],
        size_considered_large=ctx.obj["plan"]["size_considered_large"],
    )


@app.command()
def clean(ctx: typer.Context) -> None:
    do_clean(
        workspace_namespace=ctx.obj["terra"]["workspace_namespace"],
        workspace_name=ctx.obj["terra"]["workspace_name"],
        plan_path=ctx.obj["plan"]["plan_path"],
        gcp_project_id=ctx.obj["gcp_project_id"],
        other_workspaces=ctx.obj["terra"]["other_workspaces"]
        if "other_workspaces" in ctx.obj["terra"]
        else [],
    )


if __name__ == "__main__":
    app()
