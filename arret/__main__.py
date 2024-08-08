from pathlib import Path
from typing import Annotated, Any

import pandas as pd
import tomllib
import typer
from click import echo

from arret.clean import do_clean
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
    echo("Done.")


@app.callback(result_callback=done)
def main(
    ctx: typer.Context,
    config_path: Annotated[Path, typer.Option(exists=True)],
):
    with open(config_path, "rb") as f:
        config.update(tomllib.load(f))

    ctx.obj = config


@app.command()
def plan(ctx: typer.Context) -> None:
    write_plan(
        workspace_namespace=ctx.obj["terra"]["workspace_namespace"],
        workspace_name=ctx.obj["terra"]["workspace_name"],
        inventory_path=ctx.obj["plan"]["inventory_path"],
        days_considered_old=ctx.obj["plan"]["days_considered_old"],
        size_considered_large=ctx.obj["plan"]["size_considered_large"],
        timestamp_plan_file=ctx.obj["plan"]["timestamp_plan_file"],
    )


@app.command()
def clean(
    ctx: typer.Context, plan_file: Annotated[Path, typer.Option(exists=True)]
) -> None:
    do_clean(
        workspace_namespace=ctx.obj["terra"]["workspace_namespace"],
        workspace_name=ctx.obj["terra"]["workspace_name"],
        plan_file=plan_file,
        gcp_project_id=ctx.obj["gcp_project_id"],
    )


if __name__ == "__main__":
    app()
