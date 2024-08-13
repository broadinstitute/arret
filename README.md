Arret: Stop overspending on Terra GCS storage
---

Inspired by [automop](https://github.com/broadinstitute/automop/), this repo uses the output from [gcs-inventory-loader](https://github.com/domZippilli/gcs-inventory-loader) to delete unneeded objects from a Terra workspace's GCS bucket in order to reduce storage costs.

It will delete objects in the bucket that are redundant (e.g. `pipeline-logs`), older than a specified date, or larger than a specified size. In order to prevent deleting "active" files, however, any file with a `gs://` URL referenced in any data table in the Terra workspace will not be deleted.

# Installation

1. Install the required system dependencies:
    - [pyenv](https://github.com/pyenv/pyenv)
    - [Poetry](https://python-poetry.org/)
2. Install the required Python version (3.12.3):
   ```shell
   pyenv install "$(cat .python-version)"
   ```
3. Confirm that `python` maps to the correct version:
   ```
   python --version
   ```
4. Set the Poetry interpreter and install the Python dependencies:
   ```shell
   poetry env use "$(pyenv which python)"
   poetry install
   ```

This repo expects that your `GOOGLE_APPLICATION_CREDENTIALS` authorizes write access to the Terra workspace (and its bucket).

# Generating inventory

For now, gcs-inventory-loader is used to generate a new line-delimited JSON file of the bucket's objects:

1. Fill out the `PROJECT` value in `default.cfg`.
2. In `cat.py`, fix the print statement in `page_outputter` so that it prints JSON instead of Python dictionaries: `print(json.dumps(blob_metadata))`.
3. Run the tool: `python -m gcs_inventory_loader cat fc-secure-uuid-goes-here > ../arret/data/inventory.ndjson`.

# Running

Fill out a new config file based on `configs/example.toml.dist` and generate a cleanup plan:

```shell
poetry run -m arret --config-path="./configs/your_config.toml" plan
```

Then, do the cleanup:

```shell
poetry run -m arret --config-path="./configs/your_config.toml" clean --plan-file="./data/plans/the-workspace-namespace/the-workspace-name/plan.parquet"
```
