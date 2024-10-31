_Arret_: Stop overspending on Terra GCS storage
---

Inspired by [automop](https://github.com/broadinstitute/automop/), this tool deletes unneeded objects from a Terra workspace's GCS bucket in order to reduce storage costs.

It will delete objects in the bucket that are redundant (e.g. `pipelines-logs/*`), older than a specified date, or larger than a specified size. In order to prevent deleting "active" files, however, any file with a `gs://` URL referenced in any data table in the Terra workspace (or other specified workspaces) will not be deleted.

# Recommended installation

1. Install the required system dependencies:
    - [pyenv](https://github.com/pyenv/pyenv)
    - [Poetry](https://python-poetry.org/)

2. Install the required Python version (developed with 3.12.3, but other 3.12+ versions should work):
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

A `requirements.txt` file is also available and kept in sync with Poetry dependencies in case you don't want to use Poetry, or you can use arret via docker: `docker pull dmccabe606/arret:latest`.

This repo expects that your default `GOOGLE_APPLICATION_CREDENTIALS` authorizes write access to the Terra workspace (and its bucket).

# Running

The `arret` CLI app requires just a single named argument for the path to a config file. See `configs/example.dist.toml` for an example:

```toml
gcp_project_id = ""

[terra]
workspace_namespace = ""
workspace_name = ""
other_workspaces = [
   { "workspace_namespace" = "",  "workspace_name" = "" }
] # optional

[inventory]
inventory_path = "./data/inventories/inventory.ndjson"

[plan]
plan_path = "./data/plans/plan.duckdb"
days_considered_old = 30 # can be 0
bytes_considered_large = 1e6 # can be 0

[clean]
to_delete_sql = "is_pipeline_logs OR is_old OR is_large"

[batch]
region = "us-central1"
zone = "us-central1-a" # used only to look up CPU and memory for the `machine_type`
machine_type = "n2-highcpu-4"
boot_disk_mib = 20000 # should be large enough to accommodate the inventory file
max_run_seconds = 1200
provisioning_model = "STANDARD" # or, e.g., "SPOT"
service_account_email = "" # see README
container_image_uri = "docker.io/dmccabe606/arret:latest"
```

All the steps can be run in sequence with the `run-all` command:
```shell
poetry run python -m arret --config-path="./configs/your_config.toml" run-all
```

## Steps

Alternatively, the steps can be run individually: 

### 1. Inventory

```shell
poetry run python -m arret --config-path="./configs/your_config.toml" inventory
```

This will create an `.ndjson` file containing name, size, and updated datetime for all the blobs in the GCS bucket.

### 2. Plan

```shell
poetry run python -m arret --config-path="./configs/your_config.toml" plan
```

This loads the generated inventory and stores it as a DuckDB database, with additional columns indicating whether blobs are large, old, etc.

### 3. Clean

```shell
poetry run python -m arret --config-path="./configs/your_config.toml" clean
```

This reopens the DuckDB and collects blobs to be deleted. It will delete a blob if _any_ of the following is true (this logic can be changed easily by modifying the `to_delete_sql` SQL string):
- blob is old (based on `days_considered_old`)
- blob is large (based on `bytes_considered_large`)
- blob is inside a `/pipelines-logs/` folder

...except when _any_ of the following is true:
- blob is referenced in any Terra data table in the workspace of interest or any of the `other_workspaces`
- blob is forcibly kept for recordkeeping purposes (i.e. it's a `script` or `.log` file)

## Config-free commands

Alternatively, you can omit `--config-path` and pass named options to the various commands, e.g.:

```shell
poetry run python -m arret run-all \
  --workspace-namespace the-workspace-namespace \
  --workspace-name the-workspace-name \
  --gcp-project-id the-gcp-project-id \
  --inventory-path ./data/inventories/inventory.ndjson \
  --plan-path ./data/plans/plan.duckdb \
  --days-considered-old 30 \
  --bytes-considered-large 1000000 \
  --other-workspaces the-workspace-namespace/workspace-1 \
  --other-workspaces the-workspace-namespace/workspace-2 \
  --other-workspaces the-workspace-namespace/workspace-3 # etc.
```

## Runtime

Since inventory generation and blob deletion can take a long time, these steps are multithreaded. Even with many threads available, though, running arret might still take several hours if the Terra workspace has thousands of job submissions. Terra generates lots of small files (especially redundant logs) that must be iterated every time the `inventory` step runs. One source is a workflow's `/pipelines-logs/` folder, which arret deletes, so subsequent runs will be nominally faster.

## Remote execution on GCP Batch

To aid in automation and reduce runtime, the `run-all` command can also be submitted as a [GCP Batch](https://cloud.google.com/batch/docs/get-started) job:

```shell
poetry run python -m arret --config-path="./configs/your_config.toml" submit-to-gcp-batch
```

This requires having already created a GCP service account with at least these IAM permissions:
- Batch Agent Reporter 
- Logs Writer
- Service Usage Consumer
- Storage Object Admin

The service account must also [be registered in Terra and belong to a Terra group](https://support.terra.bio/hc/en-us/articles/7448594459931-How-to-use-a-service-account-in-Terra) that has write access to the workspace you're cleaning and read access to workspaces listed in `other_workspaces` (if any). Note that it might take up to a day for Terra to sync permissions from a newly registered service account to the GCS buckets it should be able to access.
