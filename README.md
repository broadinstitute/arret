_Arret_: Stop overspending on Terra GCS storage
---

Inspired by [automop](https://github.com/broadinstitute/automop/), this tool deletes unneeded objects from a Terra workspace's GCS bucket in order to reduce storage costs.

It will delete objects in the bucket that are redundant (e.g. `pipelines-logs/*`), older than a specified date, or larger than a specified size. In order to prevent deleting "active" files, however, any file with a `gs://` URL referenced in any data table in the Terra workspace (or other specified workspaces) will not be deleted.

# Installation

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

A `requirements.txt` file is also available and kept in sync with Poetry dependencies in case you don't want to use Poetry.

This repo expects that your default `GOOGLE_APPLICATION_CREDENTIALS` authorizes write access to the Terra workspace (and its bucket).

# Running

1. Fill out a new config file based on `configs/example.dist.toml` and generate a bucket inventory:
```shell
poetry run -m arret --config-path="./configs/your_config.toml" inventory
```

This will create an `.ndjson` file containing name, size, and updated datetime for all the blobs in the GCS bucket.

2. Generate a cleanup plan:
```shell
poetry run -m arret --config-path="./configs/your_config.toml" plan
```

This loads the generated inventory and stores it as a DuckDB database, with additional columns indicating whether blobs are large, old, etc.

3. Do the cleanup:
```shell
poetry run -m arret --config-path="./configs/your_config.toml" clean
```

This reopens the DuckDB and collects blobs to be deleted. It will delete a blob if any of the following is
true:

- blob is old (based on `days_considered_old`)
- blob is large (based on `bytes_considered_large`)
- blob is inside a `/pipelines-logs/` folder

...**except** when either of the following is true:
- blob is referenced in a Terra data table in the workspace of interest or any of the `other_workspaces`
- blob is forcibly kept (i.e. it's a `script` or `.log` file)
