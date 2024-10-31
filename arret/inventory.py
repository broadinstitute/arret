import json
import logging
import random
import threading
from os import PathLike
from time import sleep

import psutil
from google.api_core.page_iterator import Page
from google.cloud import storage

from arret.terra import TerraWorkspace
from arret.utils import BoundedThreadPoolExecutor


class InventoryGenerator:
    def __init__(
        self,
        workspace_namespace: str,
        workspace_name: str,
        gcp_project_id: str,
        inventory_path: PathLike,
    ):
        """
        Write a new line-delimited JSON file containing metadata for all blobs in a
        Terra Workspace's GCS bucket.

        :param workspace_namespace: the namespace of the Terra workspace
        :param workspace_name: the name of the Terra workspace
        :param gcp_project_id: a GCP project ID
        :param inventory_path: a path for the output .ndjson inventory file
        """

        self.gcp_project_id = gcp_project_id
        self.inventory_path = inventory_path
        self.n_workers = psutil.cpu_count()

        terra_workspace = TerraWorkspace(workspace_namespace, workspace_name)
        bucket_name = terra_workspace.get_bucket_name()

        self.storage_client = storage.Client(project=self.gcp_project_id)
        self.bucket = self.storage_client.bucket(
            bucket_name, user_project=self.gcp_project_id
        )

        self.file_lock = threading.Lock()
        self.counter_lock = threading.Lock()
        self.n_blobs_written = 0

        with open(self.inventory_path, "w"):
            pass  # truncate

    def write_inventory(self) -> None:
        """
        Generate the inventory using a bounded thread pool executor.
        """

        with BoundedThreadPoolExecutor(
            max_workers=self.n_workers, queue_size=self.n_workers * 2
        ) as executor:
            blobs = self.storage_client.list_blobs(
                self.bucket,
                fields="items(name,updated,size),nextPageToken",
                soft_deleted=False,
            )

            for page in blobs.pages:
                executor.submit(self.write_page_of_blobs, page)
                sleep(random.uniform(0.01, 0.03))  # calm the thundering herd

    def write_page_of_blobs(self, page: Page) -> None:
        """
        Write a page of blob metadata to the output file.

        :param page: a page of listed blobs
        """

        blob_lines = [
            json.dumps(
                {
                    "name": x.name,
                    "size": x.size,
                    "updated": x.updated.strftime("%Y-%m-%dT%H:%M:%S"),
                }
            )
            + "\n"
            for x in page
        ]

        # prevent concurrent writes to output file
        with self.file_lock:
            with open(self.inventory_path, "a") as f:
                f.writelines(blob_lines)

        # update global count of blobs written
        with self.counter_lock:
            self.n_blobs_written += len(blob_lines)
            logging.info(f"{self.n_blobs_written} blob records written.")
