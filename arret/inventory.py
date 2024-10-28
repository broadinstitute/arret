import json
import logging
import random
import threading
from concurrent.futures import ThreadPoolExecutor
from os import PathLike
from queue import Queue
from time import sleep

from google.api_core.page_iterator import Page
from google.cloud import storage

from arret.terra import TerraWorkspace


class InventoryGenerator:
    def __init__(
        self,
        workspace_namespace: str,
        workspace_name: str,
        gcp_project_id: str,
        out_file: PathLike,
        n_workers: int = 2,
        work_queue_size: int = 1000,
    ):
        self.gcp_project_id = gcp_project_id
        self.out_file = out_file
        self.n_workers = n_workers
        self.work_queue_size = work_queue_size

        terra_workspace = TerraWorkspace(workspace_namespace, workspace_name)
        bucket_name = terra_workspace.get_bucket_name()
        self.storage_client = storage.Client(project=self.gcp_project_id)
        self.bucket = self.storage_client.bucket(
            bucket_name, user_project=self.gcp_project_id
        )

        self.file_lock = threading.Lock()
        self.counter_lock = threading.Lock()
        self.n_blobs_written = 0

        with open(self.out_file, "w"):
            pass  # truncate

    def write_inventory(self) -> None:
        # Use remaining configured workers, or at least 2, for this part
        with BoundedThreadPoolExecutor(
            max_workers=self.n_workers, queue_size=int(self.work_queue_size * 0.25)
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

        with self.file_lock:
            with open(self.out_file, "a") as f:
                f.writelines(blob_lines)

        with self.counter_lock:
            self.n_blobs_written += len(blob_lines)
            logging.info(f"{self.n_blobs_written} blob records written.")


class BoundedThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, *args, queue_size: int = 100, **kwargs):
        """Construct a slightly modified ThreadPoolExecutor with a
        bounded queue for work. Causes submit() to block when full.

        Arguments:
            ThreadPoolExecutor {[type]} -- [description]
        """
        super().__init__(*args, **kwargs)
        # noinspection PyTypeChecker
        self._work_queue = Queue(queue_size)
