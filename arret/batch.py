import datetime
import logging
import re
from uuid import uuid4

from google.cloud import batch_v1, compute_v1


def do_submit_to_gcp_batch(
    workspace_namespace: str,
    workspace_name: str,
    days_considered_old: int,
    bytes_considered_large: int,
    other_workspaces: list[dict[str, str]],
    gcp_project_id: str,
    region: str,
    zone: str,
    machine_type: str,
    boot_disk_mib: int,
    max_run_seconds: int,
    provisioning_model: str,
    service_account_email: str,
    container_image_uri: str,
) -> None:
    """
    Run all arret steps as a GCP Batch job.

    :param workspace_namespace: the namespace of the Terra workspace
    :param workspace_name: the name of the Terra workspace
    :param days_considered_old: number of days after which a file is considered old
    :param bytes_considered_large: size threshold (in bytes) above which a file is
    considered large
    :param other_workspaces: a list of dictionaries containing workspace namespaces and
    names for other Terra workspaces to check for blob usage
    :param gcp_project_id: a GCP project ID
    :param region: the GCP region name to run the batch job in
    :param zone: the GCP zone (e.g. "us-central1-a") to query for machine type info
    :param machine_type: the name of the machine type (e.g. "n2-highcpu-4")
    :param boot_disk_mib: the size of the boot disk in MiB
    :param max_run_seconds: maximum runtime of the job in seconds
    :param provisioning_model: VM provisioning model (e.g. "SPOT", "STANDARD", etc.)
    :param service_account_email: an email address for a service account
    :param container_image_uri: a URL for the arret Docker image
    """

    instance_policy = batch_v1.AllocationPolicy.InstancePolicy()
    instance_policy.machine_type = machine_type
    instance_policy.provisioning_model = getattr(
        batch_v1.AllocationPolicy.ProvisioningModel, provisioning_model
    )

    instances = batch_v1.AllocationPolicy.InstancePolicyOrTemplate()
    instances.policy = instance_policy

    service_account = batch_v1.ServiceAccount()
    service_account.email = service_account_email

    allocation_policy = batch_v1.AllocationPolicy()
    allocation_policy.instances = [instances]
    allocation_policy.service_account = service_account

    logs_policy = batch_v1.LogsPolicy()
    logs_policy.destination = batch_v1.LogsPolicy.Destination.CLOUD_LOGGING

    container = batch_v1.Runnable.Container()
    container.image_uri = container_image_uri
    container.entrypoint = "/bin/bash"

    # construct the full command for the `run-all` command
    command_parts = [
        "python",
        "-m",
        "arret",
        "run-all",
        "--workspace-namespace",
        workspace_namespace,
        "--workspace-name",
        workspace_name,
        "--gcp-project-id",
        gcp_project_id,
        "--inventory-path",
        "/app/data/inventories/inventory.ndjson",
        "--plan-path",
        "/app/data/plans/plan.duckdb",
        "--days-considered-old",
        str(days_considered_old),
        "--bytes-considered-large",
        str(bytes_considered_large),
    ]

    for x in other_workspaces:
        command_parts.append("--other-workspaces")
        command_parts.append("/".join([x["workspace_namespace"], x["workspace_name"]]))

    # running `python` command via default `/bin/bash` entrypoint, so prepend `-c`
    container.commands = ["-c", " ".join(command_parts)]

    runnable = batch_v1.Runnable()
    runnable.display_name = "arret"
    runnable.container = container

    compute_resource = batch_v1.ComputeResource()
    cpu_mem = get_machine_type_info(gcp_project_id, zone, machine_type)
    compute_resource.cpu_milli = cpu_mem["cpus"] * 1000
    compute_resource.memory_mib = cpu_mem["memory_mib"]
    compute_resource.boot_disk_mib = boot_disk_mib

    task_spec = batch_v1.TaskSpec()
    task_spec.runnables = [runnable]
    task_spec.compute_resource = compute_resource
    task_spec.max_retry_count = 0
    task_spec.max_run_duration = f"{max_run_seconds}s"  # type: ignore

    task_group = batch_v1.TaskGroup()
    task_group.task_count = 1
    task_group.parallelism = 1
    task_group.task_spec = task_spec

    job = batch_v1.Job()
    job.allocation_policy = allocation_policy
    job.logs_policy = logs_policy
    job.task_groups = [task_group]

    job_request = batch_v1.CreateJobRequest()
    job_request.job = job
    job_id = make_batch_job_id(workspace_name)
    job_request.job_id = job_id
    job_request.parent = f"projects/{gcp_project_id}/locations/{region}"

    batch_client = batch_v1.BatchServiceClient()

    res = batch_client.create_job(job_request)
    logging.info(f"Job submitted: {res.name} (uid: {res.uid})")
    logging.info(
        "".join(
            [
                "Track at https://console.cloud.google.com/batch/jobsDetail/regions/",
                region,
                "/jobs/",
                job_id,
                "/details?project=",
                gcp_project_id,
            ]
        )
    )


def make_batch_job_id(workspace_name: str) -> str:
    """
    Generate a valid job ID for GCP Batch that shows the Terra workspace name, the
    current date, and a random string, while not exceeding the 63 character limit.

    :param workspace_name: the name of the Terra workspace
    :return: an ID to use as the GCP Batch job ID
    """

    job_id_parts = [
        f"arret-{re.sub(r'[^a-z0-9]', '-', workspace_name.lower())}",
        datetime.datetime.now().strftime("%Y-%m-%d"),
        str(uuid4()).split("-")[0],
    ]

    avail_job_id_len = 63 - (len(job_id_parts[1]) + len(job_id_parts[2]) + 2)
    job_id_parts[0] = job_id_parts[0][:avail_job_id_len]

    return "-".join(job_id_parts)


def get_machine_type_info(
    gcp_project_id: str, zone: str, machine_type: str
) -> dict[str, int]:
    """
    Look up a GCP machine type and get its CPU and memory.

    :param gcp_project_id: a GCP project ID
    :param zone: the GCP zone (e.g. "us-central1-a") to query for machine type info
    :param machine_type: the name of the machine type (e.g. "n2-highcpu-4")
    :return: number of CPUs and memory (in MiB) for the machine type
    """

    machine_types_client = compute_v1.MachineTypesClient()

    machine_type_info = machine_types_client.get(
        project=gcp_project_id,
        zone=zone,
        machine_type=machine_type,
    )

    return {
        "cpus": machine_type_info.guest_cpus,
        "memory_mib": machine_type_info.memory_mb * 10**6 // 2**20,
    }
