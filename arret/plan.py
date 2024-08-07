from arret.terra import TerraWorkspace
from arret.utils import extract_unique_values


def make_plan(
    workspace_namespace: str, workspace_name: str, gcp_project_id: str
) -> None:
    gs_urls = get_gs_urls(workspace_namespace, workspace_name)


def get_gs_urls(workspace_namespace: str, workspace_name: str) -> set[str]:
    tw = TerraWorkspace(workspace_namespace, workspace_name)
    bucket_name = tw.get_bucket_name()
    entity_types = tw.get_entity_types()
    entities = {k: tw.get_entities(k) for k in entity_types}

    gs_urls = set()

    for _, df in entities.items():
        unique_values = extract_unique_values(df)

        gs_urls.update(
            {
                x
                for x in unique_values
                if isinstance(x, str) and x.startswith(f"gs://{bucket_name}/")
            }
        )

    return gs_urls
