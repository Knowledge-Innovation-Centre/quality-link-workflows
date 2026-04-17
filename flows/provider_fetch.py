from prefect import flow

from tasks.loaders.extract_deqar_provider import extract_deqar_provider
from tasks.exporters.save_providers_db import save_providers_db
from tasks.transformers.providers_to_rdf import providers_to_rdf
from tasks.exporters.push_providers_jena import push_providers_jena


@flow(name="provider-fetch-database-batch")
def provider_fetch_database_batch(
    api_base_url: str = "https://backend.testzone.eqar.eu/connectapi/v1/providers/",
    limit: int = 2000,
):
    providers = extract_deqar_provider(api_base_url=api_base_url, limit=limit)
    db_result = save_providers_db(data=providers)
    rdf_data = providers_to_rdf(data=db_result)
    push_providers_jena(data=rdf_data)


if __name__ == "__main__":
    provider_fetch_database_batch()
