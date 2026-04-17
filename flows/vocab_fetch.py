from prefect import flow

from tasks.loaders.fetch_lang_json import fetch_lang_json
from tasks.transformers.transform_lang_dict import transform_lang_dict
from tasks.exporters.push_lang_jena import push_lang_jena


@flow(name="vocab-fetch-jena-batch")
def vocab_fetch_jena_batch(concept_scheme: str):
    concepts = fetch_lang_json(concept_scheme=concept_scheme)
    turtle_data = transform_lang_dict(data=concepts, concept_scheme=concept_scheme)
    push_lang_jena(data=turtle_data, concept_scheme=concept_scheme)


if __name__ == "__main__":
    vocab_fetch_jena_batch(concept_scheme="http://data.europa.eu/snb/isced-f/25831c2")
