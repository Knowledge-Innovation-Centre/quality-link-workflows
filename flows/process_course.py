from prefect import flow

from tasks.transformers.transform_fetch_bronze import transform_fetch_bronze
from tasks.transformers.transform_enrich_silver import transform_enrich_silver
from tasks.transformers.transform_index_gold import transform_index_gold
from tasks.exporters.write_transaction_db import write_transaction_db


@flow(name="process-course-message")
def process_course_message(
    provider_uuid: str,
    source_uuid: str,
    source_version_uuid: str,
):
    """
    Process a single course source through the full bronze -> silver -> gold pipeline.

    Triggered via the Prefect API by the producer, replacing the old Redis queue.
    Each invocation is a tracked flow run with full observability.
    """
    bronze_result = transform_fetch_bronze(message={
        "provider_uuid": provider_uuid,
        "source_uuid": source_uuid,
        "source_version_uuid": source_version_uuid,
    })

    if bronze_result is None:
        print("⚠️ Bronze step returned None, skipping remaining steps")
        return

    silver_result = transform_enrich_silver(messages=[bronze_result])

    if silver_result is None:
        print("⚠️ Silver step returned None, skipping remaining steps")
        return

    gold_result = transform_index_gold(messages=[silver_result])

    write_transaction_db(messages=[gold_result] if gold_result else [silver_result])


if __name__ == "__main__":
    process_course_message(
        provider_uuid="example-uuid",
        source_uuid="example-source",
        source_version_uuid="example-version",
    )
