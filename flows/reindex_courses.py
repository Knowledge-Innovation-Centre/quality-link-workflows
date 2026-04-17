from prefect import flow

from tasks.transformers.transform_index_gold import transform_index_gold


@flow(name="reindex-courses")
def reindex_courses(
    provider_uuid: str,
    source_version_uuid: str = "",
    source_type: str = "unknown",
    course_uuids: list[str] = [],
):
    transform_index_gold(messages=[{
        "provider_uuid": provider_uuid,
        "source_version_uuid": source_version_uuid,
        "source_type": source_type,
        "course_uuids": course_uuids,
    }])


if __name__ == "__main__":
    reindex_courses(provider_uuid="example-uuid", course_uuids=[])
