from prefect import flow

from tasks.conditionals.check_key_exists_db import check_key_exists_db
from tasks.exporters.save_key_pair_db import save_key_pair_db


@flow(name="push-private-keys")
def push_private_keys():
    needs_key = check_key_exists_db()
    if needs_key:
        save_key_pair_db()


if __name__ == "__main__":
    push_private_keys()
