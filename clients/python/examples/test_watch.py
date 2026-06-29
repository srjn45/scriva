"""test_watch.py — demonstrates the Watch streaming RPC.

Prerequisites:
    - FileDB server running: `make run` from the repo root.
    - Client installed:      `pip install .` from clients/python.

Run:
    python examples/test_watch.py
"""

import threading
import time

from filedbv2 import FileDB

COLLECTION = "watch_test_py"
INSERT_COUNT = 5


def main() -> None:
    db = FileDB("localhost", 5433, "dev-key")

    # Clean up from any previous run.
    if COLLECTION in db.list_collections():
        db.drop_collection(COLLECTION)
    db.create_collection(COLLECTION)

    print(f'Watching "{COLLECTION}" — will insert {INSERT_COUNT} records...')

    # Insert in a background thread so the main thread can consume the stream.
    def inserter() -> None:
        # A separate client/channel for the writer.
        writer = FileDB("localhost", 5433, "dev-key")
        for i in range(1, INSERT_COUNT + 1):
            time.sleep(0.3)
            rid = writer.insert(
                COLLECTION, {"msg": f"record-{i}", "ts": time.time()}
            )
            print(f"  [insert] id={rid}")
        writer.close()

    threading.Thread(target=inserter, daemon=True).start()

    # Collect exactly INSERT_COUNT watch events, then stop.
    received = 0
    for event in db.watch(COLLECTION):
        print(
            f"  [watch]  op={event['op']} id={event['record']['id']} "
            f"data={event['record']['data']}"
        )
        received += 1
        if received >= INSERT_COUNT:
            break

    db.drop_collection(COLLECTION)
    db.close()
    print("Watch test done.")


if __name__ == "__main__":
    main()
