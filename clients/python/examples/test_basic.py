"""test_basic.py — end-to-end example for the FileDB v2 Python client.

Prerequisites:
    - FileDB server running: `make run` from the repo root.
    - Client installed:      `pip install .` from clients/python.

Run:
    python examples/test_basic.py
"""

from filedbv2 import FileDB


def main() -> None:
    db = FileDB("localhost", 5433, "dev-key")

    # --- Collection management ---
    print("=== Collections ===")
    db.create_collection("test_py")
    print("Created collection. All collections:", db.list_collections())

    # --- Insert ---
    print("\n=== Insert ===")
    id1 = db.insert("test_py", {"name": "Alice", "age": 30, "role": "admin"})
    id2 = db.insert("test_py", {"name": "Bob", "age": 25, "role": "user"})
    id3 = db.insert("test_py", {"name": "Carol", "age": 35, "role": "admin"})
    print("Inserted IDs:", id1, id2, id3)

    ids = db.insert_many(
        "test_py",
        [
            {"name": "Dave", "age": 28, "role": "user"},
            {"name": "Eve", "age": 22, "role": "user"},
        ],
    )
    print("InsertMany IDs:", ids)

    # --- Find by ID ---
    print("\n=== FindById ===")
    print("Record:", db.find_by_id("test_py", id1))

    # --- Find with filter ---
    print("\n=== Find (filter: role=admin) ===")
    admins = db.find(
        "test_py", {"field": "role", "op": "eq", "value": "admin"}, order_by="name"
    )
    print("Admins:", [f"{r['id']}: {r['data']}" for r in admins])

    # --- AND filter ---
    print("\n=== Find (AND: role=user AND age>=25) ===")
    filtered = db.find(
        "test_py",
        {
            "and": [
                {"field": "role", "op": "eq", "value": "user"},
                {"field": "age", "op": "gte", "value": "25"},
            ]
        },
    )
    print("Filtered:", [r["data"] for r in filtered])

    # --- Find with limit ---
    print("\n=== Find (limit 2) ===")
    for r in db.find("test_py", limit=2):
        print(" -", r["id"], r["data"])

    # --- Update ---
    print("\n=== Update ===")
    db.update("test_py", id1, {"name": "Alice", "age": 31, "role": "superadmin"})
    print("Updated:", db.find_by_id("test_py", id1))

    # --- Delete ---
    print("\n=== Delete ===")
    print("Deleted id2:", db.delete("test_py", id2))

    # --- Indexes ---
    print("\n=== Indexes ===")
    db.ensure_index("test_py", "role")
    print("Indexes:", db.list_indexes("test_py"))

    indexed = db.find("test_py", {"field": "role", "op": "eq", "value": "user"})
    print("Users (via index):", [r["data"] for r in indexed])

    db.drop_index("test_py", "role")
    print("Indexes after drop:", db.list_indexes("test_py"))

    # --- Transactions ---
    print("\n=== Transactions ===")
    tx_id = db.begin_tx("test_py")
    print("TX ID:", tx_id)
    print("Committed:", db.commit_tx(tx_id))

    tx_id2 = db.begin_tx("test_py")
    print("Rolled back:", db.rollback_tx(tx_id2))

    # --- Stats ---
    print("\n=== Stats ===")
    print("Stats:", db.stats("test_py"))

    # --- TTL (per-record and per-collection default) ---
    print("\n=== TTL ===")
    db.create_collection("test_py_ttl", default_ttl_seconds=3600)
    db.insert("test_py_ttl", {"kind": "inherits-collection-default"})
    db.insert("test_py_ttl", {"kind": "own-ttl"}, ttl_seconds=60)
    print("TTL collection stats:", db.stats("test_py_ttl"))
    db.drop_collection("test_py_ttl")

    # --- Maintenance ---
    print("\n=== Compact ===")
    print("Compacted:", db.compact("test_py"))

    # --- Backup ---
    print("\n=== Snapshot ===")
    n = db.snapshot_to_file("filedb-backup.tar.gz")
    print(f"Wrote {n} bytes to filedb-backup.tar.gz (restore with: tar xzf ...)")

    # --- Cleanup ---
    print("\n=== Cleanup ===")
    db.drop_collection("test_py")
    print("Collections after drop:", db.list_collections())

    db.close()
    print("\nAll done!")


if __name__ == "__main__":
    main()
