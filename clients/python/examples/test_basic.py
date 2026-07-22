"""test_basic.py — end-to-end example for the ScrivaDB Python client.

Prerequisites:
    - ScrivaDB server running: `make run` from the repo root.
    - Client installed:      `pip install .` from clients/python.

Run:
    python examples/test_basic.py
"""

from scriva import AlreadyExistsError, ScrivaDB, NotFoundError


def main() -> None:
    db = ScrivaDB("localhost", 5433, "dev-key")

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

    # --- Projection (N2): only return selected fields ---
    print("\n=== Find with projection (fields=['name']) ===")
    projected = db.find("test_py", fields=["name"])
    print("Projected:", [r["data"] for r in projected])

    # --- Multi-field sort (N3) ---
    print("\n=== Find (multi-field order_by: role asc, age desc) ===")
    sorted_recs = db.find(
        "test_py", order_by=[("role", False), ("age", True)]
    )
    print("Sorted:", [(r["data"].get("role"), r["data"].get("age")) for r in sorted_recs])

    # --- Keyset pagination (N3) ---
    print("\n=== Keyset pagination (limit=2, order_by=age) ===")
    page, token = db.find_page("test_py", limit=2, order_by="age")
    print("Page 1:", [r["data"].get("name") for r in page])
    while token:
        page, token = db.find_page("test_py", limit=2, order_by="age", page_token=token)
        print("Next page:", [r["data"].get("name") for r in page])

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

    # --- Keyed CRUD, Upsert & CAS (N1) ---
    print("\n=== Keyed CRUD (N1) ===")
    rec = db.upsert("test_py", "user:alice", {"name": "Alice", "score": 10})
    print("Upsert (insert):", rec)  # rev == 1
    rec = db.upsert("test_py", "user:alice", {"name": "Alice", "score": 20})
    print("Upsert (replace):", rec)  # rev == 2

    print("FindByKey:", db.find_by_key("test_py", "user:alice"))

    updated = db.update_by_key("test_py", "user:alice", {"name": "Alice", "score": 30})
    print("UpdateByKey:", updated)  # rev == 3

    # Keyed create — a duplicate key is rejected with AlreadyExistsError.
    kid = db.insert("test_py", {"name": "Frank"}, key="user:frank")
    print("Keyed insert id:", kid)
    try:
        db.insert("test_py", {"name": "Frank II"}, key="user:frank")
    except AlreadyExistsError as e:
        print("Duplicate key rejected as expected:", type(e).__name__)

    # Compare-and-swap on rev.
    current_rev = db.find_by_key("test_py", "user:alice")["rev"]
    cas = db.update_if_rev(
        "test_py", "user:alice", current_rev, {"name": "Alice", "score": 40}
    )
    print("CAS (fresh rev) swapped:", cas["swapped"], "-> rev", cas["record"]["rev"])
    stale = db.update_if_rev(
        "test_py", "user:alice", current_rev, {"name": "Alice", "score": 99}
    )
    print("CAS (stale rev) swapped:", stale["swapped"])  # False, clean no-op

    print("DeleteByKey:", db.delete_by_key("test_py", "user:frank"))
    try:
        db.find_by_key("test_py", "user:frank")
    except NotFoundError as e:
        print("Missing key raises as expected:", type(e).__name__)

    # --- Aggregations (N4) ---
    print("\n=== Aggregations (N4) ===")
    print("Total count:", db.count("test_py"))
    print(
        "Admin count:",
        db.count("test_py", {"field": "role", "op": "eq", "value": "admin"}),
    )
    by_role = db.group_by(
        "test_py", "role", aggregations=["sum", "avg", "min", "max"], metric="age"
    )
    for g in by_role:
        print(f"  role={g['group']!r} count={g['count']} numeric={g['numeric']}", end="")
        if g["numeric"]:
            print(f" sum={g['sum']} avg={g['avg']:.1f} min={g['min']} max={g['max']}")
        else:
            print()

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
    n = db.snapshot_to_file("scriva-backup.tar.gz")
    print(f"Wrote {n} bytes to scriva-backup.tar.gz (restore with: tar xzf ...)")

    # --- Cleanup ---
    print("\n=== Cleanup ===")
    db.drop_collection("test_py")
    print("Collections after drop:", db.list_collections())

    db.close()
    print("\nAll done!")


if __name__ == "__main__":
    main()
