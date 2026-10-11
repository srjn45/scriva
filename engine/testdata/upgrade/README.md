# Upgrade fixtures

`v1.4.0/` is a data directory written by the released v1.4.0 engine (the last
version before cross-collection transactions): `engine.Open` with
`SegmentMaxSize: 400`, collections `accounts`, `audit`, `ledger`, a secondary
index on `kind`, 8 inserts per collection (`kind:"legacy"`, `n:id-1`), one update
of id 2, one delete of id 3, clean `Close`.

It is **input only** — tests copy it (`lcCopyTree`) before opening. It carries no
`xtx.format` / `xtx.journal`, which is what makes it a legacy root. Never edit
it by hand. To regenerate, check out tag `v1.4.0`, run the sequence above with a
throwaway program against an empty directory, and replace this directory;
`TestXTxLifecycle_Upgraded` asserts the exact content, so a mismatch fails
loudly. The tests are deterministic (fixed write sequences, no random seed); a
failure message names the directory condition, which is the replay recipe.
