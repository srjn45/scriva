# Cross-collection journal damage contract

`xtx.journal` is the coordinator's decision record for a cross-collection
transaction.  A recovery implementation must never make a damaged decision
record look like permission to reveal only some of that transaction.

## Observable outcomes

For every transaction whose participant runs can still be read, an attempt to
open a directory with journal damage has exactly three safe outcomes:

| Outcome | Required observation |
| --- | --- |
| Exact PRE | None of the transaction's participant effects are visible. |
| Exact POST | Every participant effect is visible. |
| Fail closed | The open is refused with `ErrXTxJournalCorrupt`, `ErrXTxJournalMissing`, `ErrXTxRecoveryRequired`, or `ErrXTxUnsupported`. |

There is no fourth, best-effort state: if one participant is visible and
another is not, recovery has violated atomicity.  An accepted open must also
be repeatable: a second open cannot change the transaction's visibility.

## Damage states

These states are intentionally distinguishable.  A torn suffix is a normal
crash shape and is not evidence that an earlier complete record is corrupt;
the other states require an explicit, documented recovery outcome rather than
best-effort visibility.

| Damage state | Meaning | Safe handling |
| --- | --- | --- |
| Torn final record or zero-filled final tail | Incomplete final append after a complete prefix | Retain the complete prefix, discard only the tail, then recover by its decision. |
| Complete record with invalid checksum, malformed header, or corruption in the middle | A decision that may have been durable is no longer trustworthy | Typed fail-closed refusal. |
| Missing journal with stamped participant runs | Coordinator evidence is absent | Typed fail-closed refusal; never presume abort automatically. |
| Empty/zeroed journal with stamped participant runs | Destructive journal loss, not a clean new database | Typed fail-closed refusal with `ErrXTxRecoveryRequired`. Never partial visibility. |
| Complete COMMIT followed by a torn/garbage tail | The complete COMMIT remains evidence | Exact POST, or a typed refusal if the implementation cannot establish that boundary. |

The implementation chooses fail-closed for destructive journal loss.  Recovery
does not rewrite an empty or all-zero `xtx.journal` into a fresh coordinator
when stamped participant runs are present, because non-tail participant runs
cannot always be safely rolled back into exact PRE.  It does still discard an
unterminated final append after a complete prefix, including a complete COMMIT
followed by a torn suffix, and then recovers from that complete prefix.

## v1.5.0 regression harness

`TestXTxJournalZeroedDamageContract` creates a fixed crash image at the
durable-COMMIT hook, truncates only `xtx.journal` to zero bytes, and reopens
the image.  This is the scenario in Warden PR 917's `TestJournalDamage`.

The published v1.5.0 module was checked with the exact PR 917 test on 2026-10-11:
every zeroed-journal workflow reopened as exact PRE, not mixed.  This local
harness independently checks the same atomic-visibility invariant for a
durable-COMMIT crash image.  It is a regression test, not a production recovery
change.  The Warden contract must not be weakened: it rejects a mixed state.
