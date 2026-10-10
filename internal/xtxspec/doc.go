// Package xtxspec is a *specification harness* for the planned cross-collection
// transaction (XTx) on-disk format described in
// docs/design-cross-collection-transactions.md.
//
// It contains only pure parsers, encoders, digests, the recovery truth table and
// the fault-boundary checklist, so the documented wire format can be validated
// by deterministic fixtures before any production code writes a transaction-aware
// byte. It is deliberately NOT imported by engine, server or any command:
// normal opens never see it, and TestNotImportedByProduction enforces that.
package xtxspec
