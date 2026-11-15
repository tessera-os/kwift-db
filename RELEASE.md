# 26.11.2

**Project:** KwiftDB  
**Release week:** 2026-11-09 – 2026-11-15

## Highlights

- KwiftDB, the replicated database with its sync server, replicas and sync over QUIC (`KwiftDBQUIC`), moved into the Tessera monorepo and is published as `tessera-os/kwift-db`.

## Added

- `Replica.keepLingering(_:)`: keeps up to a given number of queries subscribed after `unsubscribe`, newest kept. Subscribing again with the same name and arguments takes the lingering query back with its current result and readiness, so a page opened again shows at once with no round trip. Lingering queries report no events; the oldest past the limit are unsubscribed for real. The default, 0, keeps the spec's behaviour. `names` limits it to some queries, so one-off subscriptions (a search per keystroke) end at once instead of pushing the others out.

- CI on macOS 27, the iOS 27 simulator and Linux (x86_64 and aarch64), and the weekly source release.

## Validation

- The new CI had not run yet when these notes were written.
