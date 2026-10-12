# Archive Validator Failure

This attempt stopped before saving any verified case. The archive validator compared in-memory Python tuples with JSON lists inside the TLE configuration and raised a false config mismatch. Serializing both sides consistently fixes the validator; no scheduling or environment behavior changed. The complete rerun is `results/relay_deferred_reservation_20261012_v3/`.
