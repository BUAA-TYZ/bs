# CGS Routing Reference

Unmodified `src/routing.py` from [ApSTL/cgs](https://github.com/ApSTL/cgs),
commit `f97f0c7d66ef9ec55c46fb26ff678a23d9aba248`, retrieved 2026-10-14.
SHA-256: `ee15b1535c6f6cf7454e73fa006a71e6bdc421188cdc8f1876f652b54a7aba36`.
The upstream MIT license is included in `LICENSE`.

This file is used only as a test reference for contact routing, not as the
production scheduler. No upstream datasets or binary artifacts are included.
The complete upstream scheduler and simulation are NOT reproduced by these tests.

The production adapter differs explicitly: complete-payload store-and-forward
timing and discrete steps replace first-byte routing costs; computation completion
replaces image acquisition. See `docs/thesis/literature_baselines.md`.
