# Minimal Golden Review Fixture

This fixture is a tiny, versioned review sample for smoke, regression, and deterministic-output checks.

Its current purpose is to provide a stable review-sized input surface for:

- `StudyTable`-based downstream builders
- `review_descriptives_builder.py`
- golden regression tests around canonical analytics outputs

This is intentionally small.

It is not yet a full repository-aligned `daily_run.sh` fixture.

The current fixture surface includes:

- a minimal `review.toml`
- extraction input
- quality-band input
- expected analytics outputs for regression comparison

This makes it useful for:

- CI smoke
- regression checks
- documentation examples
- deterministic builder validation
