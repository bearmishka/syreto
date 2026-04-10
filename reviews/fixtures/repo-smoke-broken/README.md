# Repo Smoke Broken Review Fixture

This fixture is the failure-path counterpart to `repo-smoke`.

It exists to check that a repository-aligned smoke review not only succeeds
cleanly on the happy path, but also fails honestly when a required pipeline
step breaks.

Its current purpose is failure-semantics regression:

- non-zero exit code
- failed manifest state
- failed-marker creation
- failed run-event capture
- post-failure status refresh
