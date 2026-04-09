from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path

ALLOWED_REVIEW_MODES = {"template", "production"}
ALLOWED_FAIL_ON_VALUES = {"none", "minor", "major", "critical"}
REQUIRED_STAGE_KEYS = (
    "search",
    "deduplication",
    "screening",
    "extraction",
    "synthesis",
    "reporting",
)


class ReviewConfigError(ValueError):
    """Raised when a review.toml file is missing required structure or values."""


@dataclass(frozen=True)
class ReviewConfig:
    config_path: Path
    review_root: Path
    review_id: str
    title: str
    data_root: Path
    protocol_root: Path
    outputs_root: Path
    manuscript_root: Path
    review_mode: str
    stages: dict[str, bool]
    fail_on: str
    priority_policy: str | None
    output_profile: str | None


def _require_table(data: dict[str, object], key: str) -> dict[str, object]:
    value = data.get(key)
    if not isinstance(value, dict):
        raise ReviewConfigError(f"Missing required table `[{key}]` in review config.")
    return value


def _require_string(table: dict[str, object], key: str, *, table_name: str) -> str:
    value = table.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ReviewConfigError(f"Missing required string `{table_name}.{key}` in review config.")
    return value.strip()


def _optional_string(table: dict[str, object], key: str) -> str | None:
    value = table.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ReviewConfigError(f"Field `{key}` must be a non-empty string when provided.")
    return value.strip()


def _resolve_relative_path(raw_value: str, *, field_name: str, review_root: Path) -> Path:
    path = Path(raw_value)
    if path.is_absolute():
        raise ReviewConfigError(
            f"Config path `{field_name}` must be relative to the review root, not absolute."
        )
    return (review_root / path).resolve()


def load_review_config(config_path: str | Path) -> ReviewConfig:
    config_file = Path(config_path).resolve()
    if not config_file.exists():
        raise ReviewConfigError(f"Review config not found: {config_file}")
    if config_file.name != "review.toml":
        raise ReviewConfigError(
            f"Review config must be a `review.toml` file, got `{config_file.name}`."
        )

    try:
        data = tomllib.loads(config_file.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise ReviewConfigError(f"Invalid TOML in review config: {exc}") from exc

    review_root = config_file.parent

    review_table = _require_table(data, "review")
    paths_table = _require_table(data, "paths")
    mode_table = _require_table(data, "mode")
    stages_table = _require_table(data, "stages")
    status_table = _require_table(data, "status")
    output_table = data.get("output")
    if output_table is not None and not isinstance(output_table, dict):
        raise ReviewConfigError("Field `[output]` must be a table when provided.")

    review_id = _require_string(review_table, "id", table_name="review")
    title = _require_string(review_table, "title", table_name="review")

    data_root = _resolve_relative_path(
        _require_string(paths_table, "data_root", table_name="paths"),
        field_name="paths.data_root",
        review_root=review_root,
    )
    protocol_root = _resolve_relative_path(
        _require_string(paths_table, "protocol_root", table_name="paths"),
        field_name="paths.protocol_root",
        review_root=review_root,
    )
    outputs_root = _resolve_relative_path(
        _require_string(paths_table, "outputs_root", table_name="paths"),
        field_name="paths.outputs_root",
        review_root=review_root,
    )
    manuscript_root = _resolve_relative_path(
        _require_string(paths_table, "manuscript_root", table_name="paths"),
        field_name="paths.manuscript_root",
        review_root=review_root,
    )

    review_mode = _require_string(mode_table, "review_mode", table_name="mode")
    if review_mode not in ALLOWED_REVIEW_MODES:
        allowed = ", ".join(sorted(ALLOWED_REVIEW_MODES))
        raise ReviewConfigError(
            f"Invalid mode.review_mode `{review_mode}`; expected one of: {allowed}."
        )

    stages: dict[str, bool] = {}
    for key in REQUIRED_STAGE_KEYS:
        value = stages_table.get(key)
        if not isinstance(value, bool):
            raise ReviewConfigError(f"Stage toggle `stages.{key}` must be a boolean.")
        stages[key] = value

    fail_on = _require_string(status_table, "fail_on", table_name="status")
    if fail_on not in ALLOWED_FAIL_ON_VALUES:
        allowed = ", ".join(sorted(ALLOWED_FAIL_ON_VALUES))
        raise ReviewConfigError(f"Invalid status.fail_on `{fail_on}`; expected one of: {allowed}.")

    priority_policy = None
    if "priority_policy" in status_table:
        priority_policy = _optional_string(status_table, "priority_policy")

    output_profile = None
    if output_table is not None and "profile" in output_table:
        output_profile = _optional_string(output_table, "profile")

    return ReviewConfig(
        config_path=config_file,
        review_root=review_root,
        review_id=review_id,
        title=title,
        data_root=data_root,
        protocol_root=protocol_root,
        outputs_root=outputs_root,
        manuscript_root=manuscript_root,
        review_mode=review_mode,
        stages=stages,
        fail_on=fail_on,
        priority_policy=priority_policy,
        output_profile=output_profile,
    )
