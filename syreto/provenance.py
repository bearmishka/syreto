from __future__ import annotations

import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from review_config import ReviewConfigError, load_review_config
else:
    from .review_config import ReviewConfigError, load_review_config


def atomic_replace_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.tmp.", dir=path.parent)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
        tmp_path.replace(path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    atomic_replace_bytes(path, text.encode(encoding))


def provenance_sidecar_path(artifact_path: str | Path) -> Path:
    path = Path(artifact_path)
    return path.with_name(f"{path.name}.provenance.json")


def _load_review_context(config_path: Path | None) -> dict[str, object]:
    if config_path is None:
        return {}
    try:
        review_config = load_review_config(config_path)
    except (ReviewConfigError, OSError):
        return {"review_config": str(config_path)}

    return {
        "review_config": str(review_config.config_path),
        "review_id": review_config.review_id,
        "output_profile": review_config.output_profile,
    }


def write_provenance_sidecar(
    artifact_path: str | Path,
    *,
    generated_by: str,
    upstream_inputs: list[str | Path],
    review_mode: str | None = None,
    review_config_path: str | Path | None = None,
) -> Path:
    artifact = Path(artifact_path)
    config_path = (
        Path(review_config_path).resolve()
        if review_config_path is not None
        else (
            Path(os.environ["SYRETO_REVIEW_CONFIG"]).resolve()
            if os.getenv("SYRETO_REVIEW_CONFIG")
            else None
        )
    )

    payload: dict[str, object] = {
        "artifact_path": str(artifact),
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "generated_by": generated_by,
        "upstream_inputs": [str(Path(value)) for value in upstream_inputs],
        "review_mode": (review_mode or os.getenv("REVIEW_MODE") or "unknown").strip() or "unknown",
    }
    payload.update(_load_review_context(config_path))

    sidecar_path = provenance_sidecar_path(artifact)
    atomic_write_text(sidecar_path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return sidecar_path
