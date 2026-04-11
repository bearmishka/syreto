#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RUN_KEYWORD_ANALYSIS="${RUN_KEYWORD_ANALYSIS:-0}"
RUN_POLYGLOT_TRANSLATION="${RUN_POLYGLOT_TRANSLATION:-0}"
RUN_CITATION_TRACKING="${RUN_CITATION_TRACKING:-1}"
RUN_PUBLICATION_BIAS="${RUN_PUBLICATION_BIAS:-1}"
RUN_PROSPERO_DRAFTER="${RUN_PROSPERO_DRAFTER:-1}"
RUN_MULTILANG_ABSTRACT_SCREENER="${RUN_MULTILANG_ABSTRACT_SCREENER:-1}"
RUN_RETRACTION_CHECKER="${RUN_RETRACTION_CHECKER:-1}"
RUN_LIVING_REVIEW_SCHEDULER="${RUN_LIVING_REVIEW_SCHEDULER:-0}"
RUN_REVIEWER_WORKLOAD_BALANCER="${RUN_REVIEWER_WORKLOAD_BALANCER:-1}"
RUN_WEEKLY_RISK_DIGEST="${RUN_WEEKLY_RISK_DIGEST:-1}"
RUN_TRANSPARENCY_APPENDIX_SYNC="${RUN_TRANSPARENCY_APPENDIX_SYNC:-1}"
DECISION_TRACE_MAX_ROWS="${DECISION_TRACE_MAX_ROWS:-}"
DECISION_TRACE_MARKDOWN_MAX_ROWS_EXPLICIT="${DECISION_TRACE_MARKDOWN_MAX_ROWS:-}"
DECISION_TRACE_MARKDOWN_MAX_ROWS="${DECISION_TRACE_MARKDOWN_MAX_ROWS_EXPLICIT:-${DECISION_TRACE_MAX_ROWS:-50}}"
DECISION_TRACE_MAX_ROWS_LATEX="${DECISION_TRACE_MAX_ROWS_LATEX:-}"
DECISION_TRACE_LATEX_MAX_ROWS_EXPLICIT="${DECISION_TRACE_LATEX_MAX_ROWS:-}"
DECISION_TRACE_LATEX_MAX_ROWS="${DECISION_TRACE_LATEX_MAX_ROWS_EXPLICIT:-${DECISION_TRACE_MAX_ROWS_LATEX:-25}}"
RUN_PREFLIGHT_PLACEHOLDER_GUARD="${RUN_PREFLIGHT_PLACEHOLDER_GUARD:-warn}"
RUN_LIVING_REVIEW_CADENCE_CHECK="${RUN_LIVING_REVIEW_CADENCE_CHECK:-1}"
STATUS_CLI_SNAPSHOT="${STATUS_CLI_SNAPSHOT:-outputs/status_cli_snapshot.txt}"
REVIEW_MODE="${REVIEW_MODE:-template}"
STATUS_PRIORITY_POLICY="${STATUS_PRIORITY_POLICY:-priority_policy.json}"
STATUS_FAIL_ON="${STATUS_FAIL_ON:-}"
RUN_TEMPLATE_TERM_GUARD="${RUN_TEMPLATE_TERM_GUARD:-warn}"
DAILY_RUN_MANIFEST="${DAILY_RUN_MANIFEST:-outputs/daily_run_manifest.json}"
DAILY_RUN_FAILED_MARKER="${DAILY_RUN_FAILED_MARKER:-outputs/daily_run_failed.marker}"
RUN_EVENTS_PATH="${RUN_EVENTS_PATH:-outputs/run_events.jsonl}"
AUDIT_LOG_PATH="${AUDIT_LOG_PATH:-../02_data/processed/audit_log.csv}"
DAILY_RUN_ID="${DAILY_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
export DAILY_RUN_ID
DAILY_RUN_STARTED_AT_UTC="${DAILY_RUN_STARTED_AT_UTC:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
DAILY_RUN_TRANSACTIONAL="${DAILY_RUN_TRANSACTIONAL:-1}"
DAILY_RUN_KEEP_TRANSACTION_SNAPSHOT="${DAILY_RUN_KEEP_TRANSACTION_SNAPSHOT:-0}"
DAILY_RUN_TRANSACTION_ROOT="${DAILY_RUN_TRANSACTION_ROOT:-.daily_run_transaction}"
DAILY_RUN_TRANSACTION_PATHS_RAW="${DAILY_RUN_TRANSACTION_PATHS:-}"
DAILY_RUN_TRANSACTION_SNAPSHOT_DIR="${DAILY_RUN_TRANSACTION_ROOT}/${DAILY_RUN_ID}"
TRANSACTION_SNAPSHOT_MANIFEST="${DAILY_RUN_TRANSACTION_SNAPSHOT_DIR}/snapshot_manifest.tsv"
TRANSACTION_SNAPSHOT_READY=0

declare -a DAILY_RUN_TRANSACTION_PATHS_LIST
if [[ -n "$DAILY_RUN_TRANSACTION_PATHS_RAW" ]]; then
  IFS=':' read -r -a DAILY_RUN_TRANSACTION_PATHS_LIST <<< "$DAILY_RUN_TRANSACTION_PATHS_RAW"
else
  DAILY_RUN_TRANSACTION_PATHS_LIST=(
    "outputs"
    "../02_data/processed"
    "../04_manuscript/tables"
  )
fi

normalize_mode_alias() {
  local var_name="$1"
  local mode_ref
  mode_ref="$(eval "printf '%s' \"\${$var_name}\"")"

  if [[ "$mode_ref" == "1" ]]; then
    echo "[daily_run] Compatibility alias detected: ${var_name}=1 -> fail"
    printf -v "$var_name" '%s' "fail"
  elif [[ "$mode_ref" == "0" ]]; then
    echo "[daily_run] Compatibility alias detected: ${var_name}=0 -> skip"
    printf -v "$var_name" '%s' "skip"
  fi
}

normalize_mode_alias RUN_PREFLIGHT_PLACEHOLDER_GUARD
normalize_mode_alias RUN_TEMPLATE_TERM_GUARD

validate_enum_mode() {
  local var_name="$1"
  local value="$2"
  local allowed_values="$3"
  local expected_message="$4"

  local allowed
  for allowed in $allowed_values; do
    if [[ "$value" == "$allowed" ]]; then
      return 0
    fi
  done

  echo "[daily_run] Invalid ${var_name}: '$value'"
  echo "[daily_run] Expected one of: $expected_message"
  exit 2
}

validate_binary_mode() {
  local var_name="$1"
  local value="$2"

  if [[ "$value" != "0" && "$value" != "1" ]]; then
    echo "[daily_run] Invalid ${var_name}: '$value'"
    echo "[daily_run] Expected one of: 0 | 1"
    exit 2
  fi
}

validate_positive_integer_mode() {
  local var_name="$1"
  local value="$2"

  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "[daily_run] Invalid ${var_name}: '$value'"
    echo "[daily_run] Expected positive integer (>0)"
    exit 2
  fi
}

resolve_status_fail_on_default() {
  local policy_path="$1"
  local fallback="$2"
  if ! command -v python3 >/dev/null 2>&1; then
    echo "$fallback"
    return 0
  fi

  local resolved=""
  resolved="$(python3 - "$policy_path" "$fallback" <<'PY'
import json
import sys
from pathlib import Path


VALID = {"none", "critical", "major", "minor"}


def main(policy_arg: str, fallback: str) -> int:
    policy_path = Path(policy_arg)
    if not policy_path.exists():
        print(fallback)
        return 0

    try:
        parsed = json.loads(policy_path.read_text(encoding="utf-8"))
    except Exception:
        print(fallback)
        return 0

    value = None
    if isinstance(parsed, dict):
        fail_thresholds = parsed.get("fail_thresholds")
        if isinstance(fail_thresholds, dict):
            value = fail_thresholds.get("default")

        if value is None:
            warnings = parsed.get("warnings")
            if isinstance(warnings, dict):
                value = warnings.get("default")

    normalized = str(value if value is not None else fallback).strip().lower()
    if normalized not in VALID:
        normalized = fallback

    print(normalized)
    return 0


raise SystemExit(main(sys.argv[1], sys.argv[2]))
PY
)"

  case "$resolved" in
    none|critical|major|minor)
      echo "$resolved"
      ;;
    *)
      echo "$fallback"
      ;;
  esac
}

validate_binary_mode "DAILY_RUN_TRANSACTIONAL" "$DAILY_RUN_TRANSACTIONAL"
validate_binary_mode "DAILY_RUN_KEEP_TRANSACTION_SNAPSHOT" "$DAILY_RUN_KEEP_TRANSACTION_SNAPSHOT"
validate_binary_mode "RUN_TRANSPARENCY_APPENDIX_SYNC" "$RUN_TRANSPARENCY_APPENDIX_SYNC"
validate_positive_integer_mode "DECISION_TRACE_MARKDOWN_MAX_ROWS" "$DECISION_TRACE_MARKDOWN_MAX_ROWS"
validate_positive_integer_mode "DECISION_TRACE_LATEX_MAX_ROWS" "$DECISION_TRACE_LATEX_MAX_ROWS"

if [[ -z "$STATUS_FAIL_ON" ]]; then
  STATUS_FAIL_ON="$(resolve_status_fail_on_default "$STATUS_PRIORITY_POLICY" "major")"
  echo "[daily_run] STATUS_FAIL_ON not set; using policy default '${STATUS_FAIL_ON}' from ${STATUS_PRIORITY_POLICY}."
fi

if [[ "$RUN_TRANSPARENCY_APPENDIX_SYNC" == "1" ]]; then
  if [[ -n "$DECISION_TRACE_MAX_ROWS" && -z "$DECISION_TRACE_MARKDOWN_MAX_ROWS_EXPLICIT" ]]; then
    echo "[daily_run] Using DECISION_TRACE_MAX_ROWS alias for decision-trace markdown row cap: ${DECISION_TRACE_MARKDOWN_MAX_ROWS}."
  elif [[ -n "$DECISION_TRACE_MAX_ROWS" && -n "$DECISION_TRACE_MARKDOWN_MAX_ROWS_EXPLICIT" ]]; then
    echo "[daily_run] DECISION_TRACE_MAX_ROWS is set but ignored because DECISION_TRACE_MARKDOWN_MAX_ROWS is explicitly set."
  fi

  if [[ -n "$DECISION_TRACE_MAX_ROWS_LATEX" && -z "$DECISION_TRACE_LATEX_MAX_ROWS_EXPLICIT" ]]; then
    echo "[daily_run] Using DECISION_TRACE_MAX_ROWS_LATEX alias for decision-trace LaTeX row cap: ${DECISION_TRACE_LATEX_MAX_ROWS}."
  elif [[ -n "$DECISION_TRACE_MAX_ROWS_LATEX" && -n "$DECISION_TRACE_LATEX_MAX_ROWS_EXPLICIT" ]]; then
    echo "[daily_run] DECISION_TRACE_MAX_ROWS_LATEX is set but ignored because DECISION_TRACE_LATEX_MAX_ROWS is explicitly set."
  fi
fi

case "$RUN_PREFLIGHT_PLACEHOLDER_GUARD" in
  fail)
    echo "[daily_run] Running placeholder preflight guard in fail mode (protocol/search strings)..."
    python template_term_guard.py \
      --scan-path ../01_protocol/protocol.md \
      --scan-path ../01_protocol/search_strings.md \
      --scan-path ../01_protocol/screening_rules.md \
      --scan-path ../01_protocol/pubmed_query_v0.2.txt \
      --check-placeholders \
      --no-check-banned-terms \
      --fail-on-match \
      --summary-output outputs/preflight_placeholder_guard_summary.md
    ;;
  warn)
    echo "[daily_run] Running placeholder preflight guard in warn mode (protocol/search strings)..."
    python template_term_guard.py \
      --scan-path ../01_protocol/protocol.md \
      --scan-path ../01_protocol/search_strings.md \
      --scan-path ../01_protocol/screening_rules.md \
      --scan-path ../01_protocol/pubmed_query_v0.2.txt \
      --check-placeholders \
      --no-check-banned-terms \
      --no-fail-on-match \
      --summary-output outputs/preflight_placeholder_guard_summary.md
    ;;
  skip)
    echo "[daily_run] Skipping placeholder preflight guard (RUN_PREFLIGHT_PLACEHOLDER_GUARD=skip)."
    ;;
  *)
    echo "[daily_run] Invalid RUN_PREFLIGHT_PLACEHOLDER_GUARD: '$RUN_PREFLIGHT_PLACEHOLDER_GUARD'"
    echo "[daily_run] Expected one of: fail | warn | skip (aliases: 1->fail, 0->skip)"
    exit 2
    ;;
esac

validate_enum_mode \
  "REVIEW_MODE" \
  "$REVIEW_MODE" \
  "template production" \
  "template | production"

validate_enum_mode \
  "RUN_TEMPLATE_TERM_GUARD" \
  "$RUN_TEMPLATE_TERM_GUARD" \
  "fail warn skip" \
  "fail | warn | skip (aliases: 1->fail, 0->skip)"

validate_enum_mode \
  "STATUS_FAIL_ON" \
  "$STATUS_FAIL_ON" \
  "none critical major minor" \
  "none | critical | major | minor"

RUN_EVENT_STEP_ORDER=0

utc_now() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}

resolve_python_bin() {
  if command -v python3 >/dev/null 2>&1; then
    echo "python3"
    return 0
  fi
  if command -v python >/dev/null 2>&1; then
    echo "python"
    return 0
  fi
  return 1
}

write_atomic_file_from_stdin() {
  local output_path="$1"
  local output_dir
  output_dir="$(dirname "$output_path")"
  mkdir -p "$output_dir"

  local output_basename
  output_basename="$(basename "$output_path")"
  local tmp_file
  tmp_file="$(mktemp "${output_dir}/.${output_basename}.tmp.XXXXXX")"

  cat > "$tmp_file"
  mv "$tmp_file" "$output_path"
}

normalize_json_object_stream_file() {
  local output_path="$1"
  local label="$2"
  local python_bin=""

  if command -v python3 >/dev/null 2>&1; then
    python_bin="python3"
  elif command -v python >/dev/null 2>&1; then
    python_bin="python"
  else
    echo "[daily_run] WARNING: No Python interpreter available to normalize ${label}."
    return 0
  fi

  "$python_bin" - "$output_path" <<'PY'
import json
import sys
from pathlib import Path


def parse_stream(text: str) -> list[dict]:
    decoder = json.JSONDecoder()
    index = 0
    length = len(text)
    payloads: list[dict] = []

    while index < length:
        while index < length and text[index].isspace():
            index += 1
        if index >= length:
            break

        parsed, consumed = decoder.raw_decode(text, index)
        if not isinstance(parsed, dict):
            raise ValueError("JSON stream element is not an object")
        payloads.append(parsed)
        index = consumed

    if not payloads:
        raise ValueError("No JSON object found in stream")

    return payloads


def main(path_arg: str) -> int:
    target_path = Path(path_arg)
    try:
        raw_text = target_path.read_text(encoding="utf-8")
        payloads = parse_stream(raw_text)
    except Exception:
        return 2

    normalized = json.dumps(payloads[-1], ensure_ascii=False, indent=2) + "\n"
    target_path.write_text(normalized, encoding="utf-8")
    return 0


raise SystemExit(main(sys.argv[1]))
PY

  local normalize_rc=$?
  if [[ "$normalize_rc" -ne 0 ]]; then
    echo "[daily_run] ERROR: failed to normalize ${label} JSON stream (${output_path})."
    return 1
  fi

  return 0
}

write_provenance_sidecar() {
  local artifact_path="$1"
  local generated_by="$2"
  local upstream_inputs_json="${3:-[]}"

  local python_bin=""
  python_bin="$(resolve_python_bin)" || return 0

  ARTIFACT_PATH="$artifact_path" \
  GENERATED_BY="$generated_by" \
  UPSTREAM_INPUTS_JSON="$upstream_inputs_json" \
  PROVENANCE_REVIEW_MODE="$REVIEW_MODE" \
  PROVENANCE_REVIEW_CONFIG="${SYRETO_REVIEW_CONFIG:-}" \
  "$python_bin" - <<'PY'
import json
import os
from datetime import UTC, datetime
from pathlib import Path


def main() -> int:
    artifact_path = Path(os.environ["ARTIFACT_PATH"])
    sidecar_path = artifact_path.with_name(f"{artifact_path.name}.provenance.json")
    try:
        upstream_inputs = json.loads(os.environ["UPSTREAM_INPUTS_JSON"])
    except json.JSONDecodeError:
        upstream_inputs = []

    payload = {
        "artifact_path": str(artifact_path.resolve()),
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "generated_by": os.environ["GENERATED_BY"],
        "upstream_inputs": [str(item) for item in upstream_inputs if str(item).strip()],
        "review_mode": os.environ.get("PROVENANCE_REVIEW_MODE", "").strip() or "unknown",
    }
    review_config = os.environ.get("PROVENANCE_REVIEW_CONFIG", "").strip()
    if review_config:
        payload["review_config"] = review_config

    sidecar_path.parent.mkdir(parents=True, exist_ok=True)
    sidecar_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return 0


raise SystemExit(main())
PY
}

update_daily_run_manifest() {
  local state="$1"
  local pipeline_rc="$2"
  local status_checkpoint_rc="$3"
  local final_rc="$4"
  local failure_phase="$5"
  local rollback_applied="$6"
  local transactional_mode="$7"
  local updated_at_utc
  updated_at_utc="$(utc_now)"

  write_atomic_file_from_stdin "$DAILY_RUN_MANIFEST" <<EOF
{
  "run_id": "$DAILY_RUN_ID",
  "state": "$state",
  "started_at_utc": "$DAILY_RUN_STARTED_AT_UTC",
  "updated_at_utc": "$updated_at_utc",
  "pipeline_exit_code": $pipeline_rc,
  "status_checkpoint_exit_code": $status_checkpoint_rc,
  "final_exit_code": $final_rc,
  "failure_phase": "$failure_phase",
  "rollback_applied": $rollback_applied,
  "transactional_mode": "$transactional_mode"
}
EOF

  normalize_json_object_stream_file "$DAILY_RUN_MANIFEST" "daily-run manifest"
  write_provenance_sidecar "$DAILY_RUN_MANIFEST" "daily_run.sh" '[]'
}

write_daily_run_failed_marker() {
  local pipeline_rc="$1"
  local status_checkpoint_rc="$2"
  local final_rc="$3"
  local failure_phase="$4"
  local failed_at_utc
  failed_at_utc="$(utc_now)"

  write_atomic_file_from_stdin "$DAILY_RUN_FAILED_MARKER" <<EOF
{
  "run_id": "$DAILY_RUN_ID",
  "failed_at_utc": "$failed_at_utc",
  "pipeline_exit_code": $pipeline_rc,
  "status_checkpoint_exit_code": $status_checkpoint_rc,
  "final_exit_code": $final_rc,
  "failure_phase": "$failure_phase"
}
EOF

  normalize_json_object_stream_file "$DAILY_RUN_FAILED_MARKER" "daily-run failed marker"
}

clear_daily_run_failed_marker() {
  rm -f "$DAILY_RUN_FAILED_MARKER"
}

ensure_audit_log_header() {
  local audit_dir
  audit_dir="$(dirname "$AUDIT_LOG_PATH")"
  mkdir -p "$audit_dir"

  if [[ ! -f "$AUDIT_LOG_PATH" || ! -s "$AUDIT_LOG_PATH" ]]; then
    printf 'timestamp,action,file,description\n' > "$AUDIT_LOG_PATH"
    return 0
  fi

  local header_line=""
  IFS= read -r header_line < "$AUDIT_LOG_PATH" || true
  if [[ "$header_line" != "timestamp,action,file,description" ]]; then
    local tmp_file
    tmp_file="$(mktemp "${audit_dir}/.audit_log.tmp.XXXXXX")"
    {
      printf 'timestamp,action,file,description\n'
      cat "$AUDIT_LOG_PATH"
    } > "$tmp_file"
    mv "$tmp_file" "$AUDIT_LOG_PATH"
  fi
}

deduplicate_audit_log_entries() {
  ensure_audit_log_header

  local audit_dir
  audit_dir="$(dirname "$AUDIT_LOG_PATH")"
  local tmp_file
  tmp_file="$(mktemp "${audit_dir}/.audit_log.dedup.tmp.XXXXXX")"

  local header_line=""
  IFS= read -r header_line < "$AUDIT_LOG_PATH" || true

  {
    printf '%s\n' "$header_line"
    tail -n +2 "$AUDIT_LOG_PATH" | awk '
      {
        gsub(/\r$/, "", $0)
        sub(/[[:space:]]+$/, "", $0)
        if (NF && !seen[$0]++) {
          print
        }
      }
    '
  } > "$tmp_file"

  mv "$tmp_file" "$AUDIT_LOG_PATH"
}

append_audit_log_entry() {
  local timestamp="$1"
  local action="$2"
  local file_path="$3"
  local description="$4"

  ensure_audit_log_header

  local safe_description
  safe_description="${description//$'\n'/ }"
  safe_description="${safe_description//$'\r'/ }"
  safe_description="${safe_description//,/;}"

  local entry
  entry="${timestamp},${action},${file_path},${safe_description}"

  if grep -Fqx -- "$entry" "$AUDIT_LOG_PATH"; then
    deduplicate_audit_log_entries
    return 0
  fi

  if [[ -s "$AUDIT_LOG_PATH" ]] && [[ -n "$(tail -c 1 "$AUDIT_LOG_PATH" 2>/dev/null || true)" ]]; then
    printf '\n' >> "$AUDIT_LOG_PATH"
  fi

  printf '%s\n' "$entry" >> "$AUDIT_LOG_PATH"
  deduplicate_audit_log_entries
}

transactional_mode_label() {
  if [[ "$DAILY_RUN_TRANSACTIONAL" == "1" ]]; then
    echo "enabled"
  else
    echo "disabled"
  fi
}

snapshot_transaction_state() {
  if [[ "$DAILY_RUN_TRANSACTIONAL" != "1" ]]; then
    echo "[daily_run] Transactional rollback disabled (DAILY_RUN_TRANSACTIONAL=0)."
    return 0
  fi

  mkdir -p "${DAILY_RUN_TRANSACTION_SNAPSHOT_DIR}/snapshot"
  : > "$TRANSACTION_SNAPSHOT_MANIFEST"

  local target_path
  for target_path in "${DAILY_RUN_TRANSACTION_PATHS_LIST[@]}"; do
    [[ -z "$target_path" ]] && continue

    local snapshot_target="${DAILY_RUN_TRANSACTION_SNAPSHOT_DIR}/snapshot/${target_path}"
    if [[ -e "$target_path" ]]; then
      if [[ -d "$target_path" ]]; then
        printf 'd\t%s\n' "$target_path" >> "$TRANSACTION_SNAPSHOT_MANIFEST"
      else
        printf 'f\t%s\n' "$target_path" >> "$TRANSACTION_SNAPSHOT_MANIFEST"
      fi

      mkdir -p "$(dirname "$snapshot_target")"
      cp -a "$target_path" "$snapshot_target"
    else
      printf 'm\t%s\n' "$target_path" >> "$TRANSACTION_SNAPSHOT_MANIFEST"
    fi
  done

  TRANSACTION_SNAPSHOT_READY=1
  echo "[daily_run] Created transactional snapshot at ${DAILY_RUN_TRANSACTION_SNAPSHOT_DIR}."
}

restore_transaction_state() {
  if [[ "$DAILY_RUN_TRANSACTIONAL" != "1" || "$TRANSACTION_SNAPSHOT_READY" -ne 1 ]]; then
    return 0
  fi

  if [[ ! -f "$TRANSACTION_SNAPSHOT_MANIFEST" ]]; then
    echo "[daily_run] ERROR: transaction snapshot manifest missing (${TRANSACTION_SNAPSHOT_MANIFEST})."
    return 1
  fi

  local restore_rc=0
  local entry_type
  local target_path

  while IFS=$'\t' read -r entry_type target_path; do
    [[ -z "$target_path" ]] && continue

    rm -rf "$target_path"

    if [[ "$entry_type" == "d" || "$entry_type" == "f" ]]; then
      local snapshot_source="${DAILY_RUN_TRANSACTION_SNAPSHOT_DIR}/snapshot/${target_path}"
      if [[ -e "$snapshot_source" ]]; then
        mkdir -p "$(dirname "$target_path")"
        if ! mv "$snapshot_source" "$target_path" 2>/dev/null; then
          if ! cp -a "$snapshot_source" "$target_path"; then
            echo "[daily_run] ERROR: failed to restore transactional path: ${target_path}"
            restore_rc=1
          fi
        fi
      fi
    fi
  done < "$TRANSACTION_SNAPSHOT_MANIFEST"

  return "$restore_rc"
}

cleanup_transaction_snapshot() {
  if [[ "$DAILY_RUN_TRANSACTIONAL" != "1" || "$TRANSACTION_SNAPSHOT_READY" -ne 1 ]]; then
    return 0
  fi

  if [[ "$DAILY_RUN_KEEP_TRANSACTION_SNAPSHOT" == "1" ]]; then
    echo "[daily_run] Keeping transactional snapshot at ${DAILY_RUN_TRANSACTION_SNAPSHOT_DIR}."
    return 0
  fi

  rm -rf "$DAILY_RUN_TRANSACTION_SNAPSHOT_DIR"
}

clear_daily_run_failed_marker
update_daily_run_manifest "running" -1 -1 -1 "" false "$(transactional_mode_label)"

STATUS_CHECKPOINT_RAN=0

append_run_event() {
  local step="$1"
  local status="$2"
  local started_at="$3"
  local finished_at="$4"
  local duration="$5"
  local failure_reason="$6"
  local outputs_json="$7"

  local python_bin=""
  python_bin="$(resolve_python_bin)" || return 0

  mkdir -p "$(dirname "$RUN_EVENTS_PATH")"
  RUN_EVENT_STEP_ORDER=$((RUN_EVENT_STEP_ORDER + 1))

  RUN_EVENT_STEP="$step" \
  RUN_EVENT_STATUS="$status" \
  RUN_EVENT_STARTED_AT="$started_at" \
  RUN_EVENT_FINISHED_AT="$finished_at" \
  RUN_EVENT_DURATION="$duration" \
  RUN_EVENT_FAILURE_REASON="$failure_reason" \
  RUN_EVENT_OUTPUTS_JSON="$outputs_json" \
  RUN_EVENT_RUN_ID="$DAILY_RUN_ID" \
  RUN_EVENT_REVIEW_MODE="$REVIEW_MODE" \
  RUN_EVENT_STEP_ORDER="$RUN_EVENT_STEP_ORDER" \
  "$python_bin" - >> "$RUN_EVENTS_PATH" <<'PY'
import json
import os


def main() -> int:
    failure_reason = os.environ["RUN_EVENT_FAILURE_REASON"]
    outputs_json = os.environ["RUN_EVENT_OUTPUTS_JSON"]
    try:
        outputs = json.loads(outputs_json) if outputs_json else []
    except json.JSONDecodeError:
        outputs = []

    payload = {
        "run_id": os.environ["RUN_EVENT_RUN_ID"],
        "review_mode": os.environ["RUN_EVENT_REVIEW_MODE"],
        "step_order": int(os.environ["RUN_EVENT_STEP_ORDER"]),
        "step": os.environ["RUN_EVENT_STEP"],
        "started_at": os.environ["RUN_EVENT_STARTED_AT"],
        "finished_at": os.environ["RUN_EVENT_FINISHED_AT"],
        "duration": float(os.environ["RUN_EVENT_DURATION"]),
        "status": os.environ["RUN_EVENT_STATUS"],
        "failure_reason": failure_reason if failure_reason else None,
        "outputs_touched": outputs,
    }
    print(json.dumps(payload, ensure_ascii=False))
    return 0


raise SystemExit(main())
PY
}

run_logged_python_step() {
  local step="$1"
  local outputs_json="$2"
  local script_name="$3"
  shift 3

  local started_at
  started_at="$(utc_now)"
  local started_epoch
  started_epoch="$(date -u +%s)"

  local rc=0
  local status="success"
  local failure_reason=""

  if python "$script_name" "$@"; then
    rc=0
  else
    rc=$?
    status="failed"
    failure_reason="python ${script_name} exited with status ${rc}"
  fi

  local finished_at
  finished_at="$(utc_now)"
  local finished_epoch
  finished_epoch="$(date -u +%s)"
  local duration
  duration=$((finished_epoch - started_epoch))

  append_run_event "$step" "$status" "$started_at" "$finished_at" "$duration" "$failure_reason" "$outputs_json"
  return "$rc"
}

run_optional_python_step() {
  local enabled="$1"
  local run_message="$2"
  local script_name="$3"
  local skip_message="$4"
  local step_name="${5:-$script_name}"
  local outputs_json="${6:-[]}"
  shift 6

  if [[ "$enabled" == "1" ]]; then
    echo "[daily_run] $run_message"
    run_logged_python_step "$step_name" "$outputs_json" "$script_name" "$@"
  else
    echo "[daily_run] $skip_message"
  fi
}

update_progress_history() {
  echo "[daily_run] Updating progress history with run deltas..."
  if ! python progress_history_builder.py \
    --manifest "$DAILY_RUN_MANIFEST" \
    --status-summary outputs/status_summary.json \
    --history-output outputs/progress_history.csv \
    --summary-output outputs/progress_history_summary.md \
    --review-mode "$REVIEW_MODE"; then
    echo "[daily_run] WARNING: progress_history_builder.py failed; continuing with current exit code."
  fi
}

update_progress_history() {
  echo "[daily_run] Updating progress history with run deltas..."
  if ! python progress_history_builder.py \
    --manifest "$DAILY_RUN_MANIFEST" \
    --status-summary outputs/status_summary.json \
    --history-output outputs/progress_history.csv \
    --summary-output outputs/progress_history_summary.md \
    --review-mode "$REVIEW_MODE"; then
    echo "[daily_run] WARNING: progress_history_builder.py failed; continuing with current exit code."
  fi
}

update_progress_history() {
  echo "[daily_run] Updating progress history with run deltas..."
  if ! python progress_history_builder.py \
    --manifest "$DAILY_RUN_MANIFEST" \
    --status-summary outputs/status_summary.json \
    --history-output outputs/progress_history.csv \
    --summary-output outputs/progress_history_summary.md \
    --review-mode "$REVIEW_MODE"; then
    echo "[daily_run] WARNING: progress_history_builder.py failed; continuing with current exit code."
  fi
}

run_status_checkpoint() {
  if [[ "$STATUS_CHECKPOINT_RAN" -eq 1 ]]; then
    return 0
  fi
  STATUS_CHECKPOINT_RAN=1

  local checkpoint_rc=0

  echo "[daily_run] Running consolidated status report (mandatory final checkpoint)..."
  if ! run_logged_python_step "status_report" '["outputs/status_report.md","outputs/status_summary.json"]' "status_report.py"; then
    echo "[daily_run] ERROR: status_report.py failed during mandatory final checkpoint."
    checkpoint_rc=1
  fi

  echo "[daily_run] Building grouped TODO action plan (quick-fix hints)..."
  if ! run_logged_python_step "todo_action_plan" '["outputs/todo_action_plan.md"]' "todo_action_plan_builder.py" --input outputs/status_summary.json --output outputs/todo_action_plan.md; then
    echo "[daily_run] WARNING: todo_action_plan_builder.py failed during mandatory final checkpoint."
  fi

  echo "[daily_run] Running epistemic consistency guard (mandatory final checkpoint)..."
  if [[ "$REVIEW_MODE" == "production" ]]; then
    if ! run_logged_python_step "epistemic_consistency_guard" '["outputs/epistemic_consistency_report.md"]' "epistemic_consistency_guard.py" --review-mode production --fail-on-risk; then
      echo "[daily_run] ERROR: epistemic_consistency_guard.py failed during mandatory final checkpoint."
      checkpoint_rc=1
    fi
  else
    if ! run_logged_python_step "epistemic_consistency_guard" '["outputs/epistemic_consistency_report.md"]' "epistemic_consistency_guard.py" --review-mode template --no-fail-on-risk; then
      echo "[daily_run] ERROR: epistemic_consistency_guard.py failed during mandatory final checkpoint."
      checkpoint_rc=1
    fi
  fi

  if [[ "$RUN_WEEKLY_RISK_DIGEST" == "1" ]]; then
    echo "[daily_run] Generating weekly risk digest for PI update..."
    if ! run_logged_python_step "weekly_risk_digest" '["outputs/weekly_risk_digest.md"]' "weekly_risk_digest.py"; then
      echo "[daily_run] ERROR: weekly_risk_digest.py failed during mandatory final checkpoint."
      checkpoint_rc=1
    fi
  else
    echo "[daily_run] Skipping weekly risk digest (set RUN_WEEKLY_RISK_DIGEST=1 to enable)."
  fi

  echo "[daily_run] Rendering status CLI snapshot (mandatory final checkpoint)..."
  local snapshot_dir
  snapshot_dir="$(dirname "$STATUS_CLI_SNAPSHOT")"
  local snapshot_basename
  snapshot_basename="$(basename "$STATUS_CLI_SNAPSHOT")"
  mkdir -p "$snapshot_dir"
  local snapshot_tmp
  snapshot_tmp="$(mktemp "${snapshot_dir}/.${snapshot_basename}.tmp.XXXXXX")"
  local status_cli_started_at
  status_cli_started_at="$(utc_now)"
  local status_cli_started_epoch
  status_cli_started_epoch="$(date -u +%s)"
  if python status_cli.py --priority-policy "$STATUS_PRIORITY_POLICY" | tee "$snapshot_tmp"; then
    mv "$snapshot_tmp" "$STATUS_CLI_SNAPSHOT"
    append_run_event "status_cli_snapshot" "success" "$status_cli_started_at" "$(utc_now)" "$(( $(date -u +%s) - status_cli_started_epoch ))" "" "[\"${STATUS_CLI_SNAPSHOT}\"]"
  else
    rm -f "$snapshot_tmp"
    append_run_event "status_cli_snapshot" "failed" "$status_cli_started_at" "$(utc_now)" "$(( $(date -u +%s) - status_cli_started_epoch ))" "status_cli.py snapshot render failed" "[\"${STATUS_CLI_SNAPSHOT}\"]"
    echo "[daily_run] ERROR: status_cli.py failed during mandatory final checkpoint."
    checkpoint_rc=1
  fi

  if [[ "$REVIEW_MODE" == "production" ]]; then
    echo "[daily_run] Enforcing production status gate (fail-on=$STATUS_FAIL_ON)..."
    if ! run_logged_python_step "status_gate" '[]' "status_cli.py" --fail-on "$STATUS_FAIL_ON" --todo-only --priority-policy "$STATUS_PRIORITY_POLICY"; then
      echo "[daily_run] ERROR: production status gate failed (status_cli --fail-on $STATUS_FAIL_ON --priority-policy $STATUS_PRIORITY_POLICY)."
      checkpoint_rc=1
    fi
  else
    echo "[daily_run] REVIEW_MODE=template: status gate is informational only."
  fi

  return "$checkpoint_rc"
}

on_exit() {
  local pipeline_rc="$1"
  local final_rc="$pipeline_rc"
  local status_rc=0
  local rollback_applied=0
  local rollback_applied_json="false"

  set +e
  run_status_checkpoint
  status_rc=$?

  if [[ "$final_rc" -eq 0 && "$status_rc" -ne 0 ]]; then
    final_rc="$status_rc"
  fi

  local failure_phase=""
  if [[ "$pipeline_rc" -ne 0 && "$status_rc" -ne 0 ]]; then
    failure_phase="pipeline_and_checkpoint"
  elif [[ "$pipeline_rc" -ne 0 ]]; then
    failure_phase="pipeline"
  elif [[ "$status_rc" -ne 0 ]]; then
    failure_phase="checkpoint"
  fi

  if [[ "$final_rc" -ne 0 ]]; then
    if [[ "$DAILY_RUN_TRANSACTIONAL" == "1" ]]; then
      echo "[daily_run] Restoring transactional snapshot after failure..."
      if restore_transaction_state; then
        rollback_applied=1
        rollback_applied_json="true"
        echo "[daily_run] Transactional rollback completed."
      else
        echo "[daily_run] ERROR: transactional rollback failed; outputs may be partially updated."
      fi
    fi

    write_daily_run_failed_marker "$pipeline_rc" "$status_rc" "$final_rc" "$failure_phase"
    update_daily_run_manifest "failed" "$pipeline_rc" "$status_rc" "$final_rc" "$failure_phase" "$rollback_applied_json" "$(transactional_mode_label)"
    echo "[daily_run] Pipeline finished with failures (exit=$final_rc)."
    echo "[daily_run] Refreshing status report with failed-run marker..."
    if ! python status_report.py; then
      echo "[daily_run] ERROR: status_report.py refresh failed after failure marker write."
    fi
  else
    clear_daily_run_failed_marker
    update_daily_run_manifest "success" "$pipeline_rc" "$status_rc" "$final_rc" "" false "$(transactional_mode_label)"

    local audit_description
    audit_description="daily_run success (run_id=${DAILY_RUN_ID}; review_mode=${REVIEW_MODE}; status_checkpoint_exit_code=${status_rc})"
    if append_audit_log_entry "$(utc_now)" "run_success" "03_analysis/daily_run.sh" "$audit_description"; then
      echo "[daily_run] Appended run-success audit entry to ${AUDIT_LOG_PATH}."
    else
      echo "[daily_run] WARNING: failed to append run-success audit entry (${AUDIT_LOG_PATH})."
    fi
  fi

  update_progress_history

  cleanup_transaction_snapshot

  trap - EXIT
  exit "$final_rc"
}

trap 'on_exit "$?"' EXIT

snapshot_transaction_state

echo "[daily_run] Consolidating title/abstract dual-log consensus..."
run_logged_python_step "title_abstract_consensus" '[]' "consolidate_title_abstract_consensus.py"

echo "[daily_run] Running CSV input validation..."
run_logged_python_step "validate_csv_inputs" '["outputs/csv_input_validation_summary.md"]' "validate_csv_inputs.py"

echo "[daily_run] Running audit log integrity guard..."
ensure_audit_log_header
run_logged_python_step "audit_log_integrity_guard" '[]' "audit_log_integrity_guard.py" --path "$AUDIT_LOG_PATH"

echo "[daily_run] Running record-id map integrity guard..."
run_logged_python_step "record_id_map_integrity_guard" '[]' "record_id_map_integrity_guard.py"

echo "[daily_run] Running screening metrics..."
run_logged_python_step "screening_metrics" '[]' "screening_metrics.py"

run_optional_python_step \
  "$RUN_REVIEWER_WORKLOAD_BALANCER" \
  "Running reviewer workload balancer (non-blocking by default)..." \
  "reviewer_workload_balancer.py" \
  "Skipping reviewer workload balancer (set RUN_REVIEWER_WORKLOAD_BALANCER=1 to enable)." \
  "reviewer_workload_balancer" \
  '["outputs/reviewer_workload_balancer_summary.md","outputs/reviewer_workload_plan.csv"]'

echo "[daily_run] Running screening disagreement analyzer..."
run_logged_python_step "screening_disagreement_analyzer" '[]' "screening_disagreement_analyzer.py"

run_optional_python_step \
  "$RUN_MULTILANG_ABSTRACT_SCREENER" \
  "Running multilingual abstract screener (config-driven keyword rules)..." \
  "multilang_abstract_screener.py" \
  "Skipping multilingual abstract screener (set RUN_MULTILANG_ABSTRACT_SCREENER=1 to enable)." \
  "multilang_abstract_screener" \
  '[]'

echo "[daily_run] Running extraction validation..."
run_logged_python_step "validate_extraction" '["outputs/extraction_validation_summary.md"]' "validate_extraction.py"

echo "[daily_run] Running quality appraisal scoring (JBI)..."
run_logged_python_step "quality_appraisal" '[]' "quality_appraisal.py"

echo "[daily_run] Running GRADE evidence profiler..."
run_logged_python_step "grade_evidence_profiler" '["../04_manuscript/tables/grade_evidence_profile_table.tex"]' "grade_evidence_profiler.py"

echo "[daily_run] Converting effect sizes for meta-analysis harmonization..."
run_logged_python_step "effect_size_converter" '[]' "effect_size_converter.py"

echo "[daily_run] Building optional meta-analysis aggregate table..."
run_logged_python_step "meta_analysis_results_builder" '[]' "meta_analysis_results_builder.py" --fail-on none

echo "[daily_run] Building final results summary table..."
run_logged_python_step "results_summary_table_builder" '["../04_manuscript/tables/results_summary_table.tex"]' "results_summary_table_builder.py"

echo "[daily_run] Generating forest plot from converted effect sizes..."
run_logged_python_step "forest_plot_generator" '[]' "forest_plot_generator.py"

run_optional_python_step \
  "$RUN_PUBLICATION_BIAS" \
  "Running publication bias assessment (funnel + Egger)..." \
  "publication_bias_assessment.py" \
  "Skipping publication-bias assessment (set RUN_PUBLICATION_BIAS=1 to enable)." \
  "publication_bias_assessment" \
  '[]'

echo "[daily_run] Building manuscript-ready results interpretation narrative layer..."
run_logged_python_step "results_interpretation_layer" '["../04_manuscript/sections/03c_interpretation_auto.tex"]' "results_interpretation_layer.py"

echo "[daily_run] Building analysis lineage (per-outcome study IDs across synthesis artifacts)..."
run_logged_python_step "analysis_lineage" '[]' "analysis_lineage.py"

echo "[daily_run] Building study-level flow map (search -> screening -> inclusion -> analysis)..."
run_logged_python_step "study_flow_map_builder" '[]' "study_flow_map_builder.py"

run_optional_python_step \
  "$RUN_POLYGLOT_TRANSLATION" \
  "Running PubMed→Scopus/WoS/PsycINFO query translation..." \
  "polyglot_search.py" \
  "Skipping polyglot translation (set RUN_POLYGLOT_TRANSLATION=1 to enable)." \
  "polyglot_search" \
  '[]'

run_optional_python_step \
  "$RUN_KEYWORD_ANALYSIS" \
  "Running keyword co-occurrence analysis (litsearchr-style)..." \
  "keyword_network.py" \
  "Skipping keyword analysis (set RUN_KEYWORD_ANALYSIS=1 to enable)." \
  "keyword_network" \
  '[]'

echo "[daily_run] Generating synthesis characteristics table (LaTeX)..."
run_logged_python_step "synthesis_tables" '["../04_manuscript/tables/study_characteristics_table.tex"]' "synthesis_tables.py"

echo "[daily_run] Running dedup merge (only if new exports)..."
run_logged_python_step "dedup_merge" '["outputs/dedup_merge_summary.md","../02_data/processed/record_id_map.csv"]' "dedup_merge.py" --if-new-exports

echo "[daily_run] Running dedup stats + PRISMA update (apply + explicit backup)..."
run_logged_python_step "dedup_stats" '["outputs/dedup_stats_summary.md","outputs/prisma_flow_diagram.svg","outputs/prisma_flow_diagram.tex","../02_data/processed/prisma_counts_template.csv"]' "dedup_stats.py" --flow-backend both --flow-style journal --flow-output outputs/prisma_flow_diagram.svg --apply --backup

run_optional_python_step \
  "$RUN_CITATION_TRACKING" \
  "Running backward/forward citation chasing (OpenCitations)..." \
  "citation_tracker.py" \
  "Skipping citation chasing (set RUN_CITATION_TRACKING=1 to enable)." \
  "citation_tracker" \
  '[]'

echo "[daily_run] Generating PRISMA manuscript tables (LaTeX)..."
run_logged_python_step "prisma_tables" '["../04_manuscript/tables/prisma_counts_table.tex","../04_manuscript/tables/fulltext_exclusion_table.tex"]' "prisma_tables.py"

run_optional_python_step \
  "$RUN_PROSPERO_DRAFTER" \
  "Drafting pre-filled PROSPERO registration package..." \
  "prospero_submission_drafter.py" \
  "Skipping PROSPERO drafter (set RUN_PROSPERO_DRAFTER=1 to enable)." \
  "prospero_submission_drafter" \
  '["outputs/prospero_registration_prefill.md","outputs/prospero_registration_prefill.xml","outputs/prospero_submission_drafter_summary.md"]'

run_optional_python_step \
  "$RUN_RETRACTION_CHECKER" \
  "Running included-study retraction checker (single Retraction Watch DOI fetch)..." \
  "retraction_checker.py" \
  "Skipping retraction checker (set RUN_RETRACTION_CHECKER=1 to enable)." \
  "retraction_checker" \
  '[]'

if [[ "$RUN_LIVING_REVIEW_SCHEDULER" == "1" ]]; then
  SCHEDULER_ARGS=(--review-mode auto)
  if [[ "$RUN_LIVING_REVIEW_CADENCE_CHECK" == "1" ]]; then
    SCHEDULER_ARGS+=(--check-cadence)
    echo "[daily_run] Running living-review scheduler (auto mode + cadence check + session diffs)..."
  else
    echo "[daily_run] Running living-review scheduler (auto mode + session diffs)..."
  fi
  run_logged_python_step "living_review_scheduler" '[]' "living_review_scheduler.py" "${SCHEDULER_ARGS[@]}"
else
  echo "[daily_run] Skipping living-review scheduler (set RUN_LIVING_REVIEW_SCHEDULER=1 to enable)."
fi

echo "[daily_run] Running PRISMA adherence checker..."
run_logged_python_step "prisma_adherence_checker" '["outputs/prisma_adherence_report.md"]' "prisma_adherence_checker.py"

if [[ "$REVIEW_MODE" == "production" ]]; then
  echo "[daily_run] REVIEW_MODE=production: enforcing strict template leakage guard for ../04_manuscript/ ..."
  run_logged_python_step "template_term_guard" '["outputs/template_term_guard_summary.md"]' "template_term_guard.py" \
    --scan-path ../04_manuscript \
    --check-placeholders \
    --no-check-banned-terms \
    --placeholder-pattern '\[(?:[A-Z][A-Z0-9_\\ ]{3,})\]' \
    --fail-on-match \
    --summary-output outputs/template_term_guard_summary.md
else
  case "$RUN_TEMPLATE_TERM_GUARD" in
    fail)
      echo "[daily_run] Running template term guard in fail mode..."
      run_logged_python_step "template_term_guard" '["outputs/template_term_guard_summary.md"]' "template_term_guard.py" --fail-on-match || true
      ;;
    warn)
      echo "[daily_run] Running template term guard in warn mode..."
      run_logged_python_step "template_term_guard" '["outputs/template_term_guard_summary.md"]' "template_term_guard.py" --no-fail-on-match
      ;;
    skip)
      echo "[daily_run] Skipping template term guard (RUN_TEMPLATE_TERM_GUARD=skip)."
      ;;
  esac
fi

run_optional_python_step \
  "$RUN_TRANSPARENCY_APPENDIX_SYNC" \
  "Syncing decision trace into transparency appendix..." \
  "transparency_appendix_decision_trace.py" \
  "Skipping transparency appendix sync (set RUN_TRANSPARENCY_APPENDIX_SYNC=1 to enable)." \
  "transparency_appendix_decision_trace" \
  '["../04_manuscript/tables/decision_trace_table.tex","../04_manuscript/tables/analysis_trace_table.tex","../appendix_transparency.md"]' \
  --max-rows "$DECISION_TRACE_MARKDOWN_MAX_ROWS" \
  --latex-max-rows "$DECISION_TRACE_LATEX_MAX_ROWS" \
  --analysis-max-rows "$DECISION_TRACE_MARKDOWN_MAX_ROWS" \
  --analysis-latex-max-rows "$DECISION_TRACE_LATEX_MAX_ROWS"

echo "[daily_run] Running mandatory final status checkpoint..."
run_status_checkpoint

print_output_item() {
  echo "  - $1"
}

print_output_group() {
  local item
  for item in "$@"; do
    print_output_item "$item"
  done
}

echo "[daily_run] Done. Updated files:"
print_output_group \
  "outputs/csv_input_validation_summary.md" \
  "outputs/screening_metrics_summary.md" \
  "outputs/screening_statistics.csv"
if [[ "$RUN_REVIEWER_WORKLOAD_BALANCER" == "1" ]]; then
  print_output_group \
    "outputs/reviewer_workload_plan.csv" \
    "outputs/reviewer_workload_balancer_summary.md"
fi
print_output_group \
  "outputs/screening_disagreement_report.md" \
  "outputs/screening_disagreement_patterns.csv" \
  "outputs/screening_disagreement_records.csv"
if [[ "$RUN_PREFLIGHT_PLACEHOLDER_GUARD" != "skip" ]]; then
  print_output_item "outputs/preflight_placeholder_guard_summary.md"
fi
if [[ "$RUN_MULTILANG_ABSTRACT_SCREENER" == "1" ]]; then
  print_output_group \
    "outputs/multilang_abstract_screening_recommendations.csv" \
    "outputs/multilang_abstract_screening_summary.md"
fi
print_output_group \
  "outputs/extraction_validation_summary.md" \
  "outputs/quality_appraisal_summary.md" \
  "outputs/quality_appraisal_scored.csv" \
  "outputs/quality_appraisal_aggregate.csv" \
  "outputs/grade_evidence_profile.csv" \
  "outputs/grade_evidence_profile_summary.md" \
  "../04_manuscript/tables/grade_evidence_profile_table.tex" \
  "outputs/effect_size_converted.csv" \
  "outputs/effect_size_conversion_summary.md" \
  "outputs/meta_analysis_results.csv" \
  "outputs/meta_analysis_results_summary.md" \
  "outputs/results_summary_table.csv" \
  "outputs/results_summary_table_summary.md" \
  "../04_manuscript/tables/results_summary_table.tex" \
  "outputs/results_interpretation_layer.md" \
  "../04_manuscript/sections/03c_interpretation_auto.tex" \
  "outputs/forest_plot_data.csv" \
  "outputs/forest_plot.png" \
  "outputs/forest_plot.tikz" \
  "outputs/forest_plot_summary.md" \
  "outputs/analysis_lineage.json" \
  "outputs/analysis_trace.json" \
  "outputs/study_flow_map.csv" \
  "outputs/study_flow_map_summary.md"
if [[ "$RUN_PUBLICATION_BIAS" == "1" ]]; then
  print_output_group \
    "outputs/publication_bias_data.csv" \
    "outputs/publication_bias_funnel.png" \
    "outputs/publication_bias_funnel.tikz" \
    "outputs/publication_bias_summary.md" \
    "../04_manuscript/tables/publication_bias_assessment_table.tex"
fi
if [[ "$RUN_POLYGLOT_TRANSLATION" == "1" ]]; then
  print_output_group \
    "outputs/polyglot_search_summary.md" \
    "outputs/polyglot_queries/polyglot_scopus.txt" \
    "outputs/polyglot_queries/polyglot_wos.txt" \
    "outputs/polyglot_queries/polyglot_psycinfo.txt"
fi
print_output_item "outputs/synthesis_tables_summary.md"
if [[ "$RUN_KEYWORD_ANALYSIS" == "1" ]]; then
  print_output_group \
    "outputs/keyword_analysis_summary.md" \
    "outputs/keyword_candidates.csv" \
    "outputs/keyword_network_nodes.csv" \
    "outputs/keyword_network_edges.csv" \
    "outputs/keyword_block_bc_suggestions.md"
fi
print_output_group \
  "outputs/dedup_merge_summary.md" \
  "outputs/new_record_triage.csv" \
  "outputs/dedup_stats_summary.md" \
  "../02_data/processed/record_id_map.csv"
if [[ "$RUN_CITATION_TRACKING" == "1" ]]; then
  print_output_group \
    "outputs/citation_forward.csv" \
    "outputs/citation_backward.csv" \
    "outputs/citation_grey_search_log.csv" \
    "outputs/citation_tracker_summary.md"
fi
if [[ "$RUN_PROSPERO_DRAFTER" == "1" ]]; then
  print_output_group \
    "outputs/prospero_registration_prefill.md" \
    "outputs/prospero_registration_prefill.xml" \
    "outputs/prospero_submission_drafter_summary.md"
fi
if [[ "$RUN_RETRACTION_CHECKER" == "1" ]]; then
  print_output_group \
    "outputs/retraction_check_results.csv" \
    "outputs/retraction_check_summary.md"
fi
if [[ "$RUN_LIVING_REVIEW_SCHEDULER" == "1" ]]; then
  print_output_group \
    "outputs/living_review_schedule.csv" \
    "outputs/living_review_search_diffs.csv" \
    "outputs/living_review_scheduler_summary.md"
fi
print_output_group \
  "outputs/prisma_tables_summary.md" \
  "outputs/prisma_flow_diagram.svg" \
  "outputs/prisma_flow_diagram.tex" \
  "outputs/progress_history.csv" \
  "outputs/progress_history_summary.md" \
  "outputs/todo_action_plan.md" \
  "outputs/status_report.md" \
  "outputs/status_summary.json" \
  "$STATUS_CLI_SNAPSHOT" \
  "$DAILY_RUN_MANIFEST" \
  "$AUDIT_LOG_PATH" \
  "outputs/epistemic_consistency_report.md"
if [[ -f "$DAILY_RUN_FAILED_MARKER" ]]; then
  print_output_item "$DAILY_RUN_FAILED_MARKER"
fi
if [[ "$RUN_WEEKLY_RISK_DIGEST" == "1" ]]; then
  print_output_item "outputs/weekly_risk_digest.md"
fi
if [[ "$REVIEW_MODE" == "production" ]]; then
  print_output_item "production status gate (\`status_cli.py --fail-on $STATUS_FAIL_ON --todo-only --priority-policy $STATUS_PRIORITY_POLICY\`)"
fi
print_output_item "outputs/prisma_adherence_report.md"
if [[ "$RUN_TEMPLATE_TERM_GUARD" != "skip" || "$REVIEW_MODE" == "production" ]]; then
  print_output_item "outputs/template_term_guard_summary.md"
fi
if [[ "$RUN_TRANSPARENCY_APPENDIX_SYNC" == "1" ]]; then
  print_output_group \
    "outputs/transparency_appendix_decision_trace_summary.md" \
    "../04_manuscript/tables/decision_trace_table.tex" \
    "../04_manuscript/tables/analysis_trace_table.tex" \
    "../appendix_transparency.md"
fi
print_output_group \
  "../04_manuscript/tables/prisma_counts_table.tex" \
  "../04_manuscript/tables/fulltext_exclusion_table.tex" \
  "../04_manuscript/tables/study_characteristics_table.tex" \
  "../02_data/processed/prisma_counts_template.csv"
echo "[daily_run] Tip: for manual re-merge from papers/2026-next-review/, use: make merge or make merge-force"
