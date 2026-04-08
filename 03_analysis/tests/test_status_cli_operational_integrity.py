import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "status_cli.py"
spec = importlib.util.spec_from_file_location("status_cli", MODULE_PATH)
status_cli = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = status_cli
assert spec.loader is not None
spec.loader.exec_module(status_cli)


class StatusCliOperationalIntegrityTests(unittest.TestCase):
    def test_fail_on_major_flags_open_major_items(self) -> None:
        summary = {
            "health_checks": [{"level": "warning", "message": "Search totals are empty."}],
            "input_checklist": [
                {
                    "id": "search_totals",
                    "title": "Fill search totals",
                    "done": False,
                    "details": "Current sum(results_total): 0",
                }
            ],
        }
        priority_policy = {
            "checklist_priority": {"search_totals": "major"},
            "fail_thresholds": {"default": "major"},
        }

        blockers = status_cli.find_blockers(summary, "major", priority_policy)

        self.assertGreaterEqual(len(blockers), 1)
        self.assertTrue(any(blocker["severity"] == "major" for blocker in blockers))

    def test_fail_on_critical_ignores_major_findings(self) -> None:
        summary = {
            "health_checks": [{"level": "warning", "message": "Search totals are empty."}],
            "input_checklist": [
                {
                    "id": "search_totals",
                    "title": "Fill search totals",
                    "done": False,
                }
            ],
        }
        priority_policy = {
            "checklist_priority": {"search_totals": "major"},
            "fail_thresholds": {"default": "major"},
        }

        blockers = status_cli.find_blockers(summary, "critical", priority_policy)

        self.assertEqual(blockers, [])

    def test_legacy_policy_schema_is_normalized(self) -> None:
        summary = {
            "health_checks": [],
            "input_checklist": [
                {
                    "id": "prisma_sync",
                    "title": "Keep PRISMA core counts in sync",
                    "done": False,
                }
            ],
        }
        legacy_policy = {
            "checklist": {"prisma_sync": "critical", "default": "major"},
            "warnings": {"default": "major"},
        }

        blockers = status_cli.find_blockers(summary, "critical", legacy_policy)

        self.assertEqual(len(blockers), 1)
        self.assertEqual(blockers[0]["severity"], "critical")
        self.assertEqual(blockers[0]["source"], "checklist")

    def test_health_severity_mapping_is_policy_driven(self) -> None:
        summary = {
            "health_checks": [{"level": "warning", "message": "Bootstrap warning"}],
            "input_checklist": [],
        }
        custom_policy = {
            "health_level_severity": {"warning": "minor", "error": "critical"},
            "fail_thresholds": {"default": "major"},
            "checklist_priority": {"default": "major"},
        }

        blockers_major = status_cli.find_blockers(summary, "major", custom_policy)
        blockers_minor = status_cli.find_blockers(summary, "minor", custom_policy)

        self.assertEqual(blockers_major, [])
        self.assertEqual(len(blockers_minor), 1)
        self.assertEqual(blockers_minor[0]["severity"], "minor")

    def test_semantic_placeholder_checklist_item_blocks_in_major_policy(self) -> None:
        summary = {
            "health_checks": [],
            "input_checklist": [
                {
                    "id": "semantic_placeholders",
                    "title": "Resolve placeholders before production",
                    "done": False,
                    "details": "REVIEW_MODE=production; unresolved placeholders: 2",
                }
            ],
        }
        priority_policy = {
            "checklist_priority": {"semantic_placeholders": "major", "default": "major"},
            "fail_thresholds": {"default": "major"},
        }

        blockers = status_cli.find_blockers(summary, "major", priority_policy)

        self.assertEqual(len(blockers), 1)
        self.assertEqual(blockers[0]["source"], "checklist")
        self.assertEqual(blockers[0]["id"], "semantic_placeholders")
        self.assertEqual(blockers[0]["severity"], "major")

    def test_repo_priority_policy_matches_canonical_severity_taxonomy(self) -> None:
        policy_path = Path(__file__).resolve().parents[1] / "priority_policy.json"
        priority_policy = status_cli.load_priority_policy(policy_path)

        self.assertEqual(priority_policy["fail_thresholds"]["default"], "major")
        self.assertEqual(priority_policy["health_level_severity"]["warning"], "major")
        self.assertEqual(priority_policy["health_level_severity"]["error"], "critical")
        self.assertEqual(priority_policy["checklist_priority"]["prisma_sync"], "critical")
        self.assertEqual(priority_policy["checklist_priority"]["semantic_placeholders"], "critical")
        self.assertEqual(priority_policy["checklist_priority"]["default"], "major")

    def test_generate_summary_if_missing_runs_status_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            summary_path = tmp_path / "outputs" / "status_summary.json"
            status_report_script = tmp_path / "status_report.py"

            status_report_script.write_text(
                "from pathlib import Path\n"
                "import json\n"
                "base = Path(__file__).resolve().parent\n"
                "out = base / 'outputs' / 'status_summary.json'\n"
                "out.parent.mkdir(parents=True, exist_ok=True)\n"
                "out.write_text(json.dumps({'generated_at': '2026-03-14 00:00'}), encoding='utf-8')\n",
                encoding="utf-8",
            )

            status_cli.generate_summary_if_missing(
                summary_path,
                auto_generate_missing=True,
                status_report_script=status_report_script,
            )

            self.assertTrue(summary_path.exists())
            parsed = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(parsed["generated_at"], "2026-03-14 00:00")


if __name__ == "__main__":
    unittest.main()
