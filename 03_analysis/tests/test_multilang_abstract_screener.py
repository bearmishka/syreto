import copy
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

MODULE_PATH = Path(__file__).resolve().parents[1] / "multilang_abstract_screener.py"
spec = importlib.util.spec_from_file_location("multilang_abstract_screener", MODULE_PATH)
multilang_abstract_screener = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = multilang_abstract_screener
assert spec.loader is not None
spec.loader.exec_module(multilang_abstract_screener)


class MultiLangAbstractScreenerTests(unittest.TestCase):
    def test_detect_language_heuristics(self) -> None:
        ru = multilang_abstract_screener.detect_language(
            "Это исследование пациентов с клиническим диагнозом."
        )
        es = multilang_abstract_screener.detect_language(
            "Este estudio en pacientes evaluó resultados clínicos."
        )
        de = multilang_abstract_screener.detect_language(
            "Diese Studie mit Patienten analysierte Korrelationen."
        )
        fr = multilang_abstract_screener.detect_language(
            "Cette étude avec des patients a évalué les résultats cliniques."
        )
        it = multilang_abstract_screener.detect_language(
            "Questo studio con pazienti ha valutato gli esiti clinici."
        )
        pt = multilang_abstract_screener.detect_language(
            "Este estudo com pacientes avaliou desfechos clínicos."
        )
        pl = multilang_abstract_screener.detect_language(
            "To badanie pacjentów oceniło wyniki kliniczne."
        )
        tr = multilang_abstract_screener.detect_language(
            "Bu çalışma hastalarda klinik sonuçları değerlendirdi."
        )
        nl = multilang_abstract_screener.detect_language(
            "Deze studie met patiënten beoordeelde klinische resultaten."
        )
        en = multilang_abstract_screener.detect_language(
            "This study analyzed patients in a clinical cohort."
        )

        self.assertEqual(ru, "ru")
        self.assertEqual(es, "es")
        self.assertEqual(de, "de")
        self.assertEqual(fr, "fr")
        self.assertEqual(it, "it")
        self.assertEqual(pt, "pt")
        self.assertEqual(pl, "pl")
        self.assertEqual(tr, "tr")
        self.assertEqual(nl, "nl")
        self.assertEqual(en, "en")

    def test_normalize_language_aliases_for_extended_languages(self) -> None:
        self.assertEqual(multilang_abstract_screener.normalize_language("French"), "fr")
        self.assertEqual(multilang_abstract_screener.normalize_language("italiano"), "it")
        self.assertEqual(multilang_abstract_screener.normalize_language("pt-BR"), "pt")
        self.assertEqual(multilang_abstract_screener.normalize_language("Polski"), "pl")
        self.assertEqual(multilang_abstract_screener.normalize_language("Türkçe"), "tr")
        self.assertEqual(multilang_abstract_screener.normalize_language("Nederlands"), "nl")

    def test_screen_record_include_when_core_keywords_match(self) -> None:
        rules = copy.deepcopy(multilang_abstract_screener.DEFAULT_KEYWORD_RULES)
        rules["core_signals"]["population"]["ru"] = ["пациент"]
        rules["core_signals"]["concept_exposure"]["ru"] = ["межличност"]
        rules["core_signals"]["outcome"]["ru"] = ["идентич"]

        row = pd.Series(
            {
                "record_id": "R001",
                "source_database": "scopus",
                "year": "2025",
                "title": "Межличностные особенности у пациентов",
                "abstract": "Исследование пациентов показало связь межличностных факторов и идентичности, n=32.",
            }
        )

        result = multilang_abstract_screener.screen_record(
            row,
            keyword_rules=rules,
            eligible_languages={"ru", "es", "de"},
            conservative_missing_core=True,
            include_english=False,
            screening_reasons={
                multilang_abstract_screener.REASON_NO_POPULATION,
                multilang_abstract_screener.REASON_NO_CONCEPT,
                multilang_abstract_screener.REASON_NO_OUTCOME,
                multilang_abstract_screener.REASON_NON_EMPIRICAL,
                multilang_abstract_screener.REASON_CASE_REPORT,
                multilang_abstract_screener.REASON_ANIMAL_ONLY,
                multilang_abstract_screener.REASON_LANGUAGE,
                multilang_abstract_screener.REASON_OTHER,
            },
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["recommended_decision"], "include")
        self.assertEqual(result["detected_language"], "ru")

    def test_screen_record_excludes_animal_only(self) -> None:
        row = pd.Series(
            {
                "record_id": "R002",
                "source_database": "wos",
                "year": "2024",
                "title": "Tiermodell der Erkrankung",
                "abstract": "Diese Studie im Maus-Tiermodell untersuchte molekulare Marker.",
            }
        )

        result = multilang_abstract_screener.screen_record(
            row,
            keyword_rules=copy.deepcopy(multilang_abstract_screener.DEFAULT_KEYWORD_RULES),
            eligible_languages={"ru", "es", "de"},
            conservative_missing_core=True,
            include_english=False,
            screening_reasons={multilang_abstract_screener.REASON_ANIMAL_ONLY},
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["recommended_decision"], "exclude")
        self.assertEqual(
            result["recommended_reason"], multilang_abstract_screener.REASON_ANIMAL_ONLY
        )

    def test_screen_record_conservative_maybe_when_core_group_missing(self) -> None:
        rules = copy.deepcopy(multilang_abstract_screener.DEFAULT_KEYWORD_RULES)
        rules["core_signals"]["population"]["es"] = ["pacientes"]
        rules["core_signals"]["concept_exposure"]["es"] = ["apego"]
        rules["core_signals"]["outcome"]["es"] = ["identidad"]

        row = pd.Series(
            {
                "record_id": "R003",
                "source_database": "pubmed",
                "year": "2023",
                "title": "Apego en pacientes clínicos",
                "abstract": "Este estudio en pacientes evaluó factores de apego y ansiedad, n=40.",
            }
        )

        result = multilang_abstract_screener.screen_record(
            row,
            keyword_rules=rules,
            eligible_languages={"ru", "es", "de"},
            conservative_missing_core=True,
            include_english=False,
            screening_reasons={multilang_abstract_screener.REASON_NO_OUTCOME},
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["recommended_decision"], "maybe")
        self.assertEqual(
            result["recommended_reason"], multilang_abstract_screener.REASON_NO_OUTCOME
        )

    def test_load_keyword_rules_ignores_placeholder_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            rules_path = Path(tmp_dir) / "rules.json"
            rules_path.write_text(
                """
{
  "core_signals": {
    "population": {
      "ru": ["[POPULATION_RU_TERM_1]", "пациент"]
    }
  }
}
""".strip(),
                encoding="utf-8",
            )

            rules, metadata = multilang_abstract_screener.load_keyword_rules(rules_path)

        self.assertEqual(rules["core_signals"]["population"]["ru"], ["пациент"])
        self.assertEqual(int(metadata["ignored_placeholders"]), 1)

    def test_load_keyword_rules_deduplicates_eligible_languages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            rules_path = Path(tmp_dir) / "rules.json"
            rules_path.write_text(
                """
{
  "eligible_languages": ["ru", "fr", "fr", "pl", "Polski", "tr", "turkish", "nl", "nld"]
}
""".strip(),
                encoding="utf-8",
            )

            rules, _ = multilang_abstract_screener.load_keyword_rules(rules_path)

        self.assertEqual(rules["eligible_languages"], ["ru", "fr", "pl", "tr", "nl"])

    def test_main_generates_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            records_path = tmp_path / "master_records.csv"
            records_path.write_text(
                "record_id,source_database,title,abstract,year\n"
                "R001,scopus,Исследование пациентов,Это исследование пациентов оценило идентичность и межличностные отношения n=25,2024\n"
                "R002,pubmed,Animal model in mice,Diese Studie im Maus-Tiermodell zeigte Marker,2023\n"
                "R003,wos,English title,This study in adult patients examined outcomes,2022\n",
                encoding="utf-8",
            )

            keyword_rules_path = tmp_path / "rules.json"
            keyword_rules_path.write_text(
                """
{
  "eligible_languages": ["ru", "de", "es"],
  "core_signals": {
    "population": {"ru": ["пациент"]},
    "concept_exposure": {"ru": ["межличност"]},
    "outcome": {"ru": ["идентич"]}
  }
}
""".strip(),
                encoding="utf-8",
            )

            screening_rules_path = tmp_path / "screening_rules.md"
            screening_rules_path.write_text(
                """
### Recommended title/abstract exclusion reasons (`title_abstract_reason`)

- `No eligible population/context`
- `No eligible exposure/concept`
- `No eligible outcome`
- `Non-empirical/theoretical only`
- `Case report likely n<5`
- `Animal-only or non-human`
- `Language not eligible`
- `Other`
""".strip(),
                encoding="utf-8",
            )

            recommendations_output = tmp_path / "recommendations.csv"
            summary_output = tmp_path / "summary.md"

            exit_code = multilang_abstract_screener.main(
                [
                    "--records",
                    str(records_path),
                    "--screening-rules",
                    str(screening_rules_path),
                    "--keyword-rules",
                    str(keyword_rules_path),
                    "--recommendations-output",
                    str(recommendations_output),
                    "--summary-output",
                    str(summary_output),
                ]
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue(recommendations_output.exists())
            self.assertTrue(summary_output.exists())

            df = pd.read_csv(recommendations_output, dtype=str)
            self.assertEqual(int(df.shape[0]), 2)
            self.assertIn("recommended_decision", df.columns)
            self.assertIn("detected_language", df.columns)


if __name__ == "__main__":
    unittest.main()
