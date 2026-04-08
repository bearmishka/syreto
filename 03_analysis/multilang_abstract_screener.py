from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

PLACEHOLDER_PATTERN = re.compile(r"\[[^\]]+\]")
WORD_PATTERN = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿĀ-žЀ-ӿ0-9]+")
CYRILLIC_PATTERN = re.compile(r"[А-Яа-яЁё]")
CASE_REPORT_SIZE_PATTERN = re.compile(r"\bn\s*(?:=|<|<=|≤)\s*(\d+)\b", re.IGNORECASE)
CASE_SERIES_PATTERN = re.compile(r"\b([1-4])\s+cases?\b", re.IGNORECASE)

LANGUAGE_COLUMNS = ["language", "lang", "record_language", "article_language"]
TARGET_LANGUAGES_DEFAULT = ["ru", "es", "de", "fr", "it", "pt", "pl", "tr", "nl"]

LANGUAGE_ALIASES = {
    "ru": "ru",
    "rus": "ru",
    "russian": "ru",
    "рус": "ru",
    "русский": "ru",
    "es": "es",
    "spa": "es",
    "spanish": "es",
    "espanol": "es",
    "español": "es",
    "de": "de",
    "deu": "de",
    "ger": "de",
    "german": "de",
    "deutsch": "de",
    "fr": "fr",
    "fra": "fr",
    "fre": "fr",
    "french": "fr",
    "français": "fr",
    "francais": "fr",
    "it": "it",
    "ita": "it",
    "italian": "it",
    "italiano": "it",
    "pt": "pt",
    "por": "pt",
    "portuguese": "pt",
    "português": "pt",
    "portugues": "pt",
    "pt-br": "pt",
    "pt_br": "pt",
    "pt-pt": "pt",
    "pl": "pl",
    "pol": "pl",
    "polish": "pl",
    "polski": "pl",
    "tr": "tr",
    "tur": "tr",
    "turkish": "tr",
    "türkçe": "tr",
    "turkce": "tr",
    "nl": "nl",
    "nld": "nl",
    "dut": "nl",
    "dutch": "nl",
    "nederlands": "nl",
    "en": "en",
    "eng": "en",
    "english": "en",
}

SPANISH_STOPWORDS = {
    "este",
    "esta",
    "de",
    "la",
    "el",
    "en",
    "con",
    "para",
    "los",
    "las",
    "una",
    "un",
    "del",
    "que",
    "estudio",
}

GERMAN_STOPWORDS = {
    "diese",
    "dieser",
    "dieses",
    "im",
    "den",
    "des",
    "von",
    "der",
    "die",
    "das",
    "und",
    "mit",
    "für",
    "bei",
    "eine",
    "einer",
    "studie",
    "patienten",
    "analyse",
}

FRENCH_STOPWORDS = {
    "de",
    "la",
    "le",
    "les",
    "des",
    "et",
    "avec",
    "pour",
    "une",
    "un",
    "dans",
    "étude",
    "etude",
    "patients",
}

ITALIAN_STOPWORDS = {
    "di",
    "la",
    "il",
    "le",
    "gli",
    "e",
    "con",
    "per",
    "una",
    "un",
    "dello",
    "studio",
    "pazienti",
}

PORTUGUESE_STOPWORDS = {
    "de",
    "da",
    "do",
    "dos",
    "das",
    "e",
    "com",
    "para",
    "uma",
    "um",
    "em",
    "estudo",
    "pacientes",
}

POLISH_STOPWORDS = {
    "to",
    "jest",
    "w",
    "na",
    "z",
    "i",
    "oraz",
    "dla",
    "badanie",
    "pacjentów",
    "pacjenci",
    "wyniki",
}

TURKISH_STOPWORDS = {
    "bu",
    "bir",
    "ve",
    "ile",
    "için",
    "çalışma",
    "hastalar",
    "hastalarda",
    "katılımcı",
    "sonuç",
}

DUTCH_STOPWORDS = {
    "de",
    "het",
    "een",
    "van",
    "en",
    "met",
    "voor",
    "in",
    "studie",
    "patiënten",
    "resultaten",
}

REASON_NO_POPULATION = "No eligible population/context"
REASON_NO_CONCEPT = "No eligible exposure/concept"
REASON_NO_OUTCOME = "No eligible outcome"
REASON_NON_EMPIRICAL = "Non-empirical/theoretical only"
REASON_CASE_REPORT = "Case report likely n<5"
REASON_ANIMAL_ONLY = "Animal-only or non-human"
REASON_LANGUAGE = "Language not eligible"
REASON_OTHER = "Other"


DEFAULT_KEYWORD_RULES = {
    "eligible_languages": TARGET_LANGUAGES_DEFAULT,
    "decision_policy": {
        "conservative_missing_core": True,
    },
    "core_signals": {
        "population": {
            "ru": [],
            "es": [],
            "de": [],
            "fr": [],
            "it": [],
            "pt": [],
            "pl": [],
            "tr": [],
            "nl": [],
        },
        "concept_exposure": {
            "ru": [],
            "es": [],
            "de": [],
            "fr": [],
            "it": [],
            "pt": [],
            "pl": [],
            "tr": [],
            "nl": [],
        },
        "outcome": {
            "ru": [],
            "es": [],
            "de": [],
            "fr": [],
            "it": [],
            "pt": [],
            "pl": [],
            "tr": [],
            "nl": [],
        },
    },
    "support_signals": {
        "empirical": {
            "ru": [
                "исследован",
                "выборк",
                "наблюд",
                "пациент",
                "корреляц",
                "регресс",
                "когорт",
                "рандом",
                "n=",
            ],
            "es": [
                "estudio",
                "muestra",
                "pacientes",
                "observacional",
                "cohorte",
                "correlación",
                "regresión",
                "ensayo",
                "n=",
            ],
            "de": [
                "studie",
                "stichprobe",
                "patienten",
                "beobacht",
                "kohorte",
                "korrelation",
                "regression",
                "randomisiert",
                "n=",
            ],
            "fr": [
                "étude",
                "etude",
                "échantillon",
                "patients",
                "observationnel",
                "cohorte",
                "corrélation",
                "régression",
                "n=",
            ],
            "it": [
                "studio",
                "campione",
                "pazienti",
                "osservazionale",
                "coorte",
                "correlazione",
                "regressione",
                "n=",
            ],
            "pt": [
                "estudo",
                "amostra",
                "pacientes",
                "observacional",
                "coorte",
                "correlação",
                "regressão",
                "n=",
            ],
            "pl": [
                "badanie",
                "próba",
                "pacjent",
                "obserwacyj",
                "kohort",
                "korelac",
                "regresj",
                "randomiz",
                "n=",
            ],
            "tr": [
                "çalışma",
                "örneklem",
                "hasta",
                "gözlemsel",
                "kohort",
                "korelasyon",
                "regresyon",
                "randomize",
                "n=",
            ],
            "nl": [
                "studie",
                "steekproef",
                "patiënten",
                "observationeel",
                "cohort",
                "correlatie",
                "regressie",
                "gerandomiseerd",
                "n=",
            ],
        },
        "human": {
            "ru": ["пациент", "участник", "человек", "женщин", "мужчин", "подрост", "взросл"],
            "es": ["paciente", "participante", "human", "mujeres", "hombres", "adolesc", "adult"],
            "de": ["patient", "teilnehmer", "mensch", "frauen", "männer", "jugend", "erwachsen"],
            "fr": ["patient", "participant", "humain", "femmes", "hommes", "adolescent", "adulte"],
            "it": ["paziente", "partecipante", "umano", "donne", "uomini", "adolescent", "adult"],
            "pt": ["paciente", "participante", "humano", "mulheres", "homens", "adolesc", "adult"],
            "pl": ["pacjent", "uczestnik", "ludzk", "kobiet", "mężczyzn", "młodzież", "dorosł"],
            "tr": ["hasta", "katılımcı", "insan", "kadın", "erkek", "ergen", "yetişkin"],
            "nl": ["patiënt", "deelnemer", "mens", "vrouwen", "mannen", "adolescent", "volwassen"],
        },
    },
    "exclusion_signals": {
        "non_empirical": {
            "ru": ["обзор", "теоретичес", "концептуаль", "редакцион", "комментар"],
            "es": ["revisión", "teórico", "conceptual", "editorial", "comentario"],
            "de": ["übersicht", "theoretisch", "konzeption", "editorial", "kommentar"],
            "fr": ["revue", "théorique", "conceptuel", "éditorial", "commentaire"],
            "it": ["revisione", "teorico", "concettuale", "editoriale", "commento"],
            "pt": ["revisão", "teórico", "conceitual", "editorial", "comentário"],
            "pl": ["przegląd", "teoretycz", "koncepcyjn", "redakcyjn", "komentarz"],
            "tr": ["derleme", "teorik", "kavramsal", "editöryal", "yorum"],
            "nl": ["overzicht", "theoretisch", "conceptueel", "redactioneel", "commentaar"],
        },
        "animal_only": {
            "ru": ["мыш", "крыс", "животн", "in vivo"],
            "es": ["ratón", "rata", "murino", "animales", "in vivo"],
            "de": ["maus", "ratte", "tiermodell", "tiere", "in vivo"],
            "fr": ["souris", "rat", "animal", "modèle animal", "in vivo"],
            "it": ["topo", "ratto", "animale", "modello animale", "in vivo"],
            "pt": ["camundongo", "rato", "animais", "modelo animal", "in vivo"],
            "pl": ["mysz", "szczur", "zwierzę", "model zwierzęcy", "in vivo"],
            "tr": ["fare", "sıçan", "hayvan", "hayvan modeli", "in vivo"],
            "nl": ["muis", "rat", "dier", "diermodel", "in vivo"],
        },
        "case_report": {
            "ru": ["клинический случай", "серия случаев", "описание случая"],
            "es": ["reporte de caso", "caso clínico", "serie de casos"],
            "de": ["fallbericht", "einzelfall", "fallserie"],
            "fr": ["rapport de cas", "cas clinique", "série de cas"],
            "it": ["case report", "caso clinico", "serie di casi"],
            "pt": ["relato de caso", "caso clínico", "série de casos"],
            "pl": ["opis przypadku", "przypadek kliniczny", "seria przypadków"],
            "tr": ["olgu sunumu", "klinik olgu", "olgu serisi"],
            "nl": ["casusrapport", "klinische casus", "casusreeks"],
        },
    },
}


def normalize_text(value: object) -> str:
    text = str(value if value is not None else "").strip()
    text = re.sub(r"\s+", " ", text)
    return "" if text.lower() == "nan" else text


def normalize_language(value: object) -> str:
    raw = normalize_text(value).lower()
    if not raw:
        return ""
    return LANGUAGE_ALIASES.get(raw, raw)


def normalize_and_dedupe_languages(values: list[object]) -> list[str]:
    normalized: list[str] = []
    for value in values:
        language = normalize_language(value)
        if language and language not in normalized:
            normalized.append(language)
    return normalized


def tokenize(text: str) -> list[str]:
    return WORD_PATTERN.findall(text.lower())


def detect_language(text: str, explicit_language: str = "") -> str:
    explicit = normalize_language(explicit_language)
    if explicit:
        return explicit

    lowered = text.lower()
    if len(CYRILLIC_PATTERN.findall(lowered)) >= 2:
        return "ru"

    tokens = tokenize(lowered)
    language_scores = {
        "es": sum(1 for token in tokens if token in SPANISH_STOPWORDS),
        "de": sum(1 for token in tokens if token in GERMAN_STOPWORDS),
        "fr": sum(1 for token in tokens if token in FRENCH_STOPWORDS),
        "it": sum(1 for token in tokens if token in ITALIAN_STOPWORDS),
        "pt": sum(1 for token in tokens if token in PORTUGUESE_STOPWORDS),
        "pl": sum(1 for token in tokens if token in POLISH_STOPWORDS),
        "tr": sum(1 for token in tokens if token in TURKISH_STOPWORDS),
        "nl": sum(1 for token in tokens if token in DUTCH_STOPWORDS),
    }

    if re.search(r"[áéíóúñ¿¡]", lowered):
        language_scores["es"] += 2
    if re.search(r"[äöüß]", lowered):
        language_scores["de"] += 2
    if re.search(r"[àâæçéèêëîïôœùûüÿ]", lowered):
        language_scores["fr"] += 2
    if re.search(r"[àèéìíîòóù]", lowered):
        language_scores["it"] += 2
    if re.search(r"[ãõáâàçéêíóôú]", lowered):
        language_scores["pt"] += 2
    if re.search(r"[ąćęłńóśźż]", lowered):
        language_scores["pl"] += 2
    if re.search(r"[çğış]", lowered):
        language_scores["tr"] += 2

    best_language = max(language_scores, key=language_scores.get)
    if language_scores[best_language] >= 2:
        return best_language
    return "en"


def deep_merge_dicts(base: dict, override: dict) -> dict:
    merged: dict = {}
    keys = set(base) | set(override)
    for key in keys:
        base_value = base.get(key)
        override_value = override.get(key)
        if isinstance(base_value, dict) and isinstance(override_value, dict):
            merged[key] = deep_merge_dicts(base_value, override_value)
        elif key in override:
            merged[key] = override_value
        else:
            merged[key] = base_value
    return merged


def normalize_keyword_list(keywords: object) -> tuple[list[str], int]:
    if not isinstance(keywords, list):
        return [], 0

    cleaned: list[str] = []
    ignored_placeholders = 0

    for keyword in keywords:
        value = normalize_text(keyword).lower()
        if not value:
            continue
        if PLACEHOLDER_PATTERN.search(value):
            ignored_placeholders += 1
            continue
        cleaned.append(value)

    deduped = list(dict.fromkeys(cleaned))
    return deduped, ignored_placeholders


def load_keyword_rules(path: Path) -> tuple[dict, dict[str, object]]:
    loaded_from_file = path.exists()

    if loaded_from_file:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise SystemExit(f"Keyword rules file must contain JSON object: {path}")
        merged = deep_merge_dicts(DEFAULT_KEYWORD_RULES, payload)
    else:
        merged = deep_merge_dicts(DEFAULT_KEYWORD_RULES, {})

    ignored_placeholders = 0
    for section_name in ("core_signals", "support_signals", "exclusion_signals"):
        section = merged.get(section_name, {})
        if not isinstance(section, dict):
            merged[section_name] = {}
            continue
        for group_name, group_value in section.items():
            if not isinstance(group_value, dict):
                section[group_name] = {language: [] for language in TARGET_LANGUAGES_DEFAULT}
                continue
            for language in list(group_value.keys()):
                cleaned, ignored = normalize_keyword_list(group_value.get(language, []))
                group_value[language] = cleaned
                ignored_placeholders += ignored

    eligible_languages = merged.get("eligible_languages", TARGET_LANGUAGES_DEFAULT)
    if not isinstance(eligible_languages, list):
        eligible_languages = TARGET_LANGUAGES_DEFAULT
    merged["eligible_languages"] = normalize_and_dedupe_languages(eligible_languages)

    decision_policy = merged.get("decision_policy", {})
    if not isinstance(decision_policy, dict):
        decision_policy = {}
    merged["decision_policy"] = decision_policy

    metadata = {
        "loaded_from_file": loaded_from_file,
        "rules_path": path.as_posix(),
        "ignored_placeholders": ignored_placeholders,
    }
    return merged, metadata


def parse_title_abstract_reasons(path: Path) -> set[str]:
    if not path.exists():
        return {
            REASON_NO_POPULATION,
            REASON_NO_CONCEPT,
            REASON_NO_OUTCOME,
            REASON_NON_EMPIRICAL,
            REASON_CASE_REPORT,
            REASON_ANIMAL_ONLY,
            REASON_LANGUAGE,
            REASON_OTHER,
        }

    text = path.read_text(encoding="utf-8")
    heading_marker = "### Recommended title/abstract exclusion reasons"
    heading_index = text.find(heading_marker)
    if heading_index == -1:
        return set()

    tail = text[heading_index + len(heading_marker) :]
    lines = tail.splitlines()

    reasons: set[str] = set()
    for line in lines:
        stripped = line.strip()
        if not stripped:
            if reasons:
                break
            continue
        if stripped.startswith("###"):
            break
        bullet_match = re.match(r"^[-*+]\s+`?([^`]+?)`?$", stripped)
        if bullet_match:
            reasons.add(bullet_match.group(1).strip())
    return reasons


def find_matches(text_lower: str, keywords: list[str]) -> list[str]:
    matches: list[str] = []
    for keyword in keywords:
        if keyword and keyword in text_lower:
            matches.append(keyword)
    return matches


def small_case_report_detected(text_lower: str) -> bool:
    for match in CASE_REPORT_SIZE_PATTERN.finditer(text_lower):
        try:
            n_value = int(match.group(1))
        except ValueError:
            continue
        if n_value < 5:
            return True

    if CASE_SERIES_PATTERN.search(text_lower):
        return True
    return False


def language_specific_keywords(rules: dict, section: str, group: str, language: str) -> list[str]:
    return rules.get(section, {}).get(group, {}).get(language, [])


def first_non_empty_value(row: pd.Series, columns: list[str]) -> str:
    for column in columns:
        if column in row.index:
            value = normalize_text(row.get(column, ""))
            if value:
                return value
    return ""


def screening_reason_for_missing_group(group_name: str) -> str:
    mapping = {
        "population": REASON_NO_POPULATION,
        "concept_exposure": REASON_NO_CONCEPT,
        "outcome": REASON_NO_OUTCOME,
    }
    return mapping.get(group_name, REASON_OTHER)


def format_matched_groups(trace: dict[str, list[str]]) -> str:
    segments: list[str] = []
    for group_name in sorted(trace.keys()):
        keywords = trace[group_name]
        if keywords:
            segments.append(f"{group_name}: {', '.join(keywords)}")
    return " | ".join(segments)


def screen_record(
    row: pd.Series,
    *,
    keyword_rules: dict,
    eligible_languages: set[str],
    conservative_missing_core: bool,
    include_english: bool,
    screening_reasons: set[str],
) -> dict | None:
    record_id = normalize_text(row.get("record_id", ""))
    title = normalize_text(row.get("title", ""))
    abstract = normalize_text(row.get("abstract", ""))
    source_database = normalize_text(row.get("source_database", ""))
    year = normalize_text(row.get("year", ""))

    if not record_id or not (title or abstract):
        return None

    explicit_language = first_non_empty_value(row, LANGUAGE_COLUMNS)
    combined_text = f"{title} {abstract}".strip().lower()
    detected_language = detect_language(combined_text, explicit_language)

    if detected_language == "en" and not include_english:
        return None

    if detected_language not in eligible_languages and detected_language != "en":
        reason = REASON_LANGUAGE
        return {
            "record_id": record_id,
            "source_database": source_database,
            "year": year,
            "title": title,
            "detected_language": detected_language,
            "explicit_language": normalize_language(explicit_language),
            "recommended_decision": "exclude",
            "recommended_reason": reason,
            "confidence": "high",
            "reason_in_screening_rules": reason in screening_reasons,
            "matched_groups": "",
            "notes": "Detected non-eligible language by heuristic/metadata.",
        }

    if detected_language == "en" and include_english:
        return None

    matched_groups: dict[str, list[str]] = {}

    animal_hits = find_matches(
        combined_text,
        language_specific_keywords(
            keyword_rules, "exclusion_signals", "animal_only", detected_language
        ),
    )
    human_hits = find_matches(
        combined_text,
        language_specific_keywords(keyword_rules, "support_signals", "human", detected_language),
    )
    non_empirical_hits = find_matches(
        combined_text,
        language_specific_keywords(
            keyword_rules, "exclusion_signals", "non_empirical", detected_language
        ),
    )
    empirical_hits = find_matches(
        combined_text,
        language_specific_keywords(
            keyword_rules, "support_signals", "empirical", detected_language
        ),
    )
    case_report_hits = find_matches(
        combined_text,
        language_specific_keywords(
            keyword_rules, "exclusion_signals", "case_report", detected_language
        ),
    )

    if animal_hits and not human_hits:
        matched_groups["animal_only"] = animal_hits
        reason = REASON_ANIMAL_ONLY
        return {
            "record_id": record_id,
            "source_database": source_database,
            "year": year,
            "title": title,
            "detected_language": detected_language,
            "explicit_language": normalize_language(explicit_language),
            "recommended_decision": "exclude",
            "recommended_reason": reason,
            "confidence": "high",
            "reason_in_screening_rules": reason in screening_reasons,
            "matched_groups": format_matched_groups(matched_groups),
            "notes": "Animal-model signal without human-population signal.",
        }

    if non_empirical_hits and not empirical_hits:
        matched_groups["non_empirical"] = non_empirical_hits
        reason = REASON_NON_EMPIRICAL
        return {
            "record_id": record_id,
            "source_database": source_database,
            "year": year,
            "title": title,
            "detected_language": detected_language,
            "explicit_language": normalize_language(explicit_language),
            "recommended_decision": "exclude",
            "recommended_reason": reason,
            "confidence": "high",
            "reason_in_screening_rules": reason in screening_reasons,
            "matched_groups": format_matched_groups(matched_groups),
            "notes": "Theoretical/non-empirical signal without empirical-study signal.",
        }

    if case_report_hits or small_case_report_detected(combined_text):
        if case_report_hits:
            matched_groups["case_report"] = case_report_hits
        reason = REASON_CASE_REPORT
        return {
            "record_id": record_id,
            "source_database": source_database,
            "year": year,
            "title": title,
            "detected_language": detected_language,
            "explicit_language": normalize_language(explicit_language),
            "recommended_decision": "exclude",
            "recommended_reason": reason,
            "confidence": "medium",
            "reason_in_screening_rules": reason in screening_reasons,
            "matched_groups": format_matched_groups(matched_groups),
            "notes": "Case-report pattern (keyword or small sample-size pattern) detected.",
        }

    missing_core_groups: list[str] = []
    configured_core_groups = 0

    for group_name in ("population", "concept_exposure", "outcome"):
        keywords = language_specific_keywords(
            keyword_rules, "core_signals", group_name, detected_language
        )
        if not keywords:
            continue
        configured_core_groups += 1
        hits = find_matches(combined_text, keywords)
        if hits:
            matched_groups[group_name] = hits
        else:
            missing_core_groups.append(group_name)

    if missing_core_groups:
        reason = screening_reason_for_missing_group(missing_core_groups[0])
        decision = "maybe" if conservative_missing_core else "exclude"
        confidence = "low" if conservative_missing_core else "medium"
        return {
            "record_id": record_id,
            "source_database": source_database,
            "year": year,
            "title": title,
            "detected_language": detected_language,
            "explicit_language": normalize_language(explicit_language),
            "recommended_decision": decision,
            "recommended_reason": reason,
            "confidence": confidence,
            "reason_in_screening_rules": reason in screening_reasons,
            "matched_groups": format_matched_groups(matched_groups),
            "notes": f"Missing core keyword match for: {', '.join(missing_core_groups)}.",
        }

    if configured_core_groups > 0 and empirical_hits:
        matched_groups["empirical"] = empirical_hits
        return {
            "record_id": record_id,
            "source_database": source_database,
            "year": year,
            "title": title,
            "detected_language": detected_language,
            "explicit_language": normalize_language(explicit_language),
            "recommended_decision": "include",
            "recommended_reason": "Core keyword groups matched",
            "confidence": "high",
            "reason_in_screening_rules": True,
            "matched_groups": format_matched_groups(matched_groups),
            "notes": "Population/concept/outcome keyword evidence with empirical-study signal.",
        }

    if configured_core_groups > 0:
        return {
            "record_id": record_id,
            "source_database": source_database,
            "year": year,
            "title": title,
            "detected_language": detected_language,
            "explicit_language": normalize_language(explicit_language),
            "recommended_decision": "include",
            "recommended_reason": "Core keyword groups matched",
            "confidence": "medium",
            "reason_in_screening_rules": True,
            "matched_groups": format_matched_groups(matched_groups),
            "notes": "Core groups matched; empirical-study signal not explicit.",
        }

    return {
        "record_id": record_id,
        "source_database": source_database,
        "year": year,
        "title": title,
        "detected_language": detected_language,
        "explicit_language": normalize_language(explicit_language),
        "recommended_decision": "maybe",
        "recommended_reason": REASON_OTHER,
        "confidence": "low",
        "reason_in_screening_rules": REASON_OTHER in screening_reasons,
        "matched_groups": format_matched_groups(matched_groups),
        "notes": "No review-specific core keyword rules configured for this language.",
    }


def render_summary(
    *,
    records_path: Path,
    screening_rules_path: Path,
    keyword_rules_path: Path,
    rules_metadata: dict[str, object],
    recommendations_df: pd.DataFrame,
    summary_output_path: Path,
    eligible_languages: list[str],
    conservative_missing_core: bool,
    configured_core_counts: dict[str, int],
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines: list[str] = []
    lines.append("# Multi-language Abstract Screener Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Inputs/Outputs")
    lines.append("")
    lines.append(f"- Records input: `{records_path.as_posix()}`")
    lines.append(f"- Screening rules input: `{screening_rules_path.as_posix()}`")
    lines.append(f"- Keyword rules input: `{keyword_rules_path.as_posix()}`")
    lines.append(f"- Summary output: `{summary_output_path.as_posix()}`")
    lines.append("")
    lines.append("## Policy")
    lines.append("")
    lines.append(
        f"- Eligible non-English languages: {', '.join(eligible_languages) if eligible_languages else 'none'}"
    )
    lines.append(
        f"- Conservative missing-core policy: {'on' if conservative_missing_core else 'off'}"
    )
    lines.append(
        f"- Keyword rules loaded from file: {'yes' if rules_metadata.get('loaded_from_file') else 'no (defaults used)'}"
    )
    lines.append(
        f"- Placeholder keyword tokens ignored: {int(rules_metadata.get('ignored_placeholders', 0))}"
    )
    lines.append("")
    lines.append("## Config Coverage")
    lines.append("")
    for group_name in ("population", "concept_exposure", "outcome"):
        lines.append(
            f"- `{group_name}` keyword entries across target languages: {configured_core_counts.get(group_name, 0)}"
        )
    lines.append("")
    lines.append("## Recommendation Counts")
    lines.append("")
    lines.append(f"- Records evaluated: {int(recommendations_df.shape[0])}")

    if recommendations_df.empty:
        lines.append("- No non-English abstracts matched evaluation criteria.")
        lines.append("")
        return "\n".join(lines)

    by_decision = recommendations_df["recommended_decision"].value_counts(dropna=False)
    for decision, count in by_decision.items():
        lines.append(f"- `{decision}`: {int(count)}")

    lines.append("")
    lines.append("## By Language")
    lines.append("")
    for language, count in (
        recommendations_df["detected_language"].value_counts(dropna=False).items()
    ):
        lines.append(f"- `{language}`: {int(count)}")

    lines.append("")
    lines.append("## By Reason")
    lines.append("")
    for reason, count in (
        recommendations_df["recommended_reason"].value_counts(dropna=False).items()
    ):
        lines.append(f"- {reason}: {int(count)}")

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- This screener is deterministic keyword logic (no ML).")
    lines.append("- Use outputs for triage/calibration; final decisions stay reviewer-led.")
    lines.append(
        "- Recommended reasons are aligned to `screening_rules.md` title/abstract reason labels."
    )
    lines.append("")
    return "\n".join(lines)


def parser() -> argparse.ArgumentParser:
    cli_parser = argparse.ArgumentParser(
        description=(
            "Apply deterministic multilingual keyword screening rules "
            "to non-English abstracts and generate reviewer recommendation tables."
        )
    )
    cli_parser.add_argument(
        "--records",
        default="../02_data/processed/master_records.csv",
        help="Path to master records CSV containing title/abstract fields.",
    )
    cli_parser.add_argument(
        "--screening-rules",
        default="../01_protocol/screening_rules.md",
        help="Path to screening rules markdown (reason label alignment).",
    )
    cli_parser.add_argument(
        "--keyword-rules",
        default="../01_protocol/multilang_keyword_rules.json",
        help="Path to multilingual keyword rules JSON.",
    )
    cli_parser.add_argument(
        "--eligible-languages",
        default=",".join(TARGET_LANGUAGES_DEFAULT),
        help=(
            "Comma-separated target non-English language codes "
            f"(default: {','.join(TARGET_LANGUAGES_DEFAULT)})."
        ),
    )
    cli_parser.add_argument(
        "--include-english",
        action="store_true",
        help="Include English-language abstracts in evaluation (default: disabled).",
    )
    cli_parser.add_argument(
        "--conservative-missing-core",
        action="store_true",
        help="Force conservative mode: missing core keyword groups => `maybe` instead of `exclude`.",
    )
    cli_parser.add_argument(
        "--recommendations-output",
        default="outputs/multilang_abstract_screening_recommendations.csv",
        help="Path to recommendations CSV output.",
    )
    cli_parser.add_argument(
        "--summary-output",
        default="outputs/multilang_abstract_screening_summary.md",
        help="Path to markdown summary output.",
    )
    return cli_parser


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)

    records_path = Path(args.records)
    screening_rules_path = Path(args.screening_rules)
    keyword_rules_path = Path(args.keyword_rules)
    recommendations_output_path = Path(args.recommendations_output)
    summary_output_path = Path(args.summary_output)

    if not records_path.exists():
        raise SystemExit(f"Records file not found: {records_path}")

    records_df = pd.read_csv(records_path, dtype=str)
    keyword_rules, rules_metadata = load_keyword_rules(keyword_rules_path)
    screening_reasons = parse_title_abstract_reasons(screening_rules_path)

    cli_languages = normalize_and_dedupe_languages(args.eligible_languages.split(","))
    eligible_languages = (
        set(cli_languages) if cli_languages else set(keyword_rules.get("eligible_languages", []))
    )
    if not eligible_languages:
        eligible_languages = set(TARGET_LANGUAGES_DEFAULT)

    policy_default = bool(
        keyword_rules.get("decision_policy", {}).get("conservative_missing_core", True)
    )
    conservative_missing_core = args.conservative_missing_core or policy_default

    configured_core_counts: dict[str, int] = {}
    for group_name in ("population", "concept_exposure", "outcome"):
        count = 0
        for language in eligible_languages:
            count += len(
                language_specific_keywords(keyword_rules, "core_signals", group_name, language)
            )
        configured_core_counts[group_name] = count

    recommendations: list[dict] = []
    for _, row in records_df.iterrows():
        recommendation = screen_record(
            row,
            keyword_rules=keyword_rules,
            eligible_languages=eligible_languages,
            conservative_missing_core=conservative_missing_core,
            include_english=bool(args.include_english),
            screening_reasons=screening_reasons,
        )
        if recommendation is not None:
            recommendations.append(recommendation)

    recommendations_df = pd.DataFrame(recommendations)
    if not recommendations_df.empty:
        recommendations_df = recommendations_df.sort_values(
            ["recommended_decision", "detected_language", "record_id"],
            kind="stable",
        ).reset_index(drop=True)

    recommendations_output_path.parent.mkdir(parents=True, exist_ok=True)
    recommendations_df.to_csv(recommendations_output_path, index=False)

    summary_text = render_summary(
        records_path=records_path,
        screening_rules_path=screening_rules_path,
        keyword_rules_path=keyword_rules_path,
        rules_metadata=rules_metadata,
        recommendations_df=recommendations_df,
        summary_output_path=summary_output_path,
        eligible_languages=sorted(eligible_languages),
        conservative_missing_core=conservative_missing_core,
        configured_core_counts=configured_core_counts,
    )
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_output_path.write_text(summary_text, encoding="utf-8")

    print(f"Wrote: {recommendations_output_path}")
    print(f"Wrote: {summary_output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
