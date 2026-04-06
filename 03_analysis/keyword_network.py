import argparse
import re
from collections import Counter
from datetime import datetime
from itertools import combinations
from pathlib import Path

import networkx as nx
import pandas as pd

try:
    from nltk.stem import PorterStemmer
    from nltk.tokenize import RegexpTokenizer

    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False


BASE_STOPWORDS_EN = {
    "a",
    "about",
    "above",
    "after",
    "again",
    "against",
    "all",
    "also",
    "am",
    "an",
    "and",
    "any",
    "are",
    "as",
    "at",
    "be",
    "because",
    "been",
    "before",
    "being",
    "below",
    "between",
    "both",
    "but",
    "by",
    "can",
    "could",
    "did",
    "do",
    "does",
    "doing",
    "down",
    "during",
    "each",
    "few",
    "for",
    "from",
    "further",
    "had",
    "has",
    "have",
    "having",
    "he",
    "her",
    "here",
    "hers",
    "herself",
    "him",
    "himself",
    "his",
    "how",
    "i",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "itself",
    "just",
    "me",
    "more",
    "most",
    "my",
    "myself",
    "no",
    "nor",
    "not",
    "now",
    "of",
    "off",
    "on",
    "once",
    "only",
    "or",
    "other",
    "our",
    "ours",
    "ourselves",
    "out",
    "over",
    "own",
    "same",
    "she",
    "should",
    "so",
    "some",
    "such",
    "than",
    "that",
    "the",
    "their",
    "theirs",
    "them",
    "themselves",
    "then",
    "there",
    "these",
    "they",
    "this",
    "those",
    "through",
    "to",
    "too",
    "under",
    "until",
    "up",
    "very",
    "was",
    "we",
    "were",
    "what",
    "when",
    "where",
    "which",
    "while",
    "who",
    "whom",
    "why",
    "will",
    "with",
    "would",
    "you",
    "your",
    "yours",
    "yourself",
    "yourselves",
}

BASE_STOPWORDS_RU = {
    "и",
    "в",
    "во",
    "не",
    "что",
    "он",
    "на",
    "я",
    "с",
    "со",
    "как",
    "а",
    "то",
    "все",
    "она",
    "так",
    "его",
    "но",
    "да",
    "ты",
    "к",
    "у",
    "же",
    "вы",
    "за",
    "бы",
    "по",
    "ее",
    "мне",
    "есть",
    "они",
    "тут",
    "где",
    "когда",
    "или",
    "если",
    "при",
    "для",
    "из",
    "от",
    "до",
    "над",
    "под",
    "мы",
    "вы",
    "нас",
    "вам",
    "их",
    "еще",
}

DOMAIN_STOPWORDS = {
    "abstract",
    "study",
    "studies",
    "result",
    "results",
    "sample",
    "samples",
    "method",
    "methods",
    "analysis",
    "analyses",
    "participant",
    "participants",
    "group",
    "groups",
    "finding",
    "findings",
    "paper",
    "review",
}

BLOCK_B_HINTS = {
    "attach",
    "mentaliz",
    "reflect",
    "object",
    "relation",
    "interperson",
    "psychodynam",
    "insecure",
    "function",
}

BLOCK_C_HINTS = {
    "identity",
    "self",
    "esteem",
    "concept",
    "coher",
    "diffus",
    "disturb",
    "representation",
    "alexithym",
    "emotion",
    "regulation",
}

EMPTY_VALUES = {"", "nan", "none"}


def clean_text(value: object) -> str:
    text = str(value if value is not None else "")
    text = re.sub(r"\s+", " ", text).strip()
    return "" if text.lower() in EMPTY_VALUES else text


def normalize_query_text(text: str) -> str:
    normalized = text.lower().replace("*", "")
    normalized = re.sub(r"[^a-zа-яё0-9\s]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def extract_block_query_text(search_strings_path: Path, fallback_query_path: Path) -> tuple[str, str]:
    block_queries: list[str] = []
    if search_strings_path.exists():
        text = search_strings_path.read_text(encoding="utf-8")
        pattern = re.compile(r"###\s*Block\s*([BC]).*?`([^`]+)`", re.IGNORECASE | re.DOTALL)
        matches = list(pattern.finditer(text))
        for match in matches:
            block_queries.append(clean_text(match.group(2)))

    if block_queries:
        return " ".join(block_queries), f"{search_strings_path.as_posix()} (Block B/C)"

    if fallback_query_path.exists():
        return clean_text(fallback_query_path.read_text(encoding="utf-8")), fallback_query_path.as_posix()

    return "", "missing"


def is_duplicate_row(row: pd.Series) -> bool:
    value = clean_text(row.get("is_duplicate", "")).lower()
    return value in {"yes", "y", "1", "true"}


def resolve_text_columns(df: pd.DataFrame, requested: str) -> list[str]:
    if requested.strip().lower() != "auto":
        columns = [part.strip() for part in requested.split(",") if part.strip()]
        return [column for column in columns if column in df.columns]

    preferred = ["title", "abstract", "keywords", "notes"]
    return [column for column in preferred if column in df.columns]


class TextProcessor:
    def __init__(self) -> None:
        self.stopwords = BASE_STOPWORDS_EN | BASE_STOPWORDS_RU | DOMAIN_STOPWORDS
        self.token_pattern = re.compile(r"[A-Za-zА-Яа-яЁё][A-Za-zА-Яа-яЁё\-]{1,}")
        self.tokenizer = RegexpTokenizer(r"[A-Za-zА-Яа-яЁё][A-Za-zА-Яа-яЁё\-]{1,}") if NLTK_AVAILABLE else None
        self.stemmer = PorterStemmer() if NLTK_AVAILABLE else None

    def tokenize(self, text: str) -> list[str]:
        if not text:
            return []

        if self.tokenizer is not None:
            raw_tokens = self.tokenizer.tokenize(text.lower())
        else:
            raw_tokens = self.token_pattern.findall(text.lower())

        normalized_tokens: list[str] = []
        for token in raw_tokens:
            normalized = re.sub(r"^-+|-+$", "", token)
            normalized = re.sub(r"-{2,}", "-", normalized)
            normalized = normalized.replace("-", "")
            if len(normalized) < 3:
                continue
            if normalized in self.stopwords:
                continue
            if self.stemmer is not None and re.fullmatch(r"[a-z]+", normalized):
                normalized = self.stemmer.stem(normalized)
            if normalized in self.stopwords:
                continue
            normalized_tokens.append(normalized)
        return normalized_tokens


def build_document_texts(df: pd.DataFrame, text_columns: list[str], include_duplicates: bool) -> list[str]:
    documents: list[str] = []
    if not text_columns:
        return documents

    for _, row in df.iterrows():
        if not include_duplicates and is_duplicate_row(row):
            continue

        parts = [clean_text(row.get(column, "")) for column in text_columns]
        parts = [part for part in parts if part]
        if not parts:
            continue

        combined = " ".join(parts).strip()
        if len(combined) < 20:
            continue
        documents.append(combined)

    return documents


def term_set_from_tokens(tokens: list[str]) -> set[str]:
    terms: set[str] = set(tokens)
    for idx in range(len(tokens) - 1):
        left, right = tokens[idx], tokens[idx + 1]
        if left == right:
            continue
        terms.add(f"{left} {right}")
    return terms


def split_sentences(text: str) -> list[str]:
    parts = re.split(r"[\.!?;]+", text)
    return [part.strip() for part in parts if clean_text(part)]


def build_term_sets(documents: list[str], processor: TextProcessor) -> tuple[list[set[str]], list[set[str]]]:
    doc_term_sets: list[set[str]] = []
    context_term_sets: list[set[str]] = []

    for document in documents:
        tokens = processor.tokenize(document)
        if len(tokens) < 3:
            continue

        doc_terms = term_set_from_tokens(tokens)
        if not doc_terms:
            continue
        doc_term_sets.append(doc_terms)

        sentence_terms_added = 0
        for sentence in split_sentences(document):
            sentence_tokens = processor.tokenize(sentence)
            if len(sentence_tokens) < 2:
                continue
            sentence_terms = term_set_from_tokens(sentence_tokens)
            if not sentence_terms:
                continue
            context_term_sets.append(sentence_terms)
            sentence_terms_added += 1

        if sentence_terms_added == 0:
            context_term_sets.append(doc_terms)

    return doc_term_sets, context_term_sets


def document_frequency(term_sets: list[set[str]]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for terms in term_sets:
        counts.update(terms)
    return counts


def limit_doc_terms(terms: list[str], counts: Counter[str], max_terms_per_doc: int) -> list[str]:
    if max_terms_per_doc <= 0 or len(terms) <= max_terms_per_doc:
        return terms

    ranked = sorted(terms, key=lambda term: (-counts[term], term))
    return ranked[:max_terms_per_doc]


def build_cooccurrence_graph(
    doc_term_sets: list[set[str]],
    context_term_sets: list[set[str]],
    counts: Counter[str],
    min_term_doc_freq: int,
    max_terms_per_doc: int,
) -> tuple[nx.Graph, Counter[str]]:
    graph = nx.Graph()
    node_doc_counts: Counter[str] = Counter()

    for term_set in doc_term_sets:
        retained = sorted(term for term in term_set if counts[term] >= min_term_doc_freq)
        retained = limit_doc_terms(retained, counts, max_terms_per_doc)
        if not retained:
            continue

        for term in retained:
            node_doc_counts[term] += 1
            if not graph.has_node(term):
                graph.add_node(term)

    for term_set in context_term_sets:
        retained = sorted(term for term in term_set if counts[term] >= min_term_doc_freq)
        retained = limit_doc_terms(retained, counts, max_terms_per_doc)
        retained = [term for term in retained if term in graph]
        if len(retained) < 2:
            continue

        for left, right in combinations(retained, 2):
            if graph.has_edge(left, right):
                graph[left][right]["weight"] += 1
            else:
                graph.add_edge(left, right, weight=1)

    return graph, node_doc_counts


def node_metrics_df(graph: nx.Graph, node_doc_counts: Counter[str]) -> pd.DataFrame:
    if graph.number_of_nodes() == 0:
        return pd.DataFrame(
            columns=["term", "ngram_size", "n_docs", "weighted_degree", "degree_centrality", "betweenness"]
        )

    weighted_degree = dict(graph.degree(weight="weight"))
    degree_centrality = nx.degree_centrality(graph)
    betweenness = nx.betweenness_centrality(graph, weight="weight", normalized=True) if graph.number_of_nodes() > 2 else {}

    rows: list[dict[str, object]] = []
    for term in graph.nodes:
        rows.append(
            {
                "term": term,
                "ngram_size": 2 if " " in term else 1,
                "n_docs": int(node_doc_counts.get(term, 0)),
                "weighted_degree": float(weighted_degree.get(term, 0.0)),
                "degree_centrality": float(degree_centrality.get(term, 0.0)),
                "betweenness": float(betweenness.get(term, 0.0)),
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    return df.sort_values(["weighted_degree", "n_docs", "term"], ascending=[False, False, True]).reset_index(drop=True)


def edge_metrics_df(graph: nx.Graph, min_edge_weight: int) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for left, right, payload in graph.edges(data=True):
        weight = int(payload.get("weight", 0))
        if weight < min_edge_weight:
            continue
        rows.append({"source": left, "target": right, "weight": weight})

    if not rows:
        return pd.DataFrame(columns=["source", "target", "weight"])

    df = pd.DataFrame(rows)
    return df.sort_values(["weight", "source", "target"], ascending=[False, True, True]).reset_index(drop=True)


def term_in_query(term: str, normalized_query_text: str) -> bool:
    if not term or not normalized_query_text:
        return False
    haystack = f" {normalized_query_text} "
    needle = f" {term} "
    return needle in haystack


def candidate_df(
    nodes_df: pd.DataFrame,
    normalized_query_text: str,
    min_term_doc_freq: int,
    min_weighted_degree: float,
    top_candidates: int,
) -> pd.DataFrame:
    if nodes_df.empty:
        return pd.DataFrame(
            columns=[
                "term",
                "ngram_size",
                "n_docs",
                "weighted_degree",
                "degree_centrality",
                "betweenness",
                "in_query",
                "priority_score",
            ]
        )

    working = nodes_df.copy()
    working["in_query"] = working["term"].apply(lambda term: term_in_query(str(term), normalized_query_text))
    working["priority_score"] = (working["weighted_degree"] * 0.7) + (working["n_docs"] * 0.3)

    mask = (~working["in_query"]) & (working["n_docs"] >= min_term_doc_freq) & (working["weighted_degree"] >= min_weighted_degree)
    filtered = working.loc[mask].copy()

    if filtered.empty:
        return filtered

    filtered = filtered.sort_values(
        ["priority_score", "weighted_degree", "n_docs", "degree_centrality", "term"],
        ascending=[False, False, False, False, True],
    ).reset_index(drop=True)

    if top_candidates > 0:
        filtered = filtered.head(top_candidates).copy()
    return filtered


def classify_candidate_block(term: str) -> str:
    normalized = term.lower().replace(" ", "")
    score_b = sum(1 for hint in BLOCK_B_HINTS if hint in normalized)
    score_c = sum(1 for hint in BLOCK_C_HINTS if hint in normalized)

    if score_b == 0 and score_c == 0:
        return "unsorted"
    if score_b > score_c:
        return "B"
    if score_c > score_b:
        return "C"
    return "both"


def term_to_query_fragment(term: str) -> str:
    escaped = term.replace('"', "")
    if " " in escaped:
        return f'"{escaped}"[Title/Abstract]'
    return f"{escaped}[Title/Abstract]"


def join_query_fragments(fragments: list[str]) -> str:
    if not fragments:
        return ""
    if len(fragments) == 1:
        return fragments[0]

    lines = ["("]
    for idx, fragment in enumerate(fragments):
        connector = " OR" if idx < len(fragments) - 1 else ""
        lines.append(f"  {fragment}{connector}")
    lines.append(")")
    return "\n".join(lines)


def build_block_suggestions(
    *,
    candidates: pd.DataFrame,
    query_source: str,
    generated_at: str,
    top_per_block: int,
) -> str:
    lines: list[str] = []
    lines.append("# Block B/C Expansion Suggestions (Auto)")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append(f"Query source: `{query_source}`")
    lines.append("")
    lines.append("These are candidate additions from co-occurrence analysis.")
    lines.append("Review manually before adding to final database queries.")
    lines.append("")

    if candidates.empty:
        lines.append("No candidates available under current thresholds.")
        lines.append("")
        return "\n".join(lines) + "\n"

    working = candidates.copy()
    if "suggested_block" not in working.columns:
        working["suggested_block"] = working["term"].apply(lambda value: classify_candidate_block(str(value)))

    block_b_df = working.loc[working["suggested_block"].isin(["B", "both"])]
    block_c_df = working.loc[working["suggested_block"].isin(["C", "both"])]
    unsorted_df = working.loc[working["suggested_block"].eq("unsorted")]

    def block_section(title: str, frame: pd.DataFrame) -> None:
        lines.append(f"## {title}")
        lines.append("")
        if frame.empty:
            lines.append("- No suggestions.")
            lines.append("")
            return

        top_frame = frame.head(top_per_block)
        for _, row in top_frame.iterrows():
            lines.append(
                f"- `{row['term']}` (docs={int(row['n_docs'])}, weighted_degree={row['weighted_degree']:.1f}, block={row['suggested_block']})"
            )

        fragments = [term_to_query_fragment(str(term)) for term in top_frame["term"].tolist()]
        query_snippet = join_query_fragments(fragments)
        lines.append("")
        lines.append("Suggested query snippet:")
        lines.append("")
        lines.append("```text")
        lines.append(query_snippet)
        lines.append("```")
        lines.append("")

    block_section("Block B Candidates", block_b_df)
    block_section("Block C Candidates", block_c_df)

    lines.append("## Unsorted Candidates")
    lines.append("")
    if unsorted_df.empty:
        lines.append("- None.")
    else:
        for _, row in unsorted_df.head(top_per_block).iterrows():
            lines.append(f"- `{row['term']}` (docs={int(row['n_docs'])}, weighted_degree={row['weighted_degree']:.1f})")
    lines.append("")

    return "\n".join(lines) + "\n"


def build_summary(
    *,
    generated_at: str,
    master_path: Path,
    search_source: str,
    text_columns: list[str],
    documents_total: int,
    term_sets_total: int,
    nodes_df: pd.DataFrame,
    edges_df: pd.DataFrame,
    candidates: pd.DataFrame,
    suggestions_output: Path,
) -> str:
    lines: list[str] = []
    lines.append("# Keyword Network Analysis Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Inputs")
    lines.append("")
    lines.append(f"- Master records: `{master_path.as_posix()}`")
    lines.append(f"- Query source for Block B/C comparison: `{search_source}`")
    lines.append(f"- Text columns used: {', '.join(text_columns) if text_columns else 'none'}")
    lines.append(f"- NLP backend: {'nltk+networkx' if NLTK_AVAILABLE else 'regex-fallback+networkx (nltk unavailable)'}")
    lines.append("")
    lines.append("## Corpus Stats")
    lines.append("")
    lines.append(f"- Documents processed: {documents_total}")
    lines.append(f"- Documents retained after token/term filters: {term_sets_total}")
    lines.append(f"- Network nodes: {len(nodes_df)}")
    lines.append(f"- Network edges (after edge filter): {len(edges_df)}")
    lines.append(f"- Candidate terms missing from query: {len(candidates)}")
    lines.append("")
    lines.append("## Candidate Terms")
    lines.append("")

    if candidates.empty:
        lines.append("- No candidate terms passed current thresholds.")
    else:
        top = candidates.head(20)
        for _, row in top.iterrows():
            lines.append(
                f"- `{row['term']}` (docs={int(row['n_docs'])}, weighted_degree={row['weighted_degree']:.1f}, score={row['priority_score']:.2f})"
            )

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- Candidates indicate frequently co-occurring terms absent from current Block B/C query text.")
    lines.append("- Review candidates manually before adding to search strings (precision/recall trade-off).")
    lines.append(f"- Ready-to-paste query suggestions: `{suggestions_output.as_posix()}`")

    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a litsearchr-style keyword co-occurrence network from master_records and suggest missing Block B/C terms."
    )
    parser.add_argument("--master", default="../02_data/processed/master_records.csv", help="Path to master records CSV")
    parser.add_argument("--search-strings", default="../01_protocol/search_strings.md", help="Path to search strategy markdown")
    parser.add_argument("--pubmed-query", default="../01_protocol/pubmed_query_v0.2.txt", help="Fallback path to PubMed query text")
    parser.add_argument("--text-columns", default="auto", help="Comma-separated text columns or 'auto'")
    parser.add_argument("--include-duplicates", action="store_true", help="Include duplicate rows from master records")
    parser.add_argument("--min-term-doc-freq", type=int, default=2, help="Minimum document frequency for terms")
    parser.add_argument("--min-weighted-degree", type=float, default=2.0, help="Minimum weighted degree for candidates")
    parser.add_argument("--min-edge-weight", type=int, default=2, help="Minimum edge weight in exported network")
    parser.add_argument("--max-terms-per-doc", type=int, default=60, help="Cap on filtered terms retained per document")
    parser.add_argument("--top-candidates", type=int, default=40, help="Maximum candidate terms to output")
    parser.add_argument("--nodes-output", default="outputs/keyword_network_nodes.csv", help="Output CSV for node metrics")
    parser.add_argument("--edges-output", default="outputs/keyword_network_edges.csv", help="Output CSV for edge metrics")
    parser.add_argument("--candidates-output", default="outputs/keyword_candidates.csv", help="Output CSV for missing-query candidates")
    parser.add_argument(
        "--suggestions-output",
        default="outputs/keyword_block_bc_suggestions.md",
        help="Output markdown with ready-to-paste Block B/C snippets",
    )
    parser.add_argument("--suggestions-top-per-block", type=int, default=12, help="Maximum suggestions per block section")
    parser.add_argument("--summary", default="outputs/keyword_analysis_summary.md", help="Output markdown summary")
    args = parser.parse_args()

    if args.min_term_doc_freq < 1:
        raise ValueError("--min-term-doc-freq must be >= 1")
    if args.min_edge_weight < 1:
        raise ValueError("--min-edge-weight must be >= 1")
    if args.max_terms_per_doc < 1:
        raise ValueError("--max-terms-per-doc must be >= 1")
    if args.top_candidates < 1:
        raise ValueError("--top-candidates must be >= 1")
    if args.suggestions_top_per_block < 1:
        raise ValueError("--suggestions-top-per-block must be >= 1")

    master_path = Path(args.master)
    search_strings_path = Path(args.search_strings)
    fallback_query_path = Path(args.pubmed_query)

    nodes_output = Path(args.nodes_output)
    edges_output = Path(args.edges_output)
    candidates_output = Path(args.candidates_output)
    suggestions_output = Path(args.suggestions_output)
    summary_output = Path(args.summary)

    if not master_path.exists():
        raise FileNotFoundError(f"Master records CSV not found: {master_path}")

    master_df = pd.read_csv(master_path)
    text_columns = resolve_text_columns(master_df, args.text_columns)
    documents = build_document_texts(master_df, text_columns, include_duplicates=args.include_duplicates)

    processor = TextProcessor()
    doc_term_sets, context_term_sets = build_term_sets(documents, processor)
    term_counts = document_frequency(doc_term_sets)
    graph, node_doc_counts = build_cooccurrence_graph(
        doc_term_sets,
        context_term_sets,
        term_counts,
        min_term_doc_freq=args.min_term_doc_freq,
        max_terms_per_doc=args.max_terms_per_doc,
    )

    nodes_df = node_metrics_df(graph, node_doc_counts)
    edges_df = edge_metrics_df(graph, min_edge_weight=args.min_edge_weight)

    query_text, query_source = extract_block_query_text(search_strings_path, fallback_query_path)
    normalized_query = normalize_query_text(query_text)

    candidates = candidate_df(
        nodes_df,
        normalized_query,
        min_term_doc_freq=args.min_term_doc_freq,
        min_weighted_degree=args.min_weighted_degree,
        top_candidates=args.top_candidates,
    )
    if not candidates.empty:
        candidates = candidates.copy()
        candidates["suggested_block"] = candidates["term"].apply(lambda value: classify_candidate_block(str(value)))

    nodes_output.parent.mkdir(parents=True, exist_ok=True)
    edges_output.parent.mkdir(parents=True, exist_ok=True)
    candidates_output.parent.mkdir(parents=True, exist_ok=True)
    suggestions_output.parent.mkdir(parents=True, exist_ok=True)
    summary_output.parent.mkdir(parents=True, exist_ok=True)

    nodes_df.to_csv(nodes_output, index=False)
    edges_df.to_csv(edges_output, index=False)
    candidates.to_csv(candidates_output, index=False)

    suggestions_text = build_block_suggestions(
        candidates=candidates,
        query_source=query_source,
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
        top_per_block=args.suggestions_top_per_block,
    )
    suggestions_output.write_text(suggestions_text, encoding="utf-8")

    summary = build_summary(
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
        master_path=master_path,
        search_source=query_source,
        text_columns=text_columns,
        documents_total=len(documents),
        term_sets_total=len(doc_term_sets),
        nodes_df=nodes_df,
        edges_df=edges_df,
        candidates=candidates,
        suggestions_output=suggestions_output,
    )
    summary_output.write_text(summary, encoding="utf-8")

    print(f"Wrote: {nodes_output}")
    print(f"Wrote: {edges_output}")
    print(f"Wrote: {candidates_output}")
    print(f"Wrote: {suggestions_output}")
    print(f"Wrote: {summary_output}")
    print(f"Documents processed: {len(documents)}")
    print(f"Candidate terms: {len(candidates)}")
    if not NLTK_AVAILABLE:
        print("Note: nltk is not installed in this environment; regex fallback tokenization was used.")


if __name__ == "__main__":
    main()