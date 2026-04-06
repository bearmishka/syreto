import argparse
import re
from collections import Counter
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
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
    "background",
    "objective",
    "conclusion",
    "conclusions",
}

EMPTY_VALUES = {"", "nan", "none"}


def clean_text(value: object) -> str:
    text = str(value if value is not None else "")
    text = re.sub(r"\s+", " ", text).strip()
    return "" if text.lower() in EMPTY_VALUES else text


def is_duplicate_row(row: pd.Series) -> bool:
    value = clean_text(row.get("is_duplicate", "")).lower()
    return value in {"yes", "y", "1", "true"}


def resolve_text_columns(df: pd.DataFrame, requested: str) -> tuple[list[str], str]:
    if requested.strip().lower() != "auto":
        columns = [part.strip() for part in requested.split(",") if part.strip()]
        available = [column for column in columns if column in df.columns]
        missing = [column for column in columns if column not in df.columns]

        if available and missing:
            note = f"Requested columns partially available; missing: {', '.join(missing)}."
        elif not available and columns:
            note = f"Requested columns not found: {', '.join(columns)}."
        else:
            note = "Using explicitly requested text columns."

        return available, note

    if "abstract" in df.columns:
        return ["abstract"], "Auto mode: using abstract-only corpus (revtools-style)."

    fallback = [column for column in ["title", "keywords", "notes"] if column in df.columns]
    if fallback:
        return fallback, "Auto mode fallback: abstract column missing, using available title/keywords/notes fields."

    return [], "Auto mode failed: no abstract/title/keywords/notes columns found."


class TextProcessor:
    def __init__(self, min_token_length: int) -> None:
        self.min_token_length = max(2, min_token_length)
        self.stopwords = BASE_STOPWORDS_EN | DOMAIN_STOPWORDS
        self.token_pattern = re.compile(r"[A-Za-z][A-Za-z\-]{1,}")
        self.tokenizer = RegexpTokenizer(r"[A-Za-z][A-Za-z\-]{1,}") if NLTK_AVAILABLE else None
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
            if len(normalized) < self.min_token_length:
                continue
            if normalized in self.stopwords:
                continue
            if self.stemmer is not None and re.fullmatch(r"[a-z]+", normalized):
                normalized = self.stemmer.stem(normalized)
            if normalized in self.stopwords:
                continue
            normalized_tokens.append(normalized)
        return normalized_tokens


def build_documents(
    df: pd.DataFrame,
    *,
    text_columns: list[str],
    processor: TextProcessor,
    include_duplicates: bool,
    max_documents: int,
    max_tokens_per_doc: int,
    min_tokens_per_doc: int,
) -> tuple[list[dict[str, object]], int]:
    documents: list[dict[str, object]] = []
    rows_seen = 0

    for row_index, row in df.iterrows():
        rows_seen += 1
        if not include_duplicates and is_duplicate_row(row):
            continue

        parts = [clean_text(row.get(column, "")) for column in text_columns]
        parts = [part for part in parts if part]
        if not parts:
            continue

        combined_text = " ".join(parts)
        tokens = processor.tokenize(combined_text)
        if max_tokens_per_doc > 0 and len(tokens) > max_tokens_per_doc:
            tokens = tokens[:max_tokens_per_doc]
        if len(tokens) < min_tokens_per_doc:
            continue

        documents.append(
            {
                "source_row": int(row_index) + 2,
                "record_id": clean_text(row.get("record_id", "")),
                "source_database": clean_text(row.get("source_database", "")),
                "year": clean_text(row.get("year", "")),
                "title": clean_text(row.get("title", "")),
                "tokens": tokens,
            }
        )

        if max_documents > 0 and len(documents) >= max_documents:
            break

    return documents, rows_seen


def build_vocabulary(
    documents: list[dict[str, object]],
    *,
    min_doc_freq: int,
    max_doc_freq_ratio: float,
    max_vocab_size: int,
) -> dict[str, int]:
    if not documents:
        return {}

    doc_freq: Counter[str] = Counter()
    for document in documents:
        tokens = document["tokens"]
        unique_tokens = set(str(token) for token in tokens)
        doc_freq.update(unique_tokens)

    max_doc_freq = int(max(1, np.floor(len(documents) * max_doc_freq_ratio)))
    filtered_terms = [
        term
        for term, freq in doc_freq.items()
        if freq >= min_doc_freq and freq <= max_doc_freq
    ]

    filtered_terms.sort(key=lambda term: (-doc_freq[term], term))
    if max_vocab_size > 0:
        filtered_terms = filtered_terms[:max_vocab_size]

    return {term: idx for idx, term in enumerate(filtered_terms)}


def build_word_id_docs(
    documents: list[dict[str, object]],
    term_to_id: dict[str, int],
) -> tuple[list[dict[str, object]], list[list[int]]]:
    if not documents or not term_to_id:
        return [], []

    kept_documents: list[dict[str, object]] = []
    docs_word_ids: list[list[int]] = []

    for document in documents:
        word_ids = [term_to_id[token] for token in document["tokens"] if token in term_to_id]
        if not word_ids:
            continue
        kept_documents.append(document)
        docs_word_ids.append(word_ids)

    return kept_documents, docs_word_ids


def fit_lda_gibbs(
    docs_word_ids: list[list[int]],
    *,
    n_topics: int,
    vocab_size: int,
    iterations: int,
    alpha: float,
    beta: float,
    seed: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    n_docs = len(docs_word_ids)
    rng = np.random.default_rng(seed)

    doc_topic_counts = np.zeros((n_docs, n_topics), dtype=np.int64)
    topic_word_counts = np.zeros((n_topics, vocab_size), dtype=np.int64)
    topic_totals = np.zeros(n_topics, dtype=np.int64)

    assignments: list[list[int]] = []

    for doc_index, word_ids in enumerate(docs_word_ids):
        topics = rng.integers(low=0, high=n_topics, size=len(word_ids), endpoint=False)
        topic_list = topics.tolist()
        assignments.append(topic_list)

        for word_id, topic_id in zip(word_ids, topic_list):
            doc_topic_counts[doc_index, topic_id] += 1
            topic_word_counts[topic_id, word_id] += 1
            topic_totals[topic_id] += 1

    topic_range = np.arange(n_topics)

    for _ in range(iterations):
        for doc_index, word_ids in enumerate(docs_word_ids):
            doc_topics = assignments[doc_index]
            for token_index, word_id in enumerate(word_ids):
                current_topic = doc_topics[token_index]

                doc_topic_counts[doc_index, current_topic] -= 1
                topic_word_counts[current_topic, word_id] -= 1
                topic_totals[current_topic] -= 1

                left = (topic_word_counts[:, word_id] + beta) / (topic_totals + (vocab_size * beta))
                probs = left * (doc_topic_counts[doc_index, :] + alpha)
                probs_sum = float(probs.sum())

                if probs_sum <= 0:
                    probs = np.full(n_topics, 1.0 / n_topics, dtype=float)
                else:
                    probs = probs / probs_sum

                sampled_topic = int(rng.choice(topic_range, p=probs))
                doc_topics[token_index] = sampled_topic

                doc_topic_counts[doc_index, sampled_topic] += 1
                topic_word_counts[sampled_topic, word_id] += 1
                topic_totals[sampled_topic] += 1

    return doc_topic_counts, topic_word_counts, topic_totals


def compute_theta_phi(
    doc_topic_counts: np.ndarray,
    topic_word_counts: np.ndarray,
    topic_totals: np.ndarray,
    *,
    alpha: float,
    beta: float,
) -> tuple[np.ndarray, np.ndarray]:
    n_docs, n_topics = doc_topic_counts.shape
    vocab_size = topic_word_counts.shape[1]

    doc_lengths = doc_topic_counts.sum(axis=1, keepdims=True)
    theta = (doc_topic_counts + alpha) / (doc_lengths + (n_topics * alpha))

    phi = (topic_word_counts + beta) / (topic_totals[:, None] + (vocab_size * beta))
    return theta, phi


def topic_top_terms_df(
    phi: np.ndarray,
    *,
    id_to_term: dict[int, str],
    top_terms: int,
    theta: np.ndarray,
) -> pd.DataFrame:
    if phi.size == 0:
        return pd.DataFrame(columns=["topic_id", "rank", "term", "term_probability", "topic_prevalence"])

    rows: list[dict[str, object]] = []
    prevalence = theta.mean(axis=0) if theta.size > 0 else np.zeros(phi.shape[0], dtype=float)

    for topic_id in range(phi.shape[0]):
        ranked_ids = np.argsort(phi[topic_id])[::-1][:top_terms]
        for rank, term_id in enumerate(ranked_ids, start=1):
            rows.append(
                {
                    "topic_id": topic_id,
                    "rank": rank,
                    "term": id_to_term[int(term_id)],
                    "term_probability": float(phi[topic_id, term_id]),
                    "topic_prevalence": float(prevalence[topic_id]),
                }
            )

    return pd.DataFrame(rows)


def pca_2d(matrix: np.ndarray) -> np.ndarray:
    if matrix.size == 0:
        return np.zeros((0, 2), dtype=float)

    if matrix.shape[0] == 1:
        return np.zeros((1, 2), dtype=float)

    centered = matrix - matrix.mean(axis=0, keepdims=True)
    u, s, _ = np.linalg.svd(centered, full_matrices=False)

    coords = np.zeros((matrix.shape[0], 2), dtype=float)
    if s.size >= 1:
        coords[:, 0] = u[:, 0] * s[0]
    if s.size >= 2:
        coords[:, 1] = u[:, 1] * s[1]
    return coords


def build_doc_topics_df(
    documents: list[dict[str, object]],
    *,
    docs_word_ids: list[list[int]],
    theta: np.ndarray,
    coords: np.ndarray,
) -> pd.DataFrame:
    if not documents:
        return pd.DataFrame(
            columns=[
                "source_row",
                "record_id",
                "source_database",
                "year",
                "title",
                "token_count",
                "dominant_topic",
                "dominant_topic_prob",
                "cluster_x",
                "cluster_y",
            ]
        )

    rows: list[dict[str, object]] = []

    for doc_index, document in enumerate(documents):
        topic_vector = theta[doc_index]
        dominant_topic = int(np.argmax(topic_vector))
        base_row: dict[str, object] = {
            "source_row": int(document["source_row"]),
            "record_id": str(document["record_id"]),
            "source_database": str(document["source_database"]),
            "year": str(document["year"]),
            "title": str(document["title"]),
            "token_count": int(len(docs_word_ids[doc_index])),
            "dominant_topic": dominant_topic,
            "dominant_topic_prob": float(topic_vector[dominant_topic]),
            "cluster_x": float(coords[doc_index, 0]),
            "cluster_y": float(coords[doc_index, 1]),
        }

        for topic_id, score in enumerate(topic_vector):
            base_row[f"topic_{topic_id:02d}"] = float(score)

        rows.append(base_row)

    return pd.DataFrame(rows)


def topic_labels(top_terms_df: pd.DataFrame, label_terms: int = 3) -> dict[int, str]:
    labels: dict[int, str] = {}
    if top_terms_df.empty:
        return labels

    for topic_id, group in top_terms_df.groupby("topic_id"):
        terms = group.sort_values("rank")["term"].head(label_terms).tolist()
        labels[int(topic_id)] = ", ".join(str(term) for term in terms)

    return labels


def render_cluster_plot(
    doc_topics_df: pd.DataFrame,
    *,
    top_terms_df: pd.DataFrame,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(10.5, 7.2))

    if doc_topics_df.empty:
        plt.text(0.5, 0.5, "No documents available for topic clustering.", ha="center", va="center", fontsize=12)
        plt.axis("off")
        plt.tight_layout()
        plt.savefig(output_path, dpi=180)
        plt.close()
        return

    x = doc_topics_df["cluster_x"].to_numpy(dtype=float)
    y = doc_topics_df["cluster_y"].to_numpy(dtype=float)
    topics = doc_topics_df["dominant_topic"].to_numpy(dtype=int)

    unique_topics = sorted(set(int(topic) for topic in topics))
    cmap = plt.get_cmap("tab20")
    colors = [cmap(topic % 20) for topic in unique_topics]
    color_map = {topic: color for topic, color in zip(unique_topics, colors)}

    for topic in unique_topics:
        mask = topics == topic
        plt.scatter(x[mask], y[mask], s=26, alpha=0.82, color=color_map[topic], label=f"Topic {topic}")

    labels = topic_labels(top_terms_df)
    legend_labels = []
    for topic in unique_topics:
        if topic in labels and labels[topic]:
            legend_labels.append(f"T{topic}: {labels[topic]}")
        else:
            legend_labels.append(f"T{topic}")

    handles, _ = plt.gca().get_legend_handles_labels()
    plt.legend(handles, legend_labels, loc="upper left", bbox_to_anchor=(1.02, 1.00), fontsize=8, frameon=True)

    plt.title("LDA Topic Clusters (documents by dominant topic)")
    plt.xlabel("PC1 (document-topic space)")
    plt.ylabel("PC2 (document-topic space)")
    plt.grid(alpha=0.18)
    plt.tight_layout()
    plt.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close()


def build_summary(
    *,
    master_path: Path,
    generated_at: str,
    text_columns: list[str],
    column_selection_note: str,
    source_rows_seen: int,
    documents_kept: int,
    vocabulary_size: int,
    total_tokens: int,
    topics: int,
    iterations: int,
    alpha: float,
    beta: float,
    min_doc_freq: int,
    max_doc_freq_ratio: float,
    max_vocab_size: int,
    max_documents: int,
    max_tokens_per_doc: int,
    outputs: dict[str, Path],
) -> str:
    lines: list[str] = []
    lines.append("# Topic Model Visualization Summary")
    lines.append("")
    lines.append(f"Generated: {generated_at}")
    lines.append("")
    lines.append("## Inputs")
    lines.append("")
    lines.append(f"- Master records: `{master_path.as_posix()}`")
    lines.append(f"- Text columns used: {', '.join(text_columns) if text_columns else 'none'}")
    lines.append(f"- Column selection mode: {column_selection_note}")
    lines.append(f"- NLP backend: {'nltk + regex tokenizer' if NLTK_AVAILABLE else 'regex fallback tokenizer'}")
    lines.append("")
    lines.append("## LDA Settings")
    lines.append("")
    lines.append(f"- Topics: {topics}")
    lines.append(f"- Iterations: {iterations}")
    lines.append(f"- alpha: {alpha}")
    lines.append(f"- beta: {beta}")
    lines.append(f"- min_doc_freq: {min_doc_freq}")
    lines.append(f"- max_doc_freq_ratio: {max_doc_freq_ratio}")
    lines.append(f"- max_vocab_size: {max_vocab_size}")
    lines.append(f"- max_documents: {max_documents}")
    lines.append(f"- max_tokens_per_doc: {max_tokens_per_doc}")
    lines.append("")
    lines.append("## Corpus Stats")
    lines.append("")
    lines.append(f"- Source rows seen: {source_rows_seen}")
    lines.append(f"- Documents retained: {documents_kept}")
    lines.append(f"- Vocabulary size: {vocabulary_size}")
    lines.append(f"- Total tokens modeled: {total_tokens}")
    lines.append("")
    lines.append("## Outputs")
    lines.append("")
    lines.append(f"- Document-topic assignments: `{outputs['doc_topics'].as_posix()}`")
    lines.append(f"- Topic top terms: `{outputs['topic_terms'].as_posix()}`")
    lines.append(f"- Cluster visualization: `{outputs['plot'].as_posix()}`")
    lines.append(f"- Summary: `{outputs['summary'].as_posix()}`")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- Use this map for rapid corpus orientation before/around title-abstract screening.")
    lines.append("- Topic labels are heuristic (top terms); always manually inspect representative records.")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build an LDA topic model on abstract text from master_records and visualize document clusters."
    )
    parser.add_argument("--master", default="../02_data/processed/master_records.csv", help="Path to master records CSV")
    parser.add_argument(
        "--text-columns",
        default="auto",
        help="Comma-separated text columns or 'auto' (auto = abstract-only if present; else fallback to title/keywords/notes)",
    )
    parser.add_argument("--include-duplicates", action="store_true", help="Include duplicate rows from master records")
    parser.add_argument("--min-token-length", type=int, default=3, help="Minimum token length")
    parser.add_argument("--min-tokens-per-doc", type=int, default=15, help="Minimum tokens required per document")
    parser.add_argument("--max-tokens-per-doc", type=int, default=300, help="Maximum tokens retained per document")
    parser.add_argument("--max-documents", type=int, default=3000, help="Maximum documents used in LDA (0 = no cap)")
    parser.add_argument("--min-doc-freq", type=int, default=3, help="Minimum document frequency for vocabulary terms")
    parser.add_argument("--max-doc-freq-ratio", type=float, default=0.85, help="Maximum document-frequency ratio for terms")
    parser.add_argument("--max-vocab-size", type=int, default=4000, help="Maximum vocabulary size after DF filtering")
    parser.add_argument("--topics", type=int, default=8, help="Number of LDA topics")
    parser.add_argument("--iterations", type=int, default=250, help="Gibbs sampling iterations")
    parser.add_argument("--alpha", type=float, default=0.1, help="Dirichlet alpha for document-topic distribution")
    parser.add_argument("--beta", type=float, default=0.01, help="Dirichlet beta for topic-word distribution")
    parser.add_argument("--top-terms", type=int, default=12, help="Number of top terms per topic")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--doc-topics-output", default="outputs/topic_model_doc_topics.csv", help="Output CSV for doc-topic assignments")
    parser.add_argument("--topic-terms-output", default="outputs/topic_model_top_terms.csv", help="Output CSV for top terms by topic")
    parser.add_argument("--plot-output", default="outputs/topic_model_clusters.png", help="Output PNG for topic cluster visualization")
    parser.add_argument("--summary", default="outputs/topic_model_summary.md", help="Output markdown summary")
    args = parser.parse_args()

    if args.topics < 2:
        raise ValueError("--topics must be >= 2")
    if args.iterations < 1:
        raise ValueError("--iterations must be >= 1")
    if args.alpha <= 0:
        raise ValueError("--alpha must be > 0")
    if args.beta <= 0:
        raise ValueError("--beta must be > 0")
    if args.min_doc_freq < 1:
        raise ValueError("--min-doc-freq must be >= 1")
    if args.max_doc_freq_ratio <= 0 or args.max_doc_freq_ratio > 1:
        raise ValueError("--max-doc-freq-ratio must be in (0, 1]")

    master_path = Path(args.master)
    doc_topics_output = Path(args.doc_topics_output)
    topic_terms_output = Path(args.topic_terms_output)
    plot_output = Path(args.plot_output)
    summary_output = Path(args.summary)

    if not master_path.exists():
        raise FileNotFoundError(f"Master records CSV not found: {master_path}")

    master_df = pd.read_csv(master_path)
    text_columns, column_selection_note = resolve_text_columns(master_df, args.text_columns)
    if not text_columns:
        raise ValueError(
            "No usable text columns resolved. Provide --text-columns explicitly or ensure master_records has abstract/title/keywords/notes."
        )
    processor = TextProcessor(min_token_length=args.min_token_length)

    documents, source_rows_seen = build_documents(
        master_df,
        text_columns=text_columns,
        processor=processor,
        include_duplicates=args.include_duplicates,
        max_documents=args.max_documents,
        max_tokens_per_doc=args.max_tokens_per_doc,
        min_tokens_per_doc=args.min_tokens_per_doc,
    )

    term_to_id = build_vocabulary(
        documents,
        min_doc_freq=args.min_doc_freq,
        max_doc_freq_ratio=args.max_doc_freq_ratio,
        max_vocab_size=args.max_vocab_size,
    )
    id_to_term = {idx: term for term, idx in term_to_id.items()}

    kept_documents, docs_word_ids = build_word_id_docs(documents, term_to_id)
    total_tokens = int(sum(len(doc) for doc in docs_word_ids))

    if kept_documents:
        doc_topic_counts, topic_word_counts, topic_totals = fit_lda_gibbs(
            docs_word_ids,
            n_topics=args.topics,
            vocab_size=len(term_to_id),
            iterations=args.iterations,
            alpha=args.alpha,
            beta=args.beta,
            seed=args.seed,
        )
        theta, phi = compute_theta_phi(
            doc_topic_counts,
            topic_word_counts,
            topic_totals,
            alpha=args.alpha,
            beta=args.beta,
        )
        coords = pca_2d(theta)
        topic_terms_df = topic_top_terms_df(
            phi,
            id_to_term=id_to_term,
            top_terms=args.top_terms,
            theta=theta,
        )
        doc_topics_df = build_doc_topics_df(
            kept_documents,
            docs_word_ids=docs_word_ids,
            theta=theta,
            coords=coords,
        )
    else:
        theta = np.zeros((0, args.topics), dtype=float)
        topic_terms_df = pd.DataFrame(columns=["topic_id", "rank", "term", "term_probability", "topic_prevalence"])
        doc_topics_df = pd.DataFrame(
            columns=[
                "source_row",
                "record_id",
                "source_database",
                "year",
                "title",
                "token_count",
                "dominant_topic",
                "dominant_topic_prob",
                "cluster_x",
                "cluster_y",
            ]
        )

    doc_topics_output.parent.mkdir(parents=True, exist_ok=True)
    topic_terms_output.parent.mkdir(parents=True, exist_ok=True)
    summary_output.parent.mkdir(parents=True, exist_ok=True)

    doc_topics_df.to_csv(doc_topics_output, index=False)
    topic_terms_df.to_csv(topic_terms_output, index=False)
    render_cluster_plot(doc_topics_df, top_terms_df=topic_terms_df, output_path=plot_output)

    summary_text = build_summary(
        master_path=master_path,
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
        text_columns=text_columns,
        column_selection_note=column_selection_note,
        source_rows_seen=source_rows_seen,
        documents_kept=len(kept_documents),
        vocabulary_size=len(term_to_id),
        total_tokens=total_tokens,
        topics=args.topics,
        iterations=args.iterations,
        alpha=args.alpha,
        beta=args.beta,
        min_doc_freq=args.min_doc_freq,
        max_doc_freq_ratio=args.max_doc_freq_ratio,
        max_vocab_size=args.max_vocab_size,
        max_documents=args.max_documents,
        max_tokens_per_doc=args.max_tokens_per_doc,
        outputs={
            "doc_topics": doc_topics_output,
            "topic_terms": topic_terms_output,
            "plot": plot_output,
            "summary": summary_output,
        },
    )
    summary_output.write_text(summary_text, encoding="utf-8")

    print(f"Wrote: {doc_topics_output}")
    print(f"Wrote: {topic_terms_output}")
    print(f"Wrote: {plot_output}")
    print(f"Wrote: {summary_output}")
    print(f"Documents modeled: {len(kept_documents)}")
    print(f"Vocabulary size: {len(term_to_id)}")
    print(f"Column mode: {column_selection_note}")
    if not NLTK_AVAILABLE:
        print("Note: nltk is not installed in this environment; regex fallback tokenization was used.")


if __name__ == "__main__":
    main()