import argparse
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

EMPTY_VALUES = {"", "nan", "none"}
DECISION_NORMALIZATION = {
    "include": "include",
    "included": "include",
    "incl": "include",
    "in": "include",
    "yes": "include",
    "exclude": "exclude",
    "excluded": "exclude",
    "excl": "exclude",
    "ex": "exclude",
    "no": "exclude",
    "maybe": "uncertain",
    "unclear": "uncertain",
    "pending": "uncertain",
    "undecided": "uncertain",
    "uncertain": "uncertain",
}
FLOW_STAGES = [
    "records_identified_databases",
    "records_identified_other_sources",
    "duplicates_removed",
    "records_screened_title_abstract",
    "records_excluded_title_abstract",
    "reports_assessed_full_text",
    "reports_excluded_full_text",
    "studies_included_qualitative_synthesis",
    "studies_included_quantitative_synthesis",
]
CORE_PRISMA_SYNC_KEYS = [
    "records_identified_databases",
    "duplicates_removed",
    "records_screened_title_abstract",
    "records_excluded_title_abstract",
    "reports_sought_for_retrieval",
]
FULLTEXT_PRISMA_SYNC_KEYS = [
    "reports_not_retrieved",
    "reports_assessed_full_text",
    "reports_excluded_full_text",
    "studies_included_qualitative_synthesis",
]


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


def atomic_write_dataframe_csv(frame: pd.DataFrame, path: Path, *, index: bool = False) -> None:
    csv_text = frame.to_csv(index=index)
    atomic_write_text(path, csv_text)


@dataclass(frozen=True)
class FlowBox:
    key: str
    title: str
    count: int | None
    x: float
    y: float
    w: float
    h: float
    fill: str


@dataclass(frozen=True)
class FlowArrow:
    start_box: str
    start_anchor: str
    end_box: str
    end_anchor: str


@dataclass(frozen=True)
class FlowLayout:
    title: str
    source_note: str
    separator_x: float
    section_labels: tuple[tuple[str, float, float], ...]
    boxes: tuple[FlowBox, ...]
    arrows: tuple[FlowArrow, ...]


def is_non_empty_row(row: pd.Series) -> bool:
    for value in row:
        text = str(value).strip().lower()
        if text not in EMPTY_VALUES:
            return True
    return False


def to_yes(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.lower().isin({"yes", "y", "1", "true"})


def normalize_fulltext_available(value: object) -> str:
    normalized = str(value).strip().lower()
    if normalized in {"yes", "y", "1", "true"}:
        return "yes"
    if normalized in {"no", "n", "0", "false"}:
        return "no"
    return ""


def normalize_fulltext_include(value: object) -> str:
    normalized = str(value).strip().lower()
    if normalized in {"include", "included", "yes", "y", "1", "true"}:
        return "include"
    if normalized in {"exclude", "excluded", "no", "n", "0", "false"}:
        return "exclude"
    return ""


def normalize_screening_decision(value: object) -> str:
    normalized = str(value).strip().lower()
    return DECISION_NORMALIZATION.get(normalized, "")


def parse_count(value: object) -> int | None:
    text = str(value).strip()
    if text.lower() in EMPTY_VALUES:
        return None
    numeric = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return None
    return int(float(numeric))


def count_or_dash(value: int | None) -> str:
    return str(value) if value is not None else "—"


def prisma_counts(prisma_df: pd.DataFrame) -> dict[str, int | None]:
    counts = {stage: None for stage in FLOW_STAGES}
    if prisma_df.empty or "stage" not in prisma_df.columns or "count" not in prisma_df.columns:
        return counts

    stage_series = prisma_df["stage"].fillna("").astype(str).str.strip()
    for stage in FLOW_STAGES:
        mask = stage_series.eq(stage)
        if mask.any():
            counts[stage] = parse_count(prisma_df.loc[mask, "count"].iloc[0])

    return counts


def build_flow_layout(counts: dict[str, int | None]) -> FlowLayout:
    main_x, main_w = 0.07, 0.48
    side_x, side_w = 0.66, 0.28
    box_h = 0.085

    main_fill = "#dcecff"
    main_fill_soft = "#e9f3ff"
    exclusion_fill = "#f3f4f6"

    left_column_x = main_x + main_w / 2
    right_column_x = side_x + side_w / 2
    separator_x = (main_x + main_w + side_x) / 2

    boxes = (
        FlowBox(
            key="identified_databases",
            title="Records identified from databases",
            count=counts["records_identified_databases"],
            x=main_x,
            y=0.8375,
            w=main_w,
            h=box_h,
            fill=main_fill,
        ),
        FlowBox(
            key="identified_other_sources",
            title="Records identified from other sources",
            count=counts["records_identified_other_sources"],
            x=side_x,
            y=0.8375,
            w=side_w,
            h=box_h,
            fill=main_fill_soft,
        ),
        FlowBox(
            key="duplicates_removed",
            title="Duplicates removed",
            count=counts["duplicates_removed"],
            x=side_x,
            y=0.7375,
            w=side_w,
            h=box_h,
            fill=exclusion_fill,
        ),
        FlowBox(
            key="screened",
            title="Records screened (title/abstract)",
            count=counts["records_screened_title_abstract"],
            x=main_x,
            y=0.6375,
            w=main_w,
            h=box_h,
            fill=main_fill,
        ),
        FlowBox(
            key="excluded_title_abstract",
            title="Records excluded\n(title/abstract)",
            count=counts["records_excluded_title_abstract"],
            x=side_x,
            y=0.6375,
            w=side_w,
            h=box_h,
            fill=exclusion_fill,
        ),
        FlowBox(
            key="full_text_assessed",
            title="Reports assessed for eligibility\n(full text)",
            count=counts["reports_assessed_full_text"],
            x=main_x,
            y=0.4375,
            w=main_w,
            h=box_h,
            fill=main_fill,
        ),
        FlowBox(
            key="excluded_full_text",
            title="Reports excluded\n(full text)",
            count=counts["reports_excluded_full_text"],
            x=side_x,
            y=0.4375,
            w=side_w,
            h=box_h,
            fill=exclusion_fill,
        ),
        FlowBox(
            key="included_qualitative",
            title="Studies included in\nqualitative synthesis",
            count=counts["studies_included_qualitative_synthesis"],
            x=main_x,
            y=0.2375,
            w=main_w,
            h=box_h,
            fill=main_fill,
        ),
        FlowBox(
            key="included_quantitative",
            title="Studies included in\nquantitative synthesis",
            count=counts["studies_included_quantitative_synthesis"],
            x=main_x,
            y=0.0675,
            w=main_w,
            h=box_h,
            fill=main_fill_soft,
        ),
    )

    arrows = (
        FlowArrow("identified_databases", "bottom", "screened", "top"),
        FlowArrow("screened", "bottom", "full_text_assessed", "top"),
        FlowArrow("full_text_assessed", "bottom", "included_qualitative", "top"),
        FlowArrow("included_qualitative", "bottom", "included_quantitative", "top"),
        FlowArrow("identified_other_sources", "left", "identified_databases", "right"),
        FlowArrow("duplicates_removed", "left", "screened", "right"),
        FlowArrow("excluded_title_abstract", "left", "screened", "right"),
        FlowArrow("excluded_full_text", "left", "full_text_assessed", "right"),
    )

    return FlowLayout(
        title="PRISMA 2020 Flow Diagram (Auto-generated)",
        source_note="Source: 02_data/processed/prisma_counts_template.csv",
        separator_x=separator_x,
        section_labels=(
            ("Main Flow", left_column_x, 0.942),
            ("Exclusions / Side Outputs", right_column_x, 0.942),
        ),
        boxes=boxes,
        arrows=arrows,
    )


def box_center_x(box: FlowBox) -> float:
    return float(box.x + box.w / 2)


def box_center_y(box: FlowBox) -> float:
    return float(box.y + box.h / 2)


def anchor_point(box: FlowBox, anchor: str) -> tuple[float, float]:
    if anchor == "top":
        return box_center_x(box), float(box.y + box.h)
    if anchor == "bottom":
        return box_center_x(box), float(box.y)
    if anchor == "left":
        return float(box.x), box_center_y(box)
    if anchor == "right":
        return float(box.x + box.w), box_center_y(box)
    raise ValueError(f"Unsupported anchor: {anchor}")


def arrow(
    ax: plt.Axes,
    start: tuple[float, float],
    end: tuple[float, float],
    *,
    shrink_a: float = 12,
    shrink_b: float = 12,
) -> None:
    patch = FancyArrowPatch(
        start,
        end,
        arrowstyle="-|>",
        mutation_scale=11,
        linewidth=1.4,
        color="#2d3748",
        shrinkA=shrink_a,
        shrinkB=shrink_b,
        connectionstyle="arc3,rad=0",
    )
    ax.add_patch(patch)


def draw_box(ax: plt.Axes, box: FlowBox, fontsize: float = 8.8) -> None:
    patch = FancyBboxPatch(
        (box.x, box.y),
        box.w,
        box.h,
        boxstyle="round,pad=0.015,rounding_size=0.013",
        linewidth=1.3,
        edgecolor="#2d3748",
        facecolor=box.fill,
    )
    ax.add_patch(patch)
    ax.text(
        box_center_x(box),
        box_center_y(box),
        f"{box.title}\n(n = {count_or_dash(box.count)})",
        ha="center",
        va="center",
        fontsize=fontsize,
        linespacing=1.15,
    )


def render_prisma_flow_matplotlib(layout: FlowLayout, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(12.8, 12.2))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    ax.text(0.5, 0.975, layout.title, ha="center", va="top", fontsize=14, fontweight="bold")

    ax.plot(
        [layout.separator_x, layout.separator_x],
        [0.06, 0.93],
        color="#cbd5e1",
        linewidth=1.1,
        linestyle=(0, (4, 4)),
        zorder=0,
    )

    for label_text, x, y in layout.section_labels:
        ax.text(x, y, label_text, ha="center", va="center", fontsize=10, color="#334155")

    for box in layout.boxes:
        draw_box(ax, box)

    box_map = {box.key: box for box in layout.boxes}
    for arrow_spec in layout.arrows:
        start = anchor_point(box_map[arrow_spec.start_box], arrow_spec.start_anchor)
        end = anchor_point(box_map[arrow_spec.end_box], arrow_spec.end_anchor)
        arrow(ax, start, end)

    ax.text(0.5, 0.03, layout.source_note, ha="center", va="center", fontsize=8.6, color="#4a5568")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)


def tex_escape(text: str) -> str:
    escaped = text
    for source, target in (
        ("\\", r"\textbackslash{}"),
        ("&", r"\&"),
        ("%", r"\%"),
        ("$", r"\$"),
        ("#", r"\#"),
        ("_", r"\_"),
        ("{", r"\{"),
        ("}", r"\}"),
        ("~", r"\textasciitilde{}"),
        ("^", r"\textasciicircum{}"),
    ):
        escaped = escaped.replace(source, target)
    return escaped


def tikz_multiline(text: str) -> str:
    return r" \\ ".join(tex_escape(part) for part in text.split("\n"))


def tikz_style_config(flow_style: str) -> dict[str, object]:
    styles: dict[str, dict[str, object]] = {
        "default": {
            "edge": "2D3748",
            "divider": "CBD5E1",
            "label": "334155",
            "source": "4A5568",
            "main": "EAF3FF",
            "main_soft": "F3F8FF",
            "exclusion": "F5F6F8",
            "line_width": "0.9pt",
            "rounded": "2.6pt",
            "main_text_width": "8.6cm",
            "side_text_width": "5.6cm",
            "box_height": "1.4cm",
            "inner_sep": "4pt",
            "title_xy": (4.4, 0.9),
            "main_label_xy": (0.0, 0.25),
            "side_label_xy": (8.8, 0.25),
            "main_label_text": "Main flow",
            "side_label_text": "Side outputs / exclusions",
            "divider_x": 4.8,
            "divider_y_top": 0.0,
            "divider_y_bottom": -10.85,
            "source_xy": (4.4, -11.2),
            "source_font": r"\fontsize{8.5}{10.0}\selectfont",
            "side_bend_dx": -0.8,
            "positions": {
                "identified": (0.0, 0.0),
                "screened": (0.0, -2.1),
                "fulltext": (0.0, -4.9),
                "qualitative": (0.0, -7.7),
                "quantitative": (0.0, -9.8),
                "other_sources": (6.0, 0.0),
                "duplicates": (6.0, -2.1),
                "excluded_ta": (6.0, -4.2),
                "excluded_ft": (6.0, -6.9),
            },
        },
        "journal": {
            "edge": "1F2937",
            "divider": "D1D5DB",
            "label": "374151",
            "source": "6B7280",
            "main": "FFFFFF",
            "main_soft": "FAFAFA",
            "exclusion": "F4F4F5",
            "line_width": "0.8pt",
            "rounded": "1.8pt",
            "main_text_width": "8.2cm",
            "side_text_width": "5.2cm",
            "box_height": "1.22cm",
            "inner_sep": "3.5pt",
            "title_xy": (4.3, 0.75),
            "main_label_xy": (0.0, 0.18),
            "side_label_xy": (8.5, 0.18),
            "main_label_text": "Flow",
            "side_label_text": "Exclusions",
            "divider_x": 4.65,
            "divider_y_top": 0.0,
            "divider_y_bottom": -10.05,
            "source_xy": (4.3, -10.45),
            "source_font": r"\fontsize{8.2}{9.8}\selectfont",
            "side_bend_dx": -0.7,
            "positions": {
                "identified": (0.0, 0.0),
                "screened": (0.0, -1.85),
                "fulltext": (0.0, -4.35),
                "qualitative": (0.0, -6.85),
                "quantitative": (0.0, -8.8),
                "other_sources": (5.8, 0.0),
                "duplicates": (5.8, -1.85),
                "excluded_ta": (5.8, -3.7),
                "excluded_ft": (5.8, -6.1),
            },
        },
    }

    if flow_style not in styles:
        raise ValueError(f"Unsupported flow style: {flow_style}")
    return styles[flow_style]


def render_prisma_flow_tikz(
    layout: FlowLayout, output_path: Path, *, flow_style: str = "default"
) -> None:
    style = tikz_style_config(flow_style)
    positions = style["positions"]

    count_by_key = {box.key: box.count for box in layout.boxes}

    def node_text(title: str, count: int | None) -> str:
        title_text = tikz_multiline(title)
        return f"{title_text} \\\\ \\textbf{{n = {count_or_dash(count)}}}"

    fulltext_label = "Reports assessed for eligibility\n(full text)"
    qualitative_label = "Studies included in\nqualitative synthesis"
    quantitative_label = "Studies included in\nquantitative synthesis"
    excluded_ta_label = "Records excluded\n(title/abstract)"
    excluded_ft_label = "Reports excluded\n(full text)"

    lines: list[str] = [
        "% Auto-generated by 03_analysis/dedup_stats.py",
        "% Keep edits in the Python source, not in this generated file.",
        "% This file is intended to be included directly from LaTeX via \\input{...}.",
        r"\begin{tikzpicture}[x=1cm,y=1cm]",
        f"\\definecolor{{flowEdge}}{{HTML}}{{{style['edge']}}}",
        f"\\definecolor{{flowDivider}}{{HTML}}{{{style['divider']}}}",
        f"\\definecolor{{flowLabel}}{{HTML}}{{{style['label']}}}",
        f"\\definecolor{{flowSource}}{{HTML}}{{{style['source']}}}",
        f"\\definecolor{{flowMain}}{{HTML}}{{{style['main']}}}",
        f"\\definecolor{{flowMainSoft}}{{HTML}}{{{style['main_soft']}}}",
        f"\\definecolor{{flowExclusion}}{{HTML}}{{{style['exclusion']}}}",
        r"\tikzset{",
        (
            f"  prismaMain/.style={{draw=flowEdge,fill=flowMain,rounded corners={style['rounded']},"
            f"line width={style['line_width']},align=center,text width={style['main_text_width']},"
            f"minimum height={style['box_height']},inner sep={style['inner_sep']}}},"
        ),
        r"  prismaMainSoft/.style={prismaMain,fill=flowMainSoft},",
        (
            f"  prismaSide/.style={{draw=flowEdge,fill=flowExclusion,rounded corners={style['rounded']},"
            f"line width={style['line_width']},align=center,text width={style['side_text_width']},"
            f"minimum height={style['box_height']},inner sep={style['inner_sep']}}},"
        ),
        f"  prismaArrow/.style={{->,draw=flowEdge,line width={style['line_width']}}},",
        r"  prismaDivider/.style={draw=flowDivider,line width=0.7pt,dash pattern=on 2pt off 2pt}",
        r"}",
        f"\\node[align=center,font=\\bfseries\\large] at ({style['title_xy'][0]:.2f},{style['title_xy'][1]:.2f}) {{{tex_escape(layout.title)}}};",
        f"\\node[font=\\bfseries\\small,text=flowLabel] at ({style['main_label_xy'][0]:.2f},{style['main_label_xy'][1]:.2f}) {{{tex_escape(str(style['main_label_text']))}}};",
        f"\\node[font=\\bfseries\\small,text=flowLabel] at ({style['side_label_xy'][0]:.2f},{style['side_label_xy'][1]:.2f}) {{{tex_escape(str(style['side_label_text']))}}};",
        f"\\draw[prismaDivider] ({style['divider_x']:.2f},{style['divider_y_top']:.2f}) -- ({style['divider_x']:.2f},{style['divider_y_bottom']:.2f});",
        (
            f"\\node[prismaMain,anchor=north] (identified) at ({positions['identified'][0]:.2f},{positions['identified'][1]:.2f}) "
            f"{{{node_text('Records identified from databases', count_by_key.get('identified_databases'))}}};"
        ),
        (
            f"\\node[prismaMain,anchor=north] (screened) at ({positions['screened'][0]:.2f},{positions['screened'][1]:.2f}) "
            f"{{{node_text('Records screened (title/abstract)', count_by_key.get('screened'))}}};"
        ),
        (
            f"\\node[prismaMain,anchor=north] (fulltext) at ({positions['fulltext'][0]:.2f},{positions['fulltext'][1]:.2f}) "
            f"{{{node_text(fulltext_label, count_by_key.get('full_text_assessed'))}}};"
        ),
        (
            f"\\node[prismaMain,anchor=north] (qualitative) at ({positions['qualitative'][0]:.2f},{positions['qualitative'][1]:.2f}) "
            f"{{{node_text(qualitative_label, count_by_key.get('included_qualitative'))}}};"
        ),
        (
            f"\\node[prismaMainSoft,anchor=north] (quantitative) at ({positions['quantitative'][0]:.2f},{positions['quantitative'][1]:.2f}) "
            f"{{{node_text(quantitative_label, count_by_key.get('included_quantitative'))}}};"
        ),
        (
            f"\\node[prismaSide,anchor=north west] (other_sources) at ({positions['other_sources'][0]:.2f},{positions['other_sources'][1]:.2f}) "
            f"{{{node_text('Records identified from other sources', count_by_key.get('identified_other_sources'))}}};"
        ),
        (
            f"\\node[prismaSide,anchor=north west] (duplicates) at ({positions['duplicates'][0]:.2f},{positions['duplicates'][1]:.2f}) "
            f"{{{node_text('Duplicates removed', count_by_key.get('duplicates_removed'))}}};"
        ),
        (
            f"\\node[prismaSide,anchor=north west] (excluded_ta) at ({positions['excluded_ta'][0]:.2f},{positions['excluded_ta'][1]:.2f}) "
            f"{{{node_text(excluded_ta_label, count_by_key.get('excluded_title_abstract'))}}};"
        ),
        (
            f"\\node[prismaSide,anchor=north west] (excluded_ft) at ({positions['excluded_ft'][0]:.2f},{positions['excluded_ft'][1]:.2f}) "
            f"{{{node_text(excluded_ft_label, count_by_key.get('excluded_full_text'))}}};"
        ),
        r"\draw[prismaArrow] (identified.south) -- (screened.north);",
        r"\draw[prismaArrow] (screened.south) -- (fulltext.north);",
        r"\draw[prismaArrow] (fulltext.south) -- (qualitative.north);",
        r"\draw[prismaArrow] (qualitative.south) -- (quantitative.north);",
        f"\\draw[prismaArrow] (other_sources.west) -- ++({style['side_bend_dx']},0) |- (identified.east);",
        f"\\draw[prismaArrow] (duplicates.west) -- ++({style['side_bend_dx']},0) |- (screened.east);",
        f"\\draw[prismaArrow] (excluded_ta.west) -- ++({style['side_bend_dx']},0) |- (screened.east);",
        f"\\draw[prismaArrow] (excluded_ft.west) -- ++({style['side_bend_dx']},0) |- (fulltext.east);",
        f"\\node[align=center,font={style['source_font']},text=flowSource] at ({style['source_xy'][0]:.2f},{style['source_xy'][1]:.2f}) {{{tex_escape(layout.source_note)}}};",
        r"\end{tikzpicture}",
    ]

    atomic_write_text(output_path, "\n".join(lines) + "\n")


def render_prisma_flow(
    prisma_df: pd.DataFrame,
    flow_backend: str,
    flow_output_path: Path,
    flow_tex_output_path: Path,
    flow_style: str,
) -> list[Path]:
    counts = prisma_counts(prisma_df)
    layout = build_flow_layout(counts)

    generated_paths: list[Path] = []
    if flow_backend in {"matplotlib", "both"}:
        render_prisma_flow_matplotlib(layout, flow_output_path)
        generated_paths.append(flow_output_path)
    if flow_backend in {"tikz", "both"}:
        render_prisma_flow_tikz(layout, flow_tex_output_path, flow_style=flow_style)
        generated_paths.append(flow_tex_output_path)
    return generated_paths


def compute_fulltext_prisma_stats(fulltext_df: pd.DataFrame) -> dict[str, int | bool]:
    metrics: dict[str, int | bool] = {
        "fulltext_log_present": False,
        "fulltext_records_unique": 0,
        "reports_sought_for_retrieval": 0,
        "reports_not_retrieved": 0,
        "reports_assessed_full_text": 0,
        "reports_excluded_full_text": 0,
        "studies_included_qualitative_synthesis": 0,
    }

    if fulltext_df.empty:
        return metrics

    required = {"record_id", "fulltext_available", "include"}
    if not required.issubset(fulltext_df.columns):
        return metrics

    working = fulltext_df.copy()
    non_empty = working.apply(is_non_empty_row, axis=1)
    working = working[non_empty].copy()

    working["record_id"] = working["record_id"].fillna("").astype(str).str.strip()
    working = working[working["record_id"].ne("")].copy()
    if working.empty:
        return metrics

    working = working.drop_duplicates(["record_id"], keep="last")
    working["fulltext_available_norm"] = working["fulltext_available"].apply(
        normalize_fulltext_available
    )
    working["include_norm"] = working["include"].apply(normalize_fulltext_include)

    not_retrieved = working["fulltext_available_norm"].eq("no")
    assessed = working["fulltext_available_norm"].eq("yes") & working["include_norm"].isin(
        {"include", "exclude"}
    )
    excluded = working["fulltext_available_norm"].eq("yes") & working["include_norm"].eq("exclude")
    included = working["fulltext_available_norm"].eq("yes") & working["include_norm"].eq("include")

    metrics["fulltext_log_present"] = True
    metrics["fulltext_records_unique"] = int(working.shape[0])
    metrics["reports_not_retrieved"] = int(not_retrieved.sum())
    metrics["reports_assessed_full_text"] = int(assessed.sum())
    metrics["reports_excluded_full_text"] = int(excluded.sum())
    metrics["studies_included_qualitative_synthesis"] = int(included.sum())
    metrics["reports_sought_for_retrieval"] = int(metrics["reports_assessed_full_text"]) + int(
        metrics["reports_not_retrieved"]
    )
    return metrics


def compute_title_abstract_prisma_stats(results_df: pd.DataFrame) -> dict[str, int | bool]:
    metrics: dict[str, int | bool] = {
        "title_abstract_results_present": False,
        "title_abstract_records_unique": 0,
        "records_screened_title_abstract": 0,
        "records_excluded_title_abstract": 0,
        "reports_sought_for_retrieval_from_title_abstract": 0,
    }

    if results_df.empty or "record_id" not in results_df.columns:
        return metrics

    working = results_df.copy()
    non_empty = working.apply(is_non_empty_row, axis=1)
    working = working[non_empty].copy()

    working["record_id"] = working["record_id"].fillna("").astype(str).str.strip()
    working = working[working["record_id"].ne("")].copy()
    if working.empty:
        return metrics

    working = working.drop_duplicates(["record_id"], keep="last")

    if "final_decision" in working.columns:
        decision_series = working["final_decision"]
    elif "resolution_decision" in working.columns:
        decision_series = working["resolution_decision"]
    elif "title_abstract_decision" in working.columns:
        decision_series = working["title_abstract_decision"]
    else:
        decision_series = pd.Series("", index=working.index)

    working["decision_norm"] = decision_series.apply(normalize_screening_decision)

    metrics["title_abstract_results_present"] = True
    metrics["title_abstract_records_unique"] = int(working.shape[0])
    metrics["records_screened_title_abstract"] = int(working.shape[0])
    metrics["records_excluded_title_abstract"] = int(working["decision_norm"].eq("exclude").sum())
    metrics["reports_sought_for_retrieval_from_title_abstract"] = int(
        working["decision_norm"].eq("include").sum()
    )
    return metrics


def compute_stats(
    master_df: pd.DataFrame,
    search_df: pd.DataFrame,
    fulltext_df: pd.DataFrame,
    title_abstract_results_df: pd.DataFrame,
) -> dict[str, int | bool]:
    if master_df.empty:
        total_rows = 0
        duplicates = 0
    else:
        working = master_df.copy()
        non_empty = working.apply(is_non_empty_row, axis=1)
        working = working[non_empty].copy()

        total_rows = len(working)
        if "is_duplicate" in working.columns:
            duplicates = int(to_yes(working["is_duplicate"]).sum())
        else:
            duplicates = 0

    unique_rows = max(total_rows - duplicates, 0)

    identified_from_search_log = 0
    if not search_df.empty and "results_total" in search_df.columns:
        identified_from_search_log = int(
            pd.to_numeric(search_df["results_total"], errors="coerce").fillna(0).sum()
        )

    identified_databases = (
        identified_from_search_log if identified_from_search_log > 0 else total_rows
    )

    title_abstract_stats = compute_title_abstract_prisma_stats(title_abstract_results_df)
    fulltext_stats = compute_fulltext_prisma_stats(fulltext_df)

    records_screened_title_abstract = int(title_abstract_stats["records_screened_title_abstract"])
    if not bool(title_abstract_stats["title_abstract_results_present"]):
        records_screened_title_abstract = unique_rows

    reports_sought_for_retrieval = int(
        title_abstract_stats["reports_sought_for_retrieval_from_title_abstract"]
    )
    if bool(fulltext_stats["fulltext_log_present"]):
        reports_sought_for_retrieval = int(fulltext_stats["reports_sought_for_retrieval"])

    stats: dict[str, int | bool] = {
        "records_identified_databases": identified_databases,
        "duplicates_removed": duplicates,
        "records_screened_title_abstract": records_screened_title_abstract,
        "records_excluded_title_abstract": int(
            title_abstract_stats["records_excluded_title_abstract"]
        ),
        "reports_sought_for_retrieval": reports_sought_for_retrieval,
        "total_rows_in_master": total_rows,
        "identified_from_search_log": identified_from_search_log,
        "title_abstract_results_present": bool(
            title_abstract_stats["title_abstract_results_present"]
        ),
        "title_abstract_records_unique": int(title_abstract_stats["title_abstract_records_unique"]),
    }
    stats.update(fulltext_stats)
    if not bool(fulltext_stats["fulltext_log_present"]):
        stats["reports_sought_for_retrieval"] = reports_sought_for_retrieval
    return stats


def update_prisma_counts(prisma_df: pd.DataFrame, stats: dict[str, int | bool]) -> pd.DataFrame:
    required_cols = ["stage", "count", "notes"]
    for col in required_cols:
        if col not in prisma_df.columns:
            prisma_df[col] = ""

    prisma_df["count"] = prisma_df["count"].fillna("").astype(str).replace("nan", "")

    mapping: dict[str, int] = {
        "records_identified_databases": int(stats["records_identified_databases"]),
        "duplicates_removed": int(stats["duplicates_removed"]),
        "records_screened_title_abstract": int(stats["records_screened_title_abstract"]),
    }

    if bool(stats.get("title_abstract_results_present")):
        mapping.update(
            {
                "records_excluded_title_abstract": int(stats["records_excluded_title_abstract"]),
                "reports_sought_for_retrieval": int(stats["reports_sought_for_retrieval"]),
            }
        )

    if bool(stats.get("fulltext_log_present")):
        mapping.update(
            {
                "reports_sought_for_retrieval": int(stats["reports_sought_for_retrieval"]),
                "reports_not_retrieved": int(stats["reports_not_retrieved"]),
                "reports_assessed_full_text": int(stats["reports_assessed_full_text"]),
                "reports_excluded_full_text": int(stats["reports_excluded_full_text"]),
                "studies_included_qualitative_synthesis": int(
                    stats["studies_included_qualitative_synthesis"]
                ),
            }
        )

    for stage, value in mapping.items():
        mask = prisma_df["stage"].astype(str).str.strip().eq(stage)
        if mask.any():
            prisma_df.loc[mask, "count"] = str(int(value))

    return prisma_df


def prisma_sync_deltas(
    before_df: pd.DataFrame, after_df: pd.DataFrame
) -> list[tuple[str, int | None, int | None]]:
    before_counts = prisma_counts(before_df)
    after_counts = prisma_counts(after_df)
    deltas: list[tuple[str, int | None, int | None]] = []

    sync_keys = CORE_PRISMA_SYNC_KEYS + FULLTEXT_PRISMA_SYNC_KEYS
    for stage in sync_keys:
        before_value = before_counts.get(stage)
        after_value = after_counts.get(stage)
        if before_value != after_value:
            deltas.append((stage, before_value, after_value))

    return deltas


def default_backup_path(prisma_path: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return prisma_path.with_name(f"{prisma_path.stem}.backup.{timestamp}{prisma_path.suffix}")


def ensure_backup_snapshot(source_path: Path, backup_path: Path) -> bool:
    source_payload = source_path.read_bytes()
    corrected = False

    if backup_path.read_bytes() != source_payload:
        atomic_replace_bytes(backup_path, source_payload)
        corrected = True

    try:
        backup_df = pd.read_csv(backup_path, dtype=str)
    except Exception:
        atomic_replace_bytes(backup_path, source_payload)
        corrected = True
        backup_df = pd.read_csv(backup_path, dtype=str)

    required_columns = {"stage", "count", "notes"}
    missing_columns = required_columns.difference(set(backup_df.columns))
    if missing_columns:
        raise ValueError(
            f"Backup CSV missing required columns {sorted(missing_columns)}: {backup_path}"
        )

    stage_values = backup_df["stage"].fillna("").astype(str).str.strip().str.lower()
    if stage_values.eq("stage").any():
        atomic_replace_bytes(backup_path, source_payload)
        corrected = True
        reloaded_df = pd.read_csv(backup_path, dtype=str)
        reloaded_stage_values = reloaded_df["stage"].fillna("").astype(str).str.strip().str.lower()
        if reloaded_stage_values.eq("stage").any():
            raise ValueError(f"Backup CSV still contains duplicated header rows: {backup_path}")

    return corrected


def build_summary(
    stats: dict[str, int | bool],
    generated_flow_paths: list[Path],
    *,
    mode_label: str,
    deltas: list[tuple[str, int | None, int | None]],
    applied: bool,
    backup_path: Path | None,
) -> str:
    lines = []
    lines.append("# Dedup Stats Summary")
    lines.append("")
    lines.append("## PRISMA Sync Mode")
    lines.append("")
    lines.append(f"- Mode: `{mode_label}`")
    if applied:
        lines.append("- Apply status: updated `prisma_counts_template.csv`")
    else:
        lines.append("- Apply status: dry-run (no PRISMA file changes written)")
    if backup_path is not None:
        lines.append(f"- Backup path: `{backup_path.as_posix()}`")
    lines.append("")
    lines.append("## Proposed PRISMA Deltas")
    lines.append("")
    if deltas:
        for stage, before_value, after_value in deltas:
            lines.append(
                f"- `{stage}`: `{count_or_dash(before_value)}` → `{count_or_dash(after_value)}`"
            )
    else:
        lines.append("- No PRISMA count changes detected.")
    lines.append("")
    lines.append("## Counts")
    lines.append("")
    lines.append(f"- Records identified from databases: {stats['records_identified_databases']}")
    lines.append(f"- Duplicates removed: {stats['duplicates_removed']}")
    lines.append(f"- Records screened (title/abstract): {stats['records_screened_title_abstract']}")
    lines.append(f"- Reports sought for retrieval: {stats['reports_sought_for_retrieval']}")
    lines.append(f"- Reports not retrieved: {stats['reports_not_retrieved']}")
    lines.append(f"- Reports assessed (full text): {stats['reports_assessed_full_text']}")
    lines.append(f"- Reports excluded (full text): {stats['reports_excluded_full_text']}")
    lines.append(
        f"- Studies included (qualitative): {stats['studies_included_qualitative_synthesis']}"
    )
    lines.append("")
    lines.append("## Diagnostics")
    lines.append("")
    lines.append(f"- Total non-empty rows in `master_records.csv`: {stats['total_rows_in_master']}")
    lines.append(
        f"- Sum of `results_total` in `search_log.csv`: {stats['identified_from_search_log']}"
    )
    lines.append(
        f"- Title/abstract consensus rows (unique record_id): {stats['title_abstract_records_unique']}"
    )
    lines.append(f"- Full-text log rows (unique record_id): {stats['fulltext_records_unique']}")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- `records_identified_databases` uses `search_log.csv` totals when available.")
    lines.append(
        "- If `search_log.csv` totals are empty/zero, fallback uses `master_records.csv` row count."
    )
    if bool(stats.get("title_abstract_results_present")):
        lines.append(
            "- Title/abstract PRISMA counts are synced from `screening_title_abstract_results.csv`."
        )
    else:
        lines.append(
            "- `screening_title_abstract_results.csv` has no usable rows; title/abstract exclusions and retrieval counts are kept unchanged."
        )
    if bool(stats.get("fulltext_log_present")):
        lines.append("- Full-text PRISMA counts are synced from `screening_fulltext_log.csv`.")
    else:
        lines.append(
            "- `screening_fulltext_log.csv` has no usable rows; existing PRISMA full-text counts are kept unchanged."
        )
    if generated_flow_paths:
        generated = ", ".join(f"`{path.as_posix()}`" for path in generated_flow_paths)
        lines.append(f"- Running this script also regenerates {generated} from PRISMA counts.")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compute deduplication stats and update PRISMA count template."
    )
    parser.add_argument(
        "--master",
        default="../02_data/processed/master_records.csv",
        help="Path to master records CSV",
    )
    parser.add_argument(
        "--search-log", default="../02_data/processed/search_log.csv", help="Path to search log CSV"
    )
    parser.add_argument(
        "--screening-fulltext-log",
        default="../02_data/processed/screening_fulltext_log.csv",
        help="Path to full-text screening log CSV",
    )
    parser.add_argument(
        "--screening-title-abstract-results",
        default="../02_data/processed/screening_title_abstract_results.csv",
        help="Path to consolidated title/abstract screening results CSV",
    )
    parser.add_argument(
        "--prisma",
        default="../02_data/processed/prisma_counts_template.csv",
        help="Path to PRISMA counts CSV",
    )
    parser.add_argument(
        "--summary",
        default="outputs/dedup_stats_summary.md",
        help="Path to markdown summary output",
    )
    parser.add_argument(
        "--flow-output",
        default="outputs/prisma_flow_diagram.svg",
        help="Path to PRISMA flow diagram image output (format inferred from extension)",
    )
    parser.add_argument(
        "--flow-tex-output",
        dest="flow_tex_output",
        default="outputs/prisma_flow_diagram.tex",
        help="Path to PRISMA flow diagram TikZ/TeX output",
    )
    parser.add_argument(
        "--flow-tikz-output",
        dest="flow_tex_output",
        default=argparse.SUPPRESS,
        help="Deprecated alias for --flow-tex-output",
    )
    parser.add_argument(
        "--flow-backend",
        default="both",
        choices=["matplotlib", "tikz", "both"],
        help="PRISMA flow renderer backend",
    )
    parser.add_argument(
        "--flow-style",
        default="journal",
        choices=["default", "journal"],
        help="TikZ flow style preset (applies to tikz/both backends)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply synced PRISMA core counts to --prisma CSV (default: dry-run preview only).",
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Create backup before --apply (required when --apply is used).",
    )
    parser.add_argument(
        "--backup-path",
        default="",
        help="Optional explicit backup file path (used with --apply --backup).",
    )
    args = parser.parse_args(argv)

    if args.apply and not args.backup:
        parser.error("--apply requires --backup to prevent accidental PRISMA overwrites.")

    if args.backup_path and not args.apply:
        parser.error("--backup-path requires --apply (and --backup).")

    master_path = Path(args.master)
    search_log_path = Path(args.search_log)
    screening_fulltext_log_path = Path(args.screening_fulltext_log)
    screening_title_abstract_results_path = Path(args.screening_title_abstract_results)
    prisma_path = Path(args.prisma)
    summary_path = Path(args.summary)
    flow_output_path = Path(args.flow_output)
    flow_tex_output_path = Path(args.flow_tex_output)

    if not master_path.exists():
        raise FileNotFoundError(f"Master records file not found: {master_path}")
    if not search_log_path.exists():
        raise FileNotFoundError(f"Search log file not found: {search_log_path}")
    if not prisma_path.exists():
        raise FileNotFoundError(f"PRISMA counts file not found: {prisma_path}")

    master_df = pd.read_csv(master_path)
    search_df = pd.read_csv(search_log_path)
    fulltext_df = (
        pd.read_csv(screening_fulltext_log_path)
        if screening_fulltext_log_path.exists()
        else pd.DataFrame()
    )
    title_abstract_results_df = (
        pd.read_csv(screening_title_abstract_results_path)
        if screening_title_abstract_results_path.exists()
        else pd.DataFrame()
    )
    prisma_df = pd.read_csv(prisma_path)

    stats = compute_stats(master_df, search_df, fulltext_df, title_abstract_results_df)
    updated_prisma = update_prisma_counts(prisma_df.copy(), stats)
    deltas = prisma_sync_deltas(prisma_df, updated_prisma)

    backup_path: Path | None = None
    backup_snapshot_corrected = False
    if args.apply:
        backup_path = (
            Path(args.backup_path) if args.backup_path else default_backup_path(prisma_path)
        )
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(prisma_path, backup_path)
        backup_snapshot_corrected = ensure_backup_snapshot(prisma_path, backup_path)
        atomic_write_dataframe_csv(updated_prisma, prisma_path, index=False)

    generated_flow_paths = render_prisma_flow(
        updated_prisma,
        flow_backend=args.flow_backend,
        flow_output_path=flow_output_path,
        flow_tex_output_path=flow_tex_output_path,
        flow_style=args.flow_style,
    )

    mode_label = "apply" if args.apply else "dry-run"
    summary_text = build_summary(
        stats,
        generated_flow_paths,
        mode_label=mode_label,
        deltas=deltas,
        applied=bool(args.apply),
        backup_path=backup_path,
    )
    atomic_write_text(summary_path, summary_text)

    if args.apply:
        print(f"Backup: {backup_path}")
        if backup_snapshot_corrected:
            print(f"Backup normalized to exact source snapshot: {backup_path}")
        print(f"Updated: {prisma_path}")
    else:
        print(
            "Dry-run: PRISMA counts preview generated; no changes written (use --apply --backup)."
        )
    print(f"Wrote: {summary_path}")
    for output_path in generated_flow_paths:
        print(f"Wrote: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
