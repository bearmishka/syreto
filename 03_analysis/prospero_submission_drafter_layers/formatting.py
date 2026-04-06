from __future__ import annotations

import re


def strip_markdown_markup(value: str) -> str:
    text = value.strip()
    text = re.sub(r"`([^`]*)`", r"\1", text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)
    text = re.sub(r"\*([^*]+)\*", r"\1", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def collapse_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def extract_braced_content(text: str, brace_start_index: int) -> tuple[str, int] | None:
    if brace_start_index >= len(text) or text[brace_start_index] != "{":
        return None

    depth = 0
    chars: list[str] = []
    index = brace_start_index
    while index < len(text):
        char = text[index]

        if char == "\\":
            if depth > 0:
                chars.append(char)
            if index + 1 < len(text):
                if depth > 0:
                    chars.append(text[index + 1])
                index += 2
                continue

        if char == "{":
            depth += 1
            if depth > 1:
                chars.append(char)
            index += 1
            continue

        if char == "}":
            depth -= 1
            if depth == 0:
                return "".join(chars), index + 1
            chars.append(char)
            index += 1
            continue

        if depth > 0:
            chars.append(char)
        index += 1

    return None


def extract_latex_macro_argument(text: str, macro: str) -> str:
    pattern = re.compile(rf"\\{macro}\s*", re.DOTALL)
    match = pattern.search(text)
    if not match:
        return ""

    brace_start = text.find("{", match.end())
    if brace_start == -1:
        return ""

    extracted = extract_braced_content(text, brace_start)
    if extracted is None:
        return ""
    content, _ = extracted
    return content.strip()


def strip_latex_macro_with_argument(text: str, macro: str) -> str:
    output: list[str] = []
    index = 0
    needle = f"\\{macro}"

    while index < len(text):
        macro_index = text.find(needle, index)
        if macro_index == -1:
            output.append(text[index:])
            break

        output.append(text[index:macro_index])
        cursor = macro_index + len(needle)
        while cursor < len(text) and text[cursor].isspace():
            cursor += 1

        if cursor < len(text) and text[cursor] == "{":
            extracted = extract_braced_content(text, cursor)
            if extracted is None:
                index = cursor + 1
            else:
                _, cursor_after = extracted
                index = cursor_after
        else:
            index = cursor

    return "".join(output)


def latex_to_plain(text: str) -> str:
    value = text
    value = re.sub(r"\\href\{[^{}]*\}\{([^{}]*)\}", r"\1", value)
    value = re.sub(r"\\textbf\{([^{}]*)\}", r"\1", value)
    value = re.sub(r"\\textit\{([^{}]*)\}", r"\1", value)
    value = value.replace("\\\\", "\n")
    value = re.sub(r"\\[a-zA-Z]+", "", value)
    value = value.replace("{", "").replace("}", "")
    value = value.replace("~", " ")
    return collapse_whitespace(value)