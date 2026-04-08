import argparse
import ast
import io
import json
import tokenize
from dataclasses import dataclass, field
from pathlib import Path

MAIN_GUARD_NAME = "__name__"
MAIN_MODULE_NAME = "__main__"


@dataclass
class GuardReport:
    scanned_python_files: int = 0
    scanned_test_files: int = 0
    syntax_errors: list[str] = field(default_factory=list)
    multiple_main_guards: list[str] = field(default_factory=list)
    run_on_after_main_exit: list[str] = field(default_factory=list)
    duplicated_test_modules: list[str] = field(default_factory=list)
    duplicate_header_lines: list[str] = field(default_factory=list)
    duplicate_header_constants: list[str] = field(default_factory=list)
    strict_header_duplicates: bool = False

    def total_issues(self) -> int:
        return (
            len(self.syntax_errors)
            + len(self.multiple_main_guards)
            + len(self.run_on_after_main_exit)
            + len(self.duplicated_test_modules)
            + len(self.duplicate_header_lines)
            + len(self.duplicate_header_constants)
        )

    def has_issues(self) -> bool:
        return self.total_issues() > 0

    def to_dict(self) -> dict[str, object]:
        return {
            "scanned_python_files": self.scanned_python_files,
            "scanned_test_files": self.scanned_test_files,
            "syntax_errors": self.syntax_errors,
            "multiple_main_guards": self.multiple_main_guards,
            "run_on_after_main_exit": self.run_on_after_main_exit,
            "duplicated_test_modules": self.duplicated_test_modules,
            "duplicate_header_lines": self.duplicate_header_lines,
            "duplicate_header_constants": self.duplicate_header_constants,
            "strict_header_duplicates": self.strict_header_duplicates,
            "total_issues": self.total_issues(),
            "has_issues": self.has_issues(),
        }


def iter_python_files(project_root: Path, *, include_fixtures: bool = False) -> list[Path]:
    files: list[Path] = []
    for path in project_root.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        if not include_fixtures and "fixtures" in path.parts:
            continue
        files.append(path)
    return sorted(files)


def iter_test_python_files(tests_root: Path, *, include_fixtures: bool = False) -> list[Path]:
    if not tests_root.exists():
        return []

    files: list[Path] = []
    for path in tests_root.rglob("test_*.py"):
        if "__pycache__" in path.parts:
            continue
        if not include_fixtures and "fixtures" in path.parts:
            continue
        files.append(path)
    return sorted(files)


def parse_module(source: str, filename: str) -> ast.Module | None:
    try:
        return ast.parse(source, filename=filename)
    except SyntaxError:
        return None


def is_main_guard_compare(node: ast.AST) -> bool:
    if not isinstance(node, ast.Compare):
        return False
    if len(node.ops) != 1 or not isinstance(node.ops[0], ast.Eq):
        return False
    if len(node.comparators) != 1:
        return False

    left = node.left
    right = node.comparators[0]
    direct = (
        isinstance(left, ast.Name)
        and left.id == MAIN_GUARD_NAME
        and isinstance(right, ast.Constant)
        and right.value == MAIN_MODULE_NAME
    )
    reverse = (
        isinstance(right, ast.Name)
        and right.id == MAIN_GUARD_NAME
        and isinstance(left, ast.Constant)
        and left.value == MAIN_MODULE_NAME
    )
    return direct or reverse


def count_main_guard_statements(module: ast.Module) -> int:
    count = 0
    for node in module.body:
        if isinstance(node, ast.If) and is_main_guard_compare(node.test):
            count += 1
    return count


def is_raise_system_exit_main(node: ast.AST) -> bool:
    if not isinstance(node, ast.Raise):
        return False
    if not isinstance(node.exc, ast.Call):
        return False

    exit_call = node.exc
    if not isinstance(exit_call.func, ast.Name) or exit_call.func.id != "SystemExit":
        return False
    if len(exit_call.args) != 1 or exit_call.keywords:
        return False

    main_call = exit_call.args[0]
    if not isinstance(main_call, ast.Call):
        return False
    if not isinstance(main_call.func, ast.Name) or main_call.func.id != "main":
        return False
    return not main_call.keywords


def tokenize_by_line(source: str) -> dict[int, list[tokenize.TokenInfo]]:
    tokens: dict[int, list[tokenize.TokenInfo]] = {}
    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        tokens.setdefault(token.start[0], []).append(token)
    return tokens


def find_run_on_tokens_after_main_exit(path: Path, source: str, module: ast.Module) -> list[str]:
    ignored_types = {
        tokenize.NL,
        tokenize.NEWLINE,
        tokenize.COMMENT,
        tokenize.INDENT,
        tokenize.DEDENT,
        tokenize.ENDMARKER,
    }
    tokens_by_line = tokenize_by_line(source)
    issues: list[str] = []

    for node in ast.walk(module):
        if not is_raise_system_exit_main(node):
            continue

        end_line = getattr(node, "end_lineno", node.lineno)
        end_col = getattr(node, "end_col_offset", None)
        if end_col is None:
            continue

        for token in tokens_by_line.get(end_line, []):
            if token.start[1] < end_col:
                continue
            if token.type in ignored_types:
                continue
            issues.append(
                f"{path}:{end_line}:{token.start[1] + 1} token '{token.string}' after raise SystemExit(main())"
            )
            break

    return issues


def looks_like_full_module_duplication(module: ast.Module) -> bool:
    statements = module.body
    if len(statements) < 2 or len(statements) % 2 != 0:
        return False

    half = len(statements) // 2
    first_half = statements[:half]
    second_half = statements[half:]
    for left, right in zip(first_half, second_half):
        if ast.dump(left, include_attributes=False) != ast.dump(right, include_attributes=False):
            return False
    return True


def is_docstring_expression(statement: ast.stmt) -> bool:
    return (
        isinstance(statement, ast.Expr)
        and isinstance(statement.value, ast.Constant)
        and isinstance(statement.value.value, str)
    )


def is_header_statement(statement: ast.stmt) -> bool:
    if isinstance(statement, (ast.Import, ast.ImportFrom, ast.Assign, ast.AnnAssign)):
        return True
    return is_docstring_expression(statement)


def iter_header_statements(module: ast.Module) -> list[ast.stmt]:
    header: list[ast.stmt] = []
    for statement in module.body:
        if not is_header_statement(statement):
            break
        header.append(statement)
    return header


def find_duplicate_header_lines(path: Path, source: str, module: ast.Module) -> list[str]:
    line_map: dict[str, list[int]] = {}
    source_lines = source.splitlines()

    for statement in iter_header_statements(module):
        start_line = statement.lineno
        end_line = getattr(statement, "end_lineno", start_line)
        if start_line != end_line:
            continue

        line_text = source_lines[start_line - 1].strip()
        if not line_text or line_text.startswith("#"):
            continue
        line_map.setdefault(line_text, []).append(start_line)

    duplicates: list[str] = []
    for line_text, line_numbers in line_map.items():
        if len(line_numbers) < 2:
            continue
        lines_joined = ", ".join(str(number) for number in line_numbers)
        duplicates.append(f"{path}:{lines_joined} duplicate header line `{line_text}`")

    return duplicates


def find_duplicate_header_constants(
    path: Path,
    module: ast.Module,
    *,
    strict_header_duplicates: bool = False,
) -> list[str]:
    by_key: dict[tuple[str, str], int] = {}
    by_name: dict[str, int] = {}
    duplicates: list[str] = []

    for statement in iter_header_statements(module):
        name: str | None = None
        value_node: ast.AST | None = None

        if isinstance(statement, ast.Assign):
            if len(statement.targets) != 1 or not isinstance(statement.targets[0], ast.Name):
                continue
            name = statement.targets[0].id
            value_node = statement.value
        elif isinstance(statement, ast.AnnAssign):
            if not isinstance(statement.target, ast.Name) or statement.value is None:
                continue
            name = statement.target.id
            value_node = statement.value

        if name is None or value_node is None:
            continue

        current_line = statement.lineno

        if strict_header_duplicates:
            first_line = by_name.get(name)
            if first_line is None:
                by_name[name] = current_line
                continue

            duplicates.append(
                f"{path}:{current_line} duplicate header assignment `{name}` (first declared at line {first_line})"
            )
            continue

        if not name.isupper():
            continue

        key = (name, ast.dump(value_node, include_attributes=False))
        first_line = by_key.get(key)
        if first_line is None:
            by_key[key] = current_line
            continue

        duplicates.append(
            f"{path}:{current_line} duplicate header constant `{name}` (first declared at line {first_line})"
        )

    return duplicates


def run_guard(
    project_root: Path,
    tests_root: Path,
    *,
    strict_header_duplicates: bool = False,
    include_fixtures: bool = False,
) -> GuardReport:
    report = GuardReport(strict_header_duplicates=strict_header_duplicates)

    python_files = iter_python_files(project_root, include_fixtures=include_fixtures)
    report.scanned_python_files = len(python_files)

    for path in python_files:
        source = path.read_text(encoding="utf-8", errors="replace")

        try:
            compile(source, str(path), "exec")
        except SyntaxError as exc:
            report.syntax_errors.append(f"{path}:{exc.lineno}:{exc.offset} {exc.msg}")
            continue

        module = parse_module(source, str(path))
        if module is None:
            continue

        guard_count = count_main_guard_statements(module)
        if guard_count > 1:
            report.multiple_main_guards.append(f"{path}: {guard_count}")

        report.run_on_after_main_exit.extend(
            find_run_on_tokens_after_main_exit(path=path, source=source, module=module)
        )
        report.duplicate_header_lines.extend(
            find_duplicate_header_lines(path=path, source=source, module=module)
        )
        report.duplicate_header_constants.extend(
            find_duplicate_header_constants(
                path=path,
                module=module,
                strict_header_duplicates=strict_header_duplicates,
            )
        )

    test_files = iter_test_python_files(tests_root, include_fixtures=include_fixtures)
    report.scanned_test_files = len(test_files)

    for path in test_files:
        source = path.read_text(encoding="utf-8", errors="replace")
        module = parse_module(source, str(path))
        if module is None:
            continue

        if looks_like_full_module_duplication(module):
            report.duplicated_test_modules.append(str(path))

    return report


def print_report(report: GuardReport) -> None:
    print(
        f"Scanned {report.scanned_python_files} Python files "
        f"({report.scanned_test_files} test modules)."
    )

    if report.syntax_errors:
        print("[syntax_errors]")
        for item in report.syntax_errors:
            print(f"- {item}")

    if report.multiple_main_guards:
        print("[multiple_main_guards]")
        for item in report.multiple_main_guards:
            print(f"- {item}")

    if report.run_on_after_main_exit:
        print("[run_on_after_main_exit]")
        for item in report.run_on_after_main_exit:
            print(f"- {item}")

    if report.duplicated_test_modules:
        print("[duplicated_test_modules]")
        for item in report.duplicated_test_modules:
            print(f"- {item}")

    if report.duplicate_header_lines:
        print("[duplicate_header_lines]")
        for item in report.duplicate_header_lines:
            print(f"- {item}")

    if report.duplicate_header_constants:
        print("[duplicate_header_constants]")
        for item in report.duplicate_header_constants:
            print(f"- {item}")

    if report.has_issues():
        print(f"Python source guard failed with {report.total_issues()} issue(s).")
    else:
        print("Python source guard passed with no issues.")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent

    parser = argparse.ArgumentParser(
        description="Run AST/tokenize-based integrity checks for Python sources."
    )
    parser.add_argument(
        "--project-root",
        default=str(script_dir),
        help="Root directory to scan for Python files (default: script directory).",
    )
    parser.add_argument(
        "--tests-root",
        default=str(script_dir / "tests"),
        help="Directory containing test modules for duplication checks.",
    )
    parser.add_argument(
        "--quiet",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Only print failures and final status.",
    )
    parser.add_argument(
        "--json",
        dest="json_output",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Emit machine-readable JSON report.",
    )
    parser.add_argument(
        "--strict-header-duplicates",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Flag repeated header assignments for any variable name, not only UPPER_CASE constants.",
    )
    parser.add_argument(
        "--include-fixtures",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Include files inside fixture directories when scanning.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = Path(args.project_root)
    tests_root = Path(args.tests_root)

    report = run_guard(
        project_root=project_root,
        tests_root=tests_root,
        strict_header_duplicates=args.strict_header_duplicates,
        include_fixtures=args.include_fixtures,
    )

    if args.json_output:
        print(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True))
    elif args.quiet:
        if report.has_issues():
            print_report(report)
        else:
            print(
                f"Python source guard passed for {report.scanned_python_files} files "
                f"({report.scanned_test_files} test modules)."
            )
    else:
        print_report(report)

    if report.has_issues():
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
