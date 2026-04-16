"""DAG tag rewriting based on the configured source."""

import ast
from pathlib import Path

from deploy_steps.exceptions import DeploymentError
from deploy_steps.python_checks import SyntaxChecker, discover_python_files


class TagProcessor:
    """Rewrite managed DAG tags based on a top-level source variable."""

    def __init__(
        self,
        source_variable_name,
        managed_tags,
        us_sources,
        us_tag,
        global_tag,
    ):
        self.source_variable_name = source_variable_name
        self.managed_tags = list(managed_tags)
        self.us_sources = set(us_sources)
        self.us_tag = us_tag
        self.global_tag = global_tag
        self.syntax_checker = SyntaxChecker()

    def process_package(self, package_root):
        processed_files = []
        python_files = discover_python_files(package_root)
        for python_file in python_files:
            if self._process_file(python_file):
                processed_files.append(python_file)
        if not processed_files:
            raise DeploymentError("No DAG definitions found for tag processing.")
        self.syntax_checker.run(package_root, processed_files)
        return processed_files

    def _process_file(self, python_file):
        source_text = Path(python_file).read_text(encoding="utf-8")
        tree = ast.parse(source_text, filename=str(python_file))
        dag_calls = self._collect_classic_dag_calls(tree)
        if not dag_calls:
            return False

        source_value = self._extract_source_value(tree, python_file)
        target_tag = self.us_tag if source_value in self.us_sources else self.global_tag
        line_offsets = self._line_offsets(source_text)
        replacements = []

        for dag_call in dag_calls:
            tags_keyword = self._find_tags_keyword(dag_call)
            if tags_keyword is None:
                replacement = self._build_missing_tags_insertion(
                    source_text,
                    dag_call,
                    line_offsets,
                    target_tag,
                )
            else:
                current_tags = self._extract_literal_tags(tags_keyword.value, python_file)
                updated_tags = [tag for tag in current_tags if tag not in self.managed_tags]
                updated_tags.append(target_tag)
                start, end = self._span(tags_keyword.value, line_offsets)
                replacement = (start, end, repr(updated_tags))
            replacements.append(replacement)

        if replacements:
            updated_source = self._apply_replacements(source_text, replacements)
            if updated_source != source_text:
                Path(python_file).write_text(updated_source, encoding="utf-8")
        return True

    def _collect_classic_dag_calls(self, tree):
        dag_calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.With):
                for item in node.items:
                    if _is_dag_call(item.context_expr):
                        dag_calls.append(item.context_expr)
            elif isinstance(node, ast.Assign) and _is_dag_call(node.value):
                dag_calls.append(node.value)
        return dag_calls

    def _extract_source_value(self, tree, python_file):
        found_assignment = False
        for node in tree.body:
            target_name, value_node = self._top_level_assignment(node)
            if target_name != self.source_variable_name:
                continue
            found_assignment = True
            if isinstance(value_node, ast.Constant) and isinstance(value_node.value, str):
                return value_node.value
            raise DeploymentError(
                "Top-level source variable '{0}' in {1} must be a string literal.".format(
                    self.source_variable_name,
                    python_file,
                )
            )
        if found_assignment:
            raise DeploymentError(
                "Top-level source variable '{0}' in {1} must be a string literal.".format(
                    self.source_variable_name,
                    python_file,
                )
            )
        raise DeploymentError(
            "Top-level source variable '{0}' is required in {1}.".format(
                self.source_variable_name,
                python_file,
            )
        )

    def _top_level_assignment(self, node):
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            return node.targets[0].id, node.value
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            return node.target.id, node.value
        return None, None

    def _find_tags_keyword(self, dag_call):
        for keyword in dag_call.keywords:
            if keyword.arg == "tags":
                return keyword
        return None

    def _extract_literal_tags(self, node, python_file):
        if not isinstance(node, (ast.List, ast.Tuple)):
            raise DeploymentError(
                "tags in {0} must be a literal list or tuple of strings.".format(python_file)
            )
        tags = []
        for element in node.elts:
            if not (isinstance(element, ast.Constant) and isinstance(element.value, str)):
                raise DeploymentError(
                    "tags in {0} must be a literal list or tuple of strings.".format(
                        python_file
                    )
                )
            tags.append(element.value)
        return tags

    def _build_missing_tags_insertion(self, source_text, dag_call, line_offsets, target_tag):
        start, end = self._span(dag_call, line_offsets)
        close_index = end - 1
        if source_text[close_index] != ")":
            raise DeploymentError("Unable to locate DAG call closing parenthesis for tag insertion.")

        if dag_call.lineno == dag_call.end_lineno:
            return (close_index, close_index, ", tags={0}".format(repr([target_tag])))

        indent = self._keyword_indent(source_text, dag_call)
        previous_character = source_text[close_index - 1] if close_index > 0 else ""
        if previous_character == "\n":
            insertion = "{0}tags={1},\n".format(indent, repr([target_tag]))
        else:
            insertion = ",\n{0}tags={1}".format(indent, repr([target_tag]))
        return (close_index, close_index, insertion)

    def _keyword_indent(self, source_text, dag_call):
        source_lines = source_text.splitlines(True)
        if dag_call.keywords:
            line_text = source_lines[dag_call.keywords[0].lineno - 1]
            return line_text[: dag_call.keywords[0].col_offset]
        return " " * (dag_call.col_offset + 4)

    def _span(self, node, line_offsets):
        start = line_offsets[node.lineno - 1] + node.col_offset
        end = line_offsets[node.end_lineno - 1] + node.end_col_offset
        return start, end

    def _line_offsets(self, source_text):
        offsets = []
        total = 0
        for line in source_text.splitlines(True):
            offsets.append(total)
            total += len(line)
        if not offsets:
            offsets.append(0)
        return offsets

    def _apply_replacements(self, source_text, replacements):
        updated = source_text
        for start, end, replacement in sorted(replacements, key=lambda item: item[0], reverse=True):
            updated = updated[:start] + replacement + updated[end:]
        return updated


def _is_dag_call(node):
    if not isinstance(node, ast.Call):
        return False
    if isinstance(node.func, ast.Name):
        return node.func.id == "DAG"
    if isinstance(node.func, ast.Attribute):
        return node.func.attr == "DAG"
    return False
