"""DAG naming and queue rule validation."""

import ast
import re
from pathlib import Path

from deploy_steps.exceptions import DeploymentError
from deploy_steps.python_checks import discover_python_files


class RuleChecker:
    """Validate DAG IDs and task queues with configurable regex rules."""

    def __init__(self, name_rules, queue_rules, dag_variable_rules=None):
        self.name_rules = name_rules
        self.queue_rules = queue_rules
        self.dag_variable_rules = list(dag_variable_rules or [])

    def validate(self, package_root):
        python_files = discover_python_files(package_root)
        all_dag_ids = []
        all_queues = []
        dag_variable_errors = []

        for python_file in python_files:
            tree = ast.parse(python_file.read_text(encoding="utf-8"), filename=str(python_file))
            visitor = _DagRuleVisitor()
            visitor.visit(tree)

            if self.name_rules.enabled:
                for dag_call in visitor.dag_calls:
                    dag_id = self._extract_dag_id(dag_call, python_file)
                    all_dag_ids.append((python_file, dag_id))

            if self.queue_rules.enabled:
                for queue_value in visitor.queue_values:
                    queue_name = self._extract_literal_string(queue_value, python_file, "queue")
                    all_queues.append((python_file, queue_name))

            if self.dag_variable_rules and visitor.has_dag_definitions:
                dag_variable_errors.extend(
                    self._validate_dag_variables(
                        python_file=python_file,
                        assignments=visitor.module_assignments,
                    )
                )

        if self.name_rules.enabled:
            if not all_dag_ids:
                raise DeploymentError("No DAG definitions with literal dag_id values were found.")
            self._apply_rules(all_dag_ids, self.name_rules, "dag_id")

        if self.queue_rules.enabled and all_queues:
            self._apply_rules(all_queues, self.queue_rules, "queue")

        if dag_variable_errors:
            raise DeploymentError("\n".join(dag_variable_errors))

    def _extract_dag_id(self, dag_call, python_file):
        for keyword in dag_call.keywords:
            if keyword.arg == "dag_id":
                return self._extract_literal_string(keyword.value, python_file, "dag_id")
        if dag_call.args:
            return self._extract_literal_string(dag_call.args[0], python_file, "dag_id")
        raise DeploymentError(
            "DAG call in {0} does not contain a literal dag_id.".format(python_file)
        )

    def _extract_literal_string(self, node, python_file, field_name):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        raise DeploymentError(
            "{0} in {1} must be a string literal so it can be validated.".format(
                field_name,
                python_file,
            )
        )

    def _apply_rules(self, values, rules, label):
        allow_patterns = [re.compile(pattern) for pattern in rules.allow_patterns]
        deny_patterns = [re.compile(pattern) for pattern in rules.deny_patterns]
        for python_file, value in values:
            if allow_patterns and not any(pattern.search(value) for pattern in allow_patterns):
                raise DeploymentError(
                    "{0} '{1}' in {2} does not match any allowed pattern.".format(
                        label,
                        value,
                        python_file,
                    )
                )
            if any(pattern.search(value) for pattern in deny_patterns):
                raise DeploymentError(
                    "{0} '{1}' in {2} matches a denied pattern.".format(
                        label,
                        value,
                        python_file,
                    )
                )

    def _validate_dag_variables(self, python_file, assignments):
        errors = []
        for rule in self.dag_variable_rules:
            assignment = assignments.get(rule.name)
            if assignment is None:
                if rule.required:
                    errors.append(
                        "Airflow DAG file {0} must define top-level variable '{1}'.".format(
                            python_file,
                            rule.name,
                        )
                    )
                continue

            literal_value = assignment.get("value")
            if literal_value is None:
                errors.append(
                    "Variable '{0}' in {1} must be a string literal so it can be validated.".format(
                        rule.name,
                        python_file,
                    )
                )
                continue

            if rule.allowed_values and literal_value not in set(rule.allowed_values):
                errors.append(
                    "Variable '{0}' in {1} must be one of: {2}. Current value: {3!r}.".format(
                        rule.name,
                        python_file,
                        ", ".join(rule.allowed_values),
                        literal_value,
                    )
                )
        return errors


class _DagRuleVisitor(ast.NodeVisitor):
    def __init__(self):
        self.dag_calls = []
        self.dag_decorators = []
        self.queue_values = []
        self.module_assignments = {}

    @property
    def has_dag_definitions(self):
        return bool(self.dag_calls or self.dag_decorators)

    def visit_Module(self, node):
        for statement in node.body:
            _collect_module_assignment(statement, self.module_assignments)
        self.generic_visit(node)

    def visit_Call(self, node):
        if _is_dag_call(node):
            self.dag_calls.append(node)
        for keyword in node.keywords:
            if keyword.arg == "queue":
                self.queue_values.append(keyword.value)
        self.generic_visit(node)

    def visit_FunctionDef(self, node):
        for decorator in node.decorator_list:
            if _is_dag_decorator(decorator):
                self.dag_decorators.append(decorator)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node):
        for decorator in node.decorator_list:
            if _is_dag_decorator(decorator):
                self.dag_decorators.append(decorator)
        self.generic_visit(node)


def discover_airflow_dag_files(package_root=None, python_files=None):
    """Return Python files that define Airflow DAGs via DAG(...) or @dag."""
    if python_files is None:
        if package_root is None:
            raise ValueError("package_root or python_files is required.")
        python_files = discover_python_files(package_root)

    detected = []
    for python_file in sorted(Path(item).expanduser().resolve() for item in python_files):
        try:
            tree = ast.parse(python_file.read_text(encoding="utf-8"), filename=str(python_file))
        except SyntaxError:
            continue
        visitor = _DagRuleVisitor()
        visitor.visit(tree)
        if visitor.has_dag_definitions:
            detected.append(python_file)
    return detected


def _is_dag_call(node):
    if not isinstance(node, ast.Call):
        return False
    if isinstance(node.func, ast.Name):
        return node.func.id == "DAG"
    if isinstance(node.func, ast.Attribute):
        return node.func.attr == "DAG"
    return False


def _is_dag_decorator(node):
    if isinstance(node, ast.Name):
        return node.id == "dag"
    if isinstance(node, ast.Attribute):
        return node.attr == "dag"
    if isinstance(node, ast.Call):
        return _is_dag_decorator(node.func)
    return False


def _collect_module_assignment(statement, assignments):
    if isinstance(statement, ast.Assign):
        value = _extract_string_literal(statement.value)
        for target in statement.targets:
            if isinstance(target, ast.Name):
                assignments[target.id] = {"value": value}
    elif isinstance(statement, ast.AnnAssign) and isinstance(statement.target, ast.Name):
        assignments[statement.target.id] = {"value": _extract_string_literal(statement.value)}


def _extract_string_literal(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None
