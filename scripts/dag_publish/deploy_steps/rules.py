"""DAG naming and queue rule validation."""

import ast
import re
from pathlib import Path

from deploy_steps.exceptions import DeploymentError
from deploy_steps.python_checks import discover_python_files


class RuleChecker:
    """Validate DAG IDs and task queues with configurable regex rules."""

    def __init__(self, name_rules, queue_rules):
        self.name_rules = name_rules
        self.queue_rules = queue_rules

    def validate(self, package_root):
        python_files = discover_python_files(package_root)
        all_dag_ids = []
        all_queues = []

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

        if self.name_rules.enabled:
            if not all_dag_ids:
                raise DeploymentError("No DAG definitions with literal dag_id values were found.")
            self._apply_rules(all_dag_ids, self.name_rules, "dag_id")

        if self.queue_rules.enabled and all_queues:
            self._apply_rules(all_queues, self.queue_rules, "queue")

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


class _DagRuleVisitor(ast.NodeVisitor):
    def __init__(self):
        self.dag_calls = []
        self.queue_values = []

    def visit_Call(self, node):
        if _is_dag_call(node):
            self.dag_calls.append(node)
        for keyword in node.keywords:
            if keyword.arg == "queue":
                self.queue_values.append(keyword.value)
        self.generic_visit(node)


def _is_dag_call(node):
    if not isinstance(node, ast.Call):
        return False
    if isinstance(node.func, ast.Name):
        return node.func.id == "DAG"
    if isinstance(node.func, ast.Attribute):
        return node.func.attr == "DAG"
    return False
