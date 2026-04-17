# package_and_upload_dag.py Validation Guide

## 中文

### 1. 总览

`package_and_upload_dag.py` 里目前有 3 大类 validation：

1. Python 语法检查
2. Airflow CLI 校验
3. DAG 规则检查

它们的责任不同，不是同一层检查。

### 2. Validation 1: Python 语法检查

相关函数：

- `collect_python_sources()`
- `validate_python_sources()`
- `SyntaxChecker.check_files()`

检查方式：

- 遍历所有即将被打包的 `.py` 文件
- 对每个文件执行 `ast.parse(...)`

这一步负责检查：

- 代码是不是合法 Python
- 括号是否闭合
- 冒号是否缺失
- 缩进是否正确
- 语法层面的非法写法

这一步不负责检查：

- import 能不能成功
- Airflow DAG 能不能被加载
- DAG 里是否定义了 `GDT_ET_FEED_SOURCE`
- 变量值是否合法

所以：

- `def broken(:` 会在这里报错
- `printa("hello")` 不会在这里报错
- `GDT_ET_FEED_SOURCE` 缺失也不会在这里报错

如果这一层发现错误：

- 会收集全部语法错误
- 一次性展示给用户
- 用户输入 `go` 才继续

继续之后：

- 只保留语法正确的文件进入后续校验

### 3. Validation 2: Airflow CLI 校验

相关函数：

- `run_airflow_dag_validation()`
- `render_airflow_cli_env()`
- `stage_sources_for_airflow_check()`
- `execute_airflow_validation()`
- `execute_airflow_command()`

检查方式：

- 把语法正确的文件复制到临时 staging DAG 目录
- 读取 `deploy_pipeline.<environment>.json`
- 根据配置注入 Airflow 环境变量
- 如有需要，先执行 `activation_command`

然后依次执行：

1. `python -m airflow db migrate`
2. `python -m airflow dags list-import-errors -l -o json`

这一步负责检查：

- DAG 文件在 Airflow 环境里能不能被导入
- 是否存在 Airflow import error
- conda / python / Airflow 环境是否能正常启动
- 依赖、模块、Airflow runtime 层面的问题

这一步不负责检查：

- Python 语法是否合法
- DAG 的业务规则是否满足

如果这一层发现问题：

- 会把 CLI 返回的错误打印出来
- 用户输入 `go` 才继续

### 4. Validation 3: DAG 规则检查

相关函数：

- `run_dag_rule_validation()`
- `RuleChecker.validate()`

检查前提：

- 只有语法正确的 Python 文件才会进入这一层
- 这一层会进一步判断哪些文件是 Airflow DAG 文件

当前支持识别的 DAG 形式：

1. 经典 `DAG(...)`
2. `@dag` 装饰器

这一步负责检查 3 类规则：

#### 4.1 `dag_id` 规则

由 `rules.name_rules` 控制。

如果启用：

- 会读取字面量 `dag_id`
- 按 allow / deny regex 做校验

#### 4.2 `queue` 规则

由 `rules.queue_rules` 控制。

如果启用：

- 会读取任务里的字面量 `queue`
- 按 allow / deny regex 做校验

#### 4.3 DAG 顶层变量规则

由 `rules.dag_variable_rules` 控制。

例如当前默认规则：

- 变量名：`GDT_ET_FEED_SOURCE`
- 必填：`true`
- 允许值：
  - `camp-us`
  - `ucm`
  - `norkom`
  - `na`

这一步会检查：

- Airflow DAG 文件是否定义了这个顶层变量
- 这个变量是不是字符串字面量
- 它的值是否在允许列表中

所以你在公司服务器上遇到的这个场景：

- DAG 文件没有 `GDT_ET_FEED_SOURCE`

它不属于语法错误，应该在这一层报错，而不是在语法检查阶段报错。

### 5. 普通 Python 文件和 Airflow DAG 文件的区别

普通 Python 文件：

- 会做 Python 语法检查
- 可能进入 Airflow staging 目录
- 但不会因为缺少 DAG 专属变量而报错

Airflow DAG 文件：

- 会做 Python 语法检查
- 会做 Airflow CLI 校验
- 会做 DAG 规则检查
- 会检查 `GDT_ET_FEED_SOURCE` 这类 DAG 顶层变量规则

### 6. 当前 validation 顺序

实际顺序如下：

1. 收集打包范围内全部 `.py` 文件
2. 做 Python 语法检查
3. 如果用户选择继续，只保留语法正确的文件
4. 对这些文件做 Airflow CLI 校验
5. 对这些文件做 DAG 规则检查
6. 全部通过或被用户明确忽略后，才继续打包

### 7. 当前 validation 的配置来源

validation 相关配置主要来自：

- `deploy_pipeline.<environment>.json`

其中：

- `imports`
  控制 Python / conda / activation / timeout
- `airflow_cli`
  控制临时 DAG 目录和 Airflow 环境变量
- `rules`
  控制 `dag_id`、`queue`、`dag_variable_rules`

### 8. 一句话总结

可以把这 3 类 validation 理解为：

- Python 语法检查：代码是不是合法 Python
- Airflow CLI 校验：代码在 Airflow 环境里能不能被加载
- DAG 规则检查：这是不是一个符合团队规则的 DAG 文件

---

## English

### 1. Overview

`package_and_upload_dag.py` currently includes 3 categories of validation:

1. Python syntax validation
2. Airflow CLI validation
3. DAG rule validation

These checks serve different purposes and operate at different layers.

### 2. Validation 1: Python syntax validation

Related functions:

- `collect_python_sources()`
- `validate_python_sources()`
- `SyntaxChecker.check_files()`

How it works:

- iterate through all `.py` files that will be packaged
- run `ast.parse(...)` on each file

This stage is responsible for checking:

- whether the code is valid Python
- whether parentheses are balanced
- whether colons are missing
- whether indentation is correct
- other syntax-level problems

This stage does not check:

- whether imports succeed
- whether an Airflow DAG can be loaded
- whether `GDT_ET_FEED_SOURCE` is defined
- whether variable values are valid

So:

- `def broken(:` fails here
- `printa("hello")` does not fail here
- missing `GDT_ET_FEED_SOURCE` does not fail here

If errors are found at this stage:

- all syntax errors are collected
- all of them are shown together
- the user must type `go` to continue

If the user continues:

- only syntax-valid files move to later validation stages

### 3. Validation 2: Airflow CLI validation

Related functions:

- `run_airflow_dag_validation()`
- `render_airflow_cli_env()`
- `stage_sources_for_airflow_check()`
- `execute_airflow_validation()`
- `execute_airflow_command()`

How it works:

- copy syntax-valid files into a temporary staging DAG directory
- load `deploy_pipeline.<environment>.json`
- inject configured Airflow environment variables
- execute `activation_command` first when configured

Then it runs:

1. `python -m airflow db migrate`
2. `python -m airflow dags list-import-errors -l -o json`

This stage is responsible for checking:

- whether DAG files can be imported inside the Airflow environment
- whether there are Airflow import errors
- whether the conda / python / Airflow environment can start correctly
- dependency, module, and Airflow runtime issues

This stage does not check:

- Python syntax validity
- DAG business-rule compliance

If this stage finds issues:

- the CLI output is shown to the user
- the user must type `go` to continue

### 4. Validation 3: DAG rule validation

Related functions:

- `run_dag_rule_validation()`
- `RuleChecker.validate()`

Preconditions:

- only syntax-valid Python files enter this stage
- this stage further determines which files are Airflow DAG files

Currently supported DAG styles:

1. classic `DAG(...)`
2. `@dag` decorator

This stage checks 3 types of rules:

#### 4.1 `dag_id` rules

Controlled by `rules.name_rules`.

When enabled:

- literal `dag_id` values are read
- allow / deny regex checks are applied

#### 4.2 `queue` rules

Controlled by `rules.queue_rules`.

When enabled:

- literal `queue` values are read from tasks
- allow / deny regex checks are applied

#### 4.3 DAG top-level variable rules

Controlled by `rules.dag_variable_rules`.

For example, the current default rule is:

- variable name: `GDT_ET_FEED_SOURCE`
- required: `true`
- allowed values:
  - `camp-us`
  - `ucm`
  - `norkom`
  - `na`

This stage checks:

- whether an Airflow DAG file defines this top-level variable
- whether the variable is a string literal
- whether the value belongs to the allowed set

So in your server-side example:

- the DAG file does not define `GDT_ET_FEED_SOURCE`

That is not a syntax error. It should fail at this stage, not during syntax validation.

### 5. Difference between ordinary Python files and Airflow DAG files

Ordinary Python files:

- receive Python syntax validation
- may be staged for Airflow validation
- do not fail only because DAG-only variables are missing

Airflow DAG files:

- receive Python syntax validation
- receive Airflow CLI validation
- receive DAG rule validation
- are checked for DAG top-level variable rules such as `GDT_ET_FEED_SOURCE`

### 6. Current validation order

The current order is:

1. collect all `.py` files in the packaging scope
2. run Python syntax validation
3. if the user overrides errors, keep only syntax-valid files
4. run Airflow CLI validation on those files
5. run DAG rule validation on those files
6. continue to packaging only if all validations pass or are explicitly overridden

### 7. Configuration sources for validation

Validation-related configuration mainly comes from:

- `deploy_pipeline.<environment>.json`

In that config:

- `imports`
  controls Python / conda / activation / timeout
- `airflow_cli`
  controls the temporary DAG folder and Airflow environment variables
- `rules`
  controls `dag_id`, `queue`, and `dag_variable_rules`

### 8. One-line summary

You can think of the 3 validations like this:

- Python syntax validation: is this legal Python code
- Airflow CLI validation: can this code be loaded inside Airflow
- DAG rule validation: is this a DAG file that complies with team rules
