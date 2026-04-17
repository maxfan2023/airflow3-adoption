# package_and_upload_dag.py Execution Flow

## 中文

### 1. 入口

`package_and_upload_dag.py` 的入口是 `main()`。

它首先会做 3 件事：

1. `parse_args()`
   解析命令行参数，例如：
   - `sources`
   - `--artifact-id`
   - `--version`
   - `--environment`
   - `--config`
   - `--dry-run`
   - `--debug`

2. `resolve_runtime_logging_settings()`
   解析本次运行的日志目录和日志保留策略。

3. `ScriptOutputSession(...)`
   建立本次运行的 stdout / stderr tee 日志。

然后进入 `_run_with_args()`，后续主流程都在这里执行。

### 2. Step 1: 初始化打包上下文

这一阶段主要调用：

- `resolve_credentials_file()`
- `load_properties()`
- `normalize_sources()`

作用是：

- 找到 Nexus 凭据文件
- 读取凭据内容
- 把用户传入的 source 路径规范化成绝对路径

如果 source 不存在，或者凭据文件找不到，这一步就会直接失败。

### 3. Step 2: Python 语法检查

这一阶段主要调用：

- `collect_python_sources()`
- `validate_python_sources()`
- `SyntaxChecker.check_files()`

作用是：

- 从所有将被打包的内容中找出 `.py` 文件
- 对每个 `.py` 文件做 Python 语法解析

如果有语法错误：

- 会先把所有语法错误一起打印出来
- 然后要求操作者输入 `go`

如果没有输入 `go`：

- 流程终止
- 不会继续打包

如果输入了 `go`：

- 会忽略这些语法错误
- 但后续只会继续处理语法正确的 Python 文件

### 4. Step 3: Airflow CLI 校验

这一阶段主要调用：

- `run_airflow_dag_validation()`
- `load_pipeline_config()`
- `render_airflow_cli_env()`
- `stage_sources_for_airflow_check()`
- `execute_airflow_validation()`
- `execute_airflow_command()`

执行顺序是：

1. 读取 `deploy_pipeline.<environment>.json`
2. 准备临时 Airflow 校验目录
3. 把语法正确的文件复制到 staging 目录
4. 按配置注入：
   - `AIRFLOW__CORE__DAGS_FOLDER`
   - `AIRFLOW__CORE__LOAD_EXAMPLES`
   - `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`
5. 如果配置了 `activation_command`，就先激活 conda / miniconda 环境
6. 执行：
   - `python -m airflow db migrate`
   - `python -m airflow dags list-import-errors -l -o json`

如果发现 Airflow import error 或环境异常：

- 会把错误打印出来
- 要求操作者输入 `go`

如果没有输入 `go`：

- 流程终止

如果输入了 `go`：

- 继续往下走

### 5. Step 4: DAG 规则检查

这一阶段主要调用：

- `run_dag_rule_validation()`
- `load_pipeline_config()`
- `stage_sources_for_airflow_check()`
- `RuleChecker.validate()`

作用是：

- 只针对语法正确的文件继续做 DAG 规则校验
- 检测哪些文件是 Airflow DAG 文件
- 按配置里的 `rules` 执行校验

这里会识别两类 DAG 定义方式：

- 经典 `DAG(...)`
- `@dag` 装饰器

如果规则校验报错：

- 会展示错误
- 要求操作者输入 `go`

如果没有输入 `go`：

- 流程终止

如果输入了 `go`：

- 继续打包

### 6. Step 5: 创建压缩包

这一阶段主要调用：

- `derive_artifact_id()`
- `build_archive_name()`
- `create_archive()`

作用是：

- 计算最终 artifact 名称
- 生成 zip 文件
- 计算生成包的 SHA256

### 7. Step 6: 准备上传地址

这一阶段主要调用：

- `resolve_repository_url()`
- `build_default_upload_path()`
- `build_upload_url()`

作用是：

- 决定要上传到哪个 Nexus repository
- 决定仓库内路径
- 生成最终上传 URL

### 8. Step 7: 上传或 dry-run 结束

如果带了 `--dry-run`：

- 不会真正上传
- 直接输出 summary

如果没有带 `--dry-run`：

- 调用 `upload_archive()`
- 通过 HTTP PUT 上传 zip 到 Nexus

### 9. Step 8: 输出总结和日志路径

流程结束后，`main()` 会打印：

- 归档文件路径
- 环境
- 凭据文件
- SHA256
- 上传目标

最后还会调用：

- `report_log_file_locations()`

输出：

- stdout 日志文件位置
- stderr 日志文件位置

### 10. 一句话总结

`package_and_upload_dag.py` 的主调用链可以概括成：

`main()`
→ `parse_args()`
→ `resolve_runtime_logging_settings()`
→ `ScriptOutputSession`
→ `_run_with_args()`
→ 初始化
→ Python 语法检查
→ Airflow CLI 校验
→ DAG 规则检查
→ 创建 zip
→ 生成上传地址
→ 上传或 dry-run 结束
→ 输出日志路径

---

## English

### 1. Entry point

The entry point of `package_and_upload_dag.py` is `main()`.

It starts with 3 tasks:

1. `parse_args()`
   Parse CLI arguments such as:
   - `sources`
   - `--artifact-id`
   - `--version`
   - `--environment`
   - `--config`
   - `--dry-run`
   - `--debug`

2. `resolve_runtime_logging_settings()`
   Resolve the log directory and retention policy for this run.

3. `ScriptOutputSession(...)`
   Create tee logging for stdout and stderr.

Then it enters `_run_with_args()`, where the main workflow is executed.

### 2. Step 1: Initialize package context

This stage mainly calls:

- `resolve_credentials_file()`
- `load_properties()`
- `normalize_sources()`

Its purpose is to:

- find the Nexus credentials file
- read credential values
- normalize source paths to absolute paths

If a source path does not exist, or the credentials file cannot be found, the script fails here.

### 3. Step 2: Python syntax validation

This stage mainly calls:

- `collect_python_sources()`
- `validate_python_sources()`
- `SyntaxChecker.check_files()`

Its purpose is to:

- find all packaged `.py` files
- run Python syntax parsing on each file

If syntax errors are found:

- all syntax errors are shown together
- the operator is prompted to type `go`

If `go` is not provided:

- the workflow stops
- packaging does not continue

If `go` is provided:

- syntax errors are overridden
- only syntax-valid Python files continue into later stages

### 4. Step 3: Airflow CLI validation

This stage mainly calls:

- `run_airflow_dag_validation()`
- `load_pipeline_config()`
- `render_airflow_cli_env()`
- `stage_sources_for_airflow_check()`
- `execute_airflow_validation()`
- `execute_airflow_command()`

The execution order is:

1. load `deploy_pipeline.<environment>.json`
2. prepare a temporary Airflow validation directory
3. copy syntax-valid files into the staging directory
4. inject configured environment variables:
   - `AIRFLOW__CORE__DAGS_FOLDER`
   - `AIRFLOW__CORE__LOAD_EXAMPLES`
   - `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`
5. if `activation_command` is configured, activate the conda / miniconda environment first
6. execute:
   - `python -m airflow db migrate`
   - `python -m airflow dags list-import-errors -l -o json`

If an Airflow import error or environment issue is found:

- the error is displayed
- the operator must type `go`

If `go` is not provided:

- the workflow stops

If `go` is provided:

- the workflow continues

### 5. Step 4: DAG rule validation

This stage mainly calls:

- `run_dag_rule_validation()`
- `load_pipeline_config()`
- `stage_sources_for_airflow_check()`
- `RuleChecker.validate()`

Its purpose is to:

- continue validation only for syntax-valid files
- detect which files are Airflow DAG files
- apply configured rules from `rules`

It currently recognizes two DAG styles:

- classic `DAG(...)`
- `@dag` decorator

If rule validation fails:

- the error is displayed
- the operator must type `go`

If `go` is not provided:

- the workflow stops

If `go` is provided:

- packaging continues

### 6. Step 5: Create archive

This stage mainly calls:

- `derive_artifact_id()`
- `build_archive_name()`
- `create_archive()`

Its purpose is to:

- determine the final artifact name
- generate the zip archive
- calculate the SHA256 of the generated archive

### 7. Step 6: Prepare upload target

This stage mainly calls:

- `resolve_repository_url()`
- `build_default_upload_path()`
- `build_upload_url()`

Its purpose is to:

- determine the target Nexus repository
- determine the object path inside the repository
- construct the final upload URL

### 8. Step 7: Upload or finish in dry-run mode

If `--dry-run` is used:

- no real upload happens
- the script prints the summary only

If `--dry-run` is not used:

- `upload_archive()` is called
- the zip is uploaded to Nexus with HTTP PUT

### 9. Step 8: Print summary and log paths

At the end of the run, `main()` prints:

- archive path
- environment
- credentials file
- SHA256
- upload target

Finally, it calls:

- `report_log_file_locations()`

This prints:

- stdout log path
- stderr log path

### 10. One-line summary

The main call chain of `package_and_upload_dag.py` can be summarized as:

`main()`
→ `parse_args()`
→ `resolve_runtime_logging_settings()`
→ `ScriptOutputSession`
→ `_run_with_args()`
→ initialization
→ Python syntax validation
→ Airflow CLI validation
→ DAG rule validation
→ create zip
→ build upload target
→ upload or finish in dry-run mode
→ print log paths
