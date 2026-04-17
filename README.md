# airflow3-adoption

Bootstrap repository for adopting Apache Airflow 3 as a replacement for the existing Ab Initio platform.

This repository is intentionally minimal for the initial setup. We will add environment bootstrapping, Airflow installation, project structure, and company-specific customization in later steps.

## DAG 打包并上传 Nexus

针对 “4. DAG 发布与推广流水线”，仓库里新增了一个开发者可直接运行的脚本：

- 脚本路径：`scripts/dag_publish/package_and_upload_dag.py`
- 示例凭据文件：
  `configs/dag_publish/nexus_credentials.dev.env.example`
  `configs/dag_publish/nexus_credentials.uat.env.example`
  `configs/dag_publish/nexus_credentials.prod.env.example`
- 程序支持 `--environment dev|uat|prod`，会优先查找对应环境的凭据文件
- 打包前会先做 Python 语法检查，并按对应环境的 `deploy_pipeline.<environment>.json` 执行一次 Airflow CLI 校验
- 终端输出会按步骤分段显示，并带 emoji 标识当前阶段
- 每次调用都会同时写一份标准输出日志和一份错误日志

### 凭据文件格式

建议按环境分别复制示例文件为实际凭据文件：

- `configs/dag_publish/nexus_credentials.dev.env`
- `configs/dag_publish/nexus_credentials.uat.env`
- `configs/dag_publish/nexus_credentials.prod.env`

然后填入各自环境的 Nexus 账号密码：

```bash
NEXUS_USERNAME=your_username
NEXUS_PASSWORD=your_password
NEXUS_TIMEOUT_SECONDS=60
NEXUS_INSECURE=false
```

说明：

- 脚本会按下面的顺序查找凭据文件：
  `configs/dag_publish/nexus_credentials.<environment>.env` 相对 repo 根目录
  `configs/dag_publish/nexus_credentials.env` 相对 repo 根目录
  `configs/dag_publish/nexus_credentials.<environment>.env` 相对脚本目录
  `configs/dag_publish/nexus_credentials.env` 相对脚本目录
  `nexus_credentials.<environment>.env` 与脚本同目录
  `nexus_credentials.env` 与脚本同目录
- 凭据值支持不加引号、普通引号 `"value"` / `'value'`，也兼容中文弯引号 `“value”`
- 默认 Nexus 仓库根路径是：
  `https://nexus302.systems.uk.hsbc:8081/nexus/repository/raw-alm-uat_n3p`
- 默认 Nexus 仓库内前缀路径是：
  `com/hsbc/gdt/et/fctm/1646753/CHG123456`
- 如果将来需要覆盖默认仓库地址，可以在凭据文件里增加 `NEXUS_REPOSITORY_URL`
- 如果将来需要覆盖默认路径前缀，可以在凭据文件里增加 `NEXUS_PATH_PREFIX`
- 如果公司环境更适合拆开配置，也可以改用 `NEXUS_BASE_URL` + `NEXUS_REPOSITORY`
- 实际凭据文件已被 `.gitignore` 忽略，不会被提交
- 脚本只使用 Python 标准库，不需要额外安装第三方依赖包
- 脚本已经避免使用 Python 3.9 专属语法，适合 RHEL8 常见的 `python3` 环境
- 如果 `deploy_pipeline.<environment>.json` 里配置了 `imports.activation_command`，脚本会先激活 Miniconda/Conda 环境，再执行 `airflow db migrate` 和 `airflow dags list-import-errors -l -o json`
- Python 语法检查会收集所有 `.py` 文件中的语法错误一起展示；如果你输入 `go`，脚本会忽略这些语法问题继续打包，并且只对语法正确的 Python 文件继续做 Airflow CLI 校验
- Airflow CLI 校验发现 import error / 校验环境异常时，脚本会先把错误打印到终端；只有在提示后输入 `go`，才会忽略这些问题继续打包
- Airflow CLI 校验通过后，脚本还会按 `deploy_pipeline.<environment>.json` 里的 `rules` 做 DAG 规则检查；默认会检查 Airflow DAG 文件里是否定义了 `GDT_ET_FEED_SOURCE`，以及它的值是否属于配置允许列表
- 日志目录和保留天数由 `deploy_pipeline.<environment>.json` 里的 `logging.directory` 与 `logging.retention_days` 控制；脚本启动时会自动清理超过保留天数的旧日志
- Airflow CLI 校验使用的临时目录和环境变量由 `deploy_pipeline.<environment>.json` 里的 `airflow_cli.temp_root` 与 `airflow_cli.env` 控制，不再固定写死到系统 `/tmp`

### 使用示例

将单个 DAG 目录打包并上传：

```bash
python3 scripts/dag_publish/package_and_upload_dag.py \
  dags/customer_sync \
  --environment dev \
  --artifact-id customer-sync \
  --version 1.0.0
```

将多个文件一起打包并上传：

```bash
python3 scripts/dag_publish/package_and_upload_dag.py \
  dags/customer_sync.py dags/common \
  --artifact-id customer-sync-bundle \
  --version 1.0.0
```

只验证打包结果和上传目标，不真正上传：

```bash
python3 scripts/dag_publish/package_and_upload_dag.py \
  dags/customer_sync \
  --environment uat \
  --artifact-id customer-sync \
  --version 1.0.0 \
  --dry-run
```

查看每个步骤和执行命令的调试输出：

```bash
python3 scripts/dag_publish/package_and_upload_dag.py \
  dags/customer_sync \
  --environment dev \
  --artifact-id customer-sync \
  --version 1.0.0 \
  --dry-run \
  --debug
```

如果要生成你提供的那种公司命名风格，可以这样调用：

```bash
python3 scripts/dag_publish/package_and_upload_dag.py \
  dags/customer_sync \
  --environment prod \
  --artifact-id DAG_ID_RELEASE \
  --version 0001.4972.user_name
```

默认上传方式按 Nexus Raw 仓库的路径约定构建 URL，上传路径为：

```text
com/hsbc/gdt/et/fctm/1646753/CHG123456/<artifact-id>.<version>.zip
```

如果公司的 Nexus Raw 仓库路径有特殊要求，可以通过 `--upload-path` 显式指定仓库内路径。

如果要显式指定用于 Airflow CLI 校验的部署配置，可以增加 `--config`，例如：

```bash
python3 scripts/dag_publish/package_and_upload_dag.py \
  dags/customer_sync \
  --environment dev \
  --config configs/dag_publish/deploy_pipeline.dev.json \
  --artifact-id customer-sync \
  --version 1.0.0 \
  --dry-run
```

## DAG 从 Nexus 落地并发布到 Airflow dags

仓库里新增了一个部署侧脚本，用来完成以下流水线步骤：

- 下载或接收 DAG 压缩包
- checksum 计算/校验
- 安全解包
- Python 语法检查
- import 检查
- DAG `dag_id` / `queue` / 顶层变量 规则检查
- 根据顶层 `source` 变量改写受管 tags
- 落地到 landing zone
- 以 backup + rename + rollback 的方式发布到 `dags`

### 主要文件

- 脚本入口：`scripts/dag_publish/deploy_dag_from_nexus.py`
- 步骤模块：`scripts/dag_publish/deploy_steps/`
- 本地通用配置：`configs/dag_publish/deploy_pipeline.json`
- 环境专属配置：
  `configs/dag_publish/deploy_pipeline.dev.json`
  `configs/dag_publish/deploy_pipeline.uat.json`
  `configs/dag_publish/deploy_pipeline.prod.json`
- 环境专属凭据：
  `configs/dag_publish/nexus_credentials.dev.env`
  `configs/dag_publish/nexus_credentials.uat.env`
  `configs/dag_publish/nexus_credentials.prod.env`

### 使用示例

从本地 zip 做完整校验并发布：

```bash
python3 scripts/dag_publish/deploy_dag_from_nexus.py \
  --environment dev \
  --archive-file build/dag_packages/customer-sync-1.0.0.zip
```

只做校验，不真正写 landing / dags：

```bash
python3 scripts/dag_publish/deploy_dag_from_nexus.py \
  --environment uat \
  --archive-file build/dag_packages/customer-sync-1.0.0.zip \
  --dry-run
```

从 Nexus 仓库内路径下载并发布：

```bash
python3 scripts/dag_publish/deploy_dag_from_nexus.py \
  --environment prod \
  --artifact-path com/hsbc/gdt/et/fctm/1646753/CHG123456/customer-sync-1.0.0.zip
```

### 配置说明

部署脚本默认会优先读取 `deploy_pipeline.<environment>.json`；如果显式传 `--config`，则使用指定文件。

`configs/dag_publish/deploy_pipeline.<environment>.json` 中可以配置：

- `paths`: `working_root`、`landing_root`、`dags_root`、`backup_root`
- `nexus`: `repository_url`、`timeout_seconds`、`verify_tls`
- `archive`: 允许的压缩包后缀，以及是否强制单顶层目录
- `checksum`: `compute_only`、`sidecar_file`、`cli_value`
- `logging`: `directory`、`retention_days`
- `airflow_cli`: `temp_root`，以及 `env` 中的 `AIRFLOW__CORE__DAGS_FOLDER`、`AIRFLOW__CORE__LOAD_EXAMPLES`、`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`
- `imports`: `extra_pythonpath`、`shell_executable`、`activation_command`、`python_executable`、`timeout_seconds`
- `tagging`: `source` 变量名、US source 列表、受管 tags
- `rules`: DAG 命名规则、queue 规则，以及 DAG 顶层变量规则

例如：

```json
"airflow_cli": {
  "temp_root": "build/dag_deploy/dev/airflow_cli",
  "env": {
    "AIRFLOW__CORE__DAGS_FOLDER": "{session_root}/staging",
    "AIRFLOW__CORE__LOAD_EXAMPLES": false,
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "sqlite:///{session_root}/airflow_metadata.db",
    "PYTHONDONTWRITEBYTECODE": "1"
  }
}
```

`rules.dag_variable_rules` 支持把 DAG 顶层变量检查做成纯配置，例如：

```json
"rules": {
  "name_rules": {
    "enabled": false,
    "allow_patterns": [],
    "deny_patterns": []
  },
  "queue_rules": {
    "enabled": false,
    "allow_patterns": [],
    "deny_patterns": []
  },
  "dag_variable_rules": [
    {
      "name": "GDT_ET_FEED_SOURCE",
      "required": true,
      "allowed_values": ["camp-us", "ucm", "norkom", "na"]
    }
  ]
}
```

默认行为是：

- 只对检测到 `DAG(...)` 定义的 Airflow DAG 文件生效
- 普通 Python 文件不会因为缺少这些变量而报错
- 变量必须定义在模块顶层，并且必须是字符串字面量，才能被自动校验

其中 `{session_root}` 会在每次运行时展开成该次校验专用的临时目录。

如果公司环境需要先激活 Miniconda/Conda 才能拿到 Airflow 运行时，请在对应环境配置里填写：

```json
"imports": {
  "extra_pythonpath": [],
  "shell_executable": "/bin/bash",
  "activation_command": "source /path/to/miniconda3/etc/profile.d/conda.sh && conda activate airflow3_dev",
  "python_executable": "python",
  "timeout_seconds": 300
}
```

这样即使 `deploy_dag_from_nexus.py` 本身不是在 Airflow 环境里启动，`import` 检查也会自动进入目标 conda 环境后再执行。

### 当前约束

- 只接受解包后“单一顶层目录”的压缩包
- tag 改写只支持经典 `with DAG(...)` 和 `dag = DAG(...)`
- `source` 必须是文件顶层字符串常量
- `tags` 如果存在，必须是字符串字面量 list/tuple
