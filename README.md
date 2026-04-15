# airflow3-adoption

Bootstrap repository for adopting Apache Airflow 3 as a replacement for the existing Ab Initio platform.

This repository is intentionally minimal for the initial setup. We will add environment bootstrapping, Airflow installation, project structure, and company-specific customization in later steps.

## DAG 打包并上传 Nexus

针对 “4. DAG 发布与推广流水线”，仓库里新增了一个开发者可直接运行的脚本：

- 脚本路径：`scripts/dag_publish/package_and_upload_dag.py`
- 示例凭据文件：`configs/dag_publish/nexus_credentials.env.example`
- 程序固定读取的凭据文件：`configs/dag_publish/nexus_credentials.env`

### 凭据文件格式

先复制一份示例文件为实际凭据文件 `configs/dag_publish/nexus_credentials.env`，然后填入 Nexus 账号密码：

```bash
NEXUS_USERNAME=your_username
NEXUS_PASSWORD=your_password
NEXUS_TIMEOUT_SECONDS=60
NEXUS_INSECURE=false
```

说明：

- 脚本默认使用固定的凭据文件路径：`configs/dag_publish/nexus_credentials.env`
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

### 使用示例

将单个 DAG 目录打包并上传：

```bash
python3 scripts/dag_publish/package_and_upload_dag.py \
  dags/customer_sync \
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
  --artifact-id customer-sync \
  --version 1.0.0 \
  --dry-run
```

如果要生成你提供的那种公司命名风格，可以这样调用：

```bash
python3 scripts/dag_publish/package_and_upload_dag.py \
  dags/customer_sync \
  --artifact-id DAG_ID_RELEASE \
  --version 0001.4972.user_name
```

默认上传方式按 Nexus Raw 仓库的路径约定构建 URL，上传路径为：

```text
com/hsbc/gdt/et/fctm/1646753/CHG123456/<artifact-id>.<version>.zip
```

如果公司的 Nexus Raw 仓库路径有特殊要求，可以通过 `--upload-path` 显式指定仓库内路径。
