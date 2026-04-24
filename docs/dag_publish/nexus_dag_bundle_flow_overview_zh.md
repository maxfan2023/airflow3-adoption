# Nexus DAG Bundle 流程说明

## 概要

本文说明两件事：

1. DAG 如何被打包并上传到 Nexus。
2. Airflow 如何从 Nexus 下载 DAG 包，并缓存在本地使用。

用比较简单的话说，这套流程做的是：

1. 先检查 DAG 包是否基本可用。
2. 再把 DAG 打成 ZIP 包并上传到 Nexus。
3. 同时上传校验文件和发布元数据。
4. Airflow 运行时读取 Nexus 上的元数据，下载正确版本的 DAG 包。
5. Airflow 校验包没有被篡改或损坏后，解压到本地缓存目录。
6. 后续 Airflow 从本地缓存目录读取 DAG。

这样可以让 DAG 发布更可追踪、更稳定，也更容易审计。

## 适用读者

本文面向不熟悉代码实现的人，重点解释流程做了什么、为什么这样做、以及大概如何实现。

## 相关流程图

### 上传 DAG 到 Nexus

![上传 DAG 到 Nexus](flowcharts/dag-upload-to-nexus-flow.drawio.svg)

可编辑源文件：[dag-upload-to-nexus-flow.drawio](flowcharts/dag-upload-to-nexus-flow.drawio)

### Airflow 从 Nexus 下载并缓存 DAG

![Airflow 从 Nexus 下载并缓存 DAG](flowcharts/airflow-nexus-dag-cache-flow.drawio.svg)

可编辑源文件：[airflow-nexus-dag-cache-flow.drawio](flowcharts/airflow-nexus-dag-cache-flow.drawio)

## 业务目标

这套设计的目标，是把 Airflow DAG 发布流程接入 Nexus，让 Nexus 作为统一的制品仓库。

这样做的好处包括：

- DAG 包集中存放在 Nexus 中。
- 每次发布都有版本、变更单、源码提交号、发布时间、发布人等记录。
- Airflow 可以用统一方式从 Nexus 获取 DAG。
- 通过 checksum 校验，降低运行损坏包或错误包的风险。
- 下载后会缓存在 Airflow 节点本地，减少重复访问 Nexus。

## 流程一：上传 DAG 包到 Nexus

上传流程主要由 `scripts/dag_publish/package_and_upload_dag.py` 实现。

整体过程如下。

### 1. 解析输入

发布人员运行上传脚本，并传入 DAG 源目录或文件、artifact id、版本号和目标环境。

脚本会解析这些信息：

- 目标环境，例如 `dev`、`uat`、`prod`
- Nexus 凭据文件
- 需要打包的源文件
- artifact id 和 bundle name
- 发布版本号

### 2. 执行发布前检查

真正上传之前，脚本会先做检查。

检查内容包括：

- Python 语法检查，确保 `.py` 文件可以被解析。
- Airflow 导入检查，使用临时 Airflow 环境验证 DAG 是否有导入错误。
- DAG 规则检查，例如 DAG 命名规则、queue 规则、必需的顶层变量等。

如果检查发现问题，脚本会展示问题。某些情况下，发布人员必须明确确认继续，流程才会往下走。

### 3. 创建 ZIP 包

检查完成后，脚本会创建一个 ZIP 包。

ZIP 包会保留预期的目录结构，这样 Airflow 后续解压和读取 DAG 时能保持一致。

脚本还会计算 ZIP 包的 SHA256 checksum。可以把 checksum 理解成这个包的“数字指纹”，后续 Airflow 下载包之后会用它确认文件没有损坏或被替换。

### 4. 生成发布元数据

脚本会生成几类元数据。

这些元数据用于描述本次发布，也用于 Airflow 后续查找正确的 DAG 包：

- **版本记录**：描述某个具体版本的 DAG bundle。
- **发布记录**：记录发布时间、变更单、发布人等审计信息。
- **latest manifest**：指向当前最新可用版本。
- **checksum sidecar**：保存 ZIP 包对应的 SHA256 checksum。

### 5. 上传到 Nexus

如果不是 dry-run 模式，脚本会把以下对象上传到 Nexus：

- DAG bundle ZIP 文件
- `.sha256` checksum 文件
- 版本记录 JSON
- 发布记录 JSON
- latest manifest JSON

如果是 dry-run 模式，脚本只会生成包并打印目标 Nexus 地址，不会真正上传。

## 流程二：Airflow 从 Nexus 下载并缓存 DAG

Airflow 运行时的下载和缓存逻辑主要由 `my_company/airflow_bundles/nexus.py` 实现。

Airflow 使用 `NexusDagBundle` 来找到正确的 DAG 包，下载它，校验它，并放到本地缓存目录。

### 1. Airflow 请求 DAG 路径

当 Airflow 需要解析 DAG 时，会向 bundle 实现请求一个本地路径。

bundle 会先初始化缓存目录和锁文件。锁文件的作用是避免多个 Airflow 进程同时下载、解压同一个包。

### 2. 确定要使用的版本

bundle 支持两种方式：

- **使用最新版本**：读取 Nexus 上的 `latest.json`。
- **使用指定版本**：读取 Nexus 上对应的版本记录。

manifest 会告诉 Airflow 应该下载哪个 ZIP 包，以及这个 ZIP 包应该对应哪个 checksum。

### 3. 优先复用本地缓存

下载之前，bundle 会先检查本地缓存里是否已经有目标版本。

如果本地缓存有效，Airflow 会直接使用缓存，不再重复下载。

### 4. 下载并校验

如果本地没有可用缓存，bundle 会从 Nexus 下载：

- ZIP 包
- `.sha256` checksum 文件

然后执行两层校验：

- checksum 文件里的值必须和 manifest 中记录的值一致。
- 实际下载的 ZIP 包计算出的 checksum 也必须和 manifest 中记录的值一致。

如果校验不通过，Airflow 会拒绝使用这个包。

### 5. 安全解压

ZIP 包不会直接解压到最终目录，而是先解压到临时目录。

解压时会检查：

- ZIP 包中必须只有一个顶层目录。
- ZIP 包不能包含危险路径，例如绝对路径或试图跳出解压目录的路径。
- 不支持不安全的符号链接条目。

检查通过后，才会把解压结果移动到正式缓存目录。

### 6. 更新本地指针

缓存成功后，bundle 会写入本地元数据：

- `.bundle-version.json`：记录当前缓存版本的信息。
- `current.json`：指向当前正在使用的版本。

这样 Airflow 后续运行时可以快速找到本地缓存，不需要每次都重新下载。

### 7. 失败时的处理

如果 Airflow 无法读取 Nexus，或者最新版本下载/校验失败，但本地已有一个有效的当前版本，Airflow 可以继续使用这个有效缓存。

如果本地也没有有效缓存，就会报错，而不是运行一个未经验证的包。

## 关键实现文件

| 文件 | 作用 |
| --- | --- |
| `scripts/dag_publish/package_and_upload_dag.py` | 打包 DAG、执行校验、生成元数据，并上传发布对象到 Nexus。 |
| `my_company/airflow_bundles/nexus.py` | Airflow 侧的 Nexus bundle 实现，负责读取元数据、下载、校验、解压和缓存。 |
| `scripts/dag_publish/common.py` | 提供环境、路径、发布记录路径、Git commit 等共享工具方法。 |
| `scripts/dag_publish/prewarm_dag_bundle_cache.py` | 可在 Airflow 节点上提前把 bundle 下载到本地缓存。 |
| `docs/dag_publish/flowcharts/` | 保存 draw.io 可编辑流程图和导出的 SVG 图片。 |

## 关键控制点

这套实现包含以下控制点：

- **上传前校验**：检查 Python 语法、Airflow 导入结果和 DAG 规则。
- **发布可追踪**：记录版本、变更单、源码提交号、发布时间和发布人。
- **checksum 校验**：Airflow 下载后会校验文件完整性。
- **安全解压**：避免 ZIP 包中包含不安全路径。
- **缓存锁**：避免多个进程同时写同一个缓存目录。
- **失败回退**：新版本不可用时，可以继续使用本地已有的有效版本。
- **dry-run 支持**：正式上传前可以预览包和 Nexus 目标路径。

## 预期效果

这套流程让 DAG 发布从手工文件移动，变成一个更标准、更可审计的制品发布流程。

发布人员可以通过脚本把 DAG 包和发布信息上传到 Nexus。Airflow 再从 Nexus 获取正确版本，完成校验和本地缓存后运行。

整体上，它降低了发布过程中的人为错误，提高了 DAG 发布的可追踪性和运行时稳定性。
