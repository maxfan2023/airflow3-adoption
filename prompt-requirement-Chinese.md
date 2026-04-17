总体的要求


- airflow使用airlfow3 而不是airfow2
- airflow会被多个不同的组来使用。需要做到区隔。比如data copy组开发一些DAG，并使用airflow来编排数据复制的任务。而另一个Regional on-demand data组开发另外一些DAG，分成打了US-DAG Tag DAG的和打了GLOBAL-DAG tag的DAG，airflow来编排数据处理的任务。Regional on-demand data组内有的人可以trigger US-DAG，而另一些人可以trigger GLOBAL-DAG。在不同的组之间需要做到隔离，不能互相干扰。
- airflow.cfg  中executor=celery，需要部署celery worker，使用postgresql 15 作为backend，使用redis 8 作为broker
- celery worker会使用多个账号来运行，不能只使用一个账号来运行所有的worker，这些worker都需要把日志写到一个共享的存储上，当然现在可以先使用本地存储（但是要处理好权限问题，需要保证账号a创建的目录，账号b可以继续更新等），后续可以考虑使用hdfs或者nfs来做共享存储。
- day 1的时候，worker和master节点都在一台机器上，但是后续需要支持worker和master节点分开部署在不同的机器上
- 一共有四个环境，分别是testing，dev，uat和prod，三个环境之间需要做到隔离，不能互相干扰。
    - testing环境是给管理员用的，用来测试airflow的功能和配置的正确性，管理员可以在这个环境里测试一些新的功能和配置，确保它们在正式环境里能够正常工作。
    - dev环境是给开发人员用的，开发人员可以在这个环境里开发和测试他们的DAG，确保它们能够正常工作。
    - uat环境是给业务人员用的，业务人员可以在这个环境里测试他们的DAG，确保它们能够满足业务需求。
    - prod环境是正式环境，所有的DAG都需要经过测试和验证才能部署到这个环境里，确保它们能够正常工作，并且不会对业务造成影响
    - testing和dev环境是在一台服务器上的
    - uat是在另一台服务器上的
    - prod是在第三台服务器上的
- DAG的部署方式是开发人员把DAG或者DAG的zip包上传到公司内部的nexus仓库里，然后有一个pipeline会把这些DAG从nexus仓库里拉取出来，放到airflow的landing zone里，在这个过程中会有一些预处理的步骤，比如代码质量检查，语法检查，根据TAG来源给DAG打标签等等，预处理完成之后，这些DAG会被自动部署到airflow的dags文件夹里，准备好被airflow调度和执行了。 这个流程适用于dev，uat和prod环境，testing环境不需要这个流程，管理员可以直接把DAG放到testing环境的airflow的dags文件夹里来测试。
- 每个服务器上都会配置一套airflow+postgresql+redis+celery worker的环境，四套环境之间需要做到隔离，不能互相干扰。
- 需要有一个监控系统来监控airflow的运行状态，及时发现和处理问题，确保airflow能够稳定运行。
- 需要有一个日志系统来收集和分析airflow的日志，帮助我们了解airflow的运行情况，及时发现和处理问题，确保airflow能够稳定运行。
- 需要有一个权限系统来管理airflow的用户和权限，确保不同的用户只能访问他们有权限访问的资源，保护airflow的安全性。
- 需要有一个备份和恢复系统来备份airflow的数据和配置，确保在发生故障时能够快速恢复airflow的运行，减少对业务的影响。
- 需要有一个升级和维护系统来升级airflow的版本和维护airflow的运行，确保airflow能够持续稳定地运行，满足业务的需求

- Regional on-demand data组主要是用借由airflow来运行一些hadoop HDFS，hive相关的命令来进行数据处理的任务，data copy组主要是用airflow来运行一些数据复制的命令来进行数据复制的任务。不同的组之间需要做到隔离，不能互相干扰。

- 安装airflow3的方法是：
    - 首先需要在testing，dev环境里创建miniconda的环境，在这个环境里安装好airflow3的二进制包的依赖，并且把这些依赖打包成一个tar包，上传到公司内部的nexus仓库里，然后在uat和prod环境里从nexus仓库里拉取这个tar包，解压之后就可以使用了。这个方法可以保证三个环境里安装的airflow3的版本和依赖是一致的，避免了版本不一致导致的问题。
    - 激活了miniconda环境后，就可以安装airflow3了，不过方法也是从nexus仓库里拉取已经打包好的二进制包，然后解压安装。
    - 这个方法的好处是可以保证三个环境里安装的airflow3的版本和依赖是一致的，避免了版本不一致导致的问题，同时也可以避免在每个环境里都需要安装airflow3的依赖，节省了时间和资源。
    - 只有testing和dev环境的安装是在线的，需要连接公司内部的repositryy来拉取依赖，uat和prod环境的安装是离线的，需要从nexus仓库里拉取已经打包好的二进制包来安装，这样可以保证uat和prod环境的安全性，避免了在线安装可能带来的安全风险。
    - 如果将来要升级airflow3的版本了，只需要在testing环境里安装新的版本，测试没有问题之后，把新的版本打包成tar包上传到nexus仓库里，然后在uat和prod环境里拉取新的tar包来安装就可以了，这样可以保证三个环境里安装的airflow3的版本和依赖是一致的，避免了版本不一致导致的问题，同时也可以避免在每个环境里都需要安装airflow3的依赖，节省了时间和资源。





