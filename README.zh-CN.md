## qin-cdc data sync
简单、高效、实时、稳定、可扩展、高可用、AI、开源

![LICENSE](https://img.shields.io/badge/license-AGPLv3%20-blue.svg)
![](https://img.shields.io/github/languages/top/sqlpub/qin-cdc)
![](https://img.shields.io/badge/build-release-brightgreen.svg)
[![Release](https://img.shields.io/github/release/sqlpub/qin-cdc.svg?style=flat-square)](https://github.com/sqlpub/qin-cdc/releases)

### 支持同步的数据库
#### 源
1. mysql
2. TODO sqlserver
3. TODO mongo
4. TODO oracle

#### 目的

1. mysql 
2. starrocks 
3. doris
4. kafka json
5. TODO kafka canal
6. kafka aliyun_dts_canal
7. 
### Quick start
#### 1. 安装
[Download](https://github.com/sqlpub/qin-cdc/releases/latest) the latest release and extract it.

#### 2. 创建同步账号
```sql
CREATE USER 'qin_cdc'@'%' IDENTIFIED BY 'xxxxxx';
GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'qin_cdc'@'%';
```
#### 3. 创建配置文件
mysql-to-starrocks.toml
```toml
# name 必填，多实例运行时保证全局唯一
name = "mysql2starrocks"

[input]
type = "mysql"

[input.config.source]
host = "127.0.0.1"
port = 3306
username = "root"
password = "root"

[input.config.source.options]
#start-gtid = "3ba13781-44eb-2157-88a5-0dc879ec2221:1-123456"
#server-id = 1001

[[transforms]]
type = "rename-column"
[transforms.config]
match-schema = "mysql_test"
match-table = "tb1"
columns = ["col_1", "col_2"]
rename-as = ["col_11", "col_22"]

[[transforms]]
type = "delete-column"
[transforms.config]
match-schema = "mysql_test"
match-table = "tb1"
columns = ["phone"]

[[transforms]]
type = "mapper-column"
[transforms.config]
match-schema = "mysql_test"
match-table = "tb1"
[transforms.config.mapper]
id = "user_id"
name = "nick_name"

[output]
type = "starrocks"

[output.config.target]
host = "127.0.0.1"
port = 9030
load-port = 8040 # support fe httpPort:8030 or be httpPort:8040
username = "root"
password = ""

[input.config.target.options]
batch-size = 1000
batch-interval-ms = 1000
parallel-workers = 4

[[output.config.routers]]
source-schema = "sysbenchts"
source-table = "sbtest1"
target-schema = "sr_test"
target-table = "ods_sbtest1"

[[output.config.routers]]
source-schema = "sysbenchts"
source-table = "sbtest2"
target-schema = "sr_test"
target-table = "ods_sbtest2"
[output.config.routers.columns-mapper]
source-columns = []
target-columns = []
```

#### 4. 查看帮助
```shell
[sr@ ~]$ ./qin-cdc-linux-xxxxxx -h
```

#### 5. 启动
```shell
[sr@ ~]$ ./qin-cdc-linux-xxxxxx -config mysql-to-starrocks.toml -log-file mysql2starrocks.log -level info -daemon
```

#### 6. 查看日志
```shell
[sr@ ~]$ tail -f mysql2starrocks.log
```

#### TODO AI功能点
1. 智能数据同步和迁移
2. 数据安全与监控
3. 智能化运维管理
4. 用户体验优化
5. 自动化数据映射
6. 自然语言处理