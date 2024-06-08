## qin-cdc data sync
Simple, efficient, real-time, stable, scalable, highly available, AI, open source

![LICENSE](https://img.shields.io/badge/license-AGPLv3%20-blue.svg)
![](https://img.shields.io/github/languages/top/sqlpub/qin-cdc)
[![Release](https://img.shields.io/github/release/sqlpub/qin-cdc.svg?style=flat-square)](https://github.com/sqlpub/qin-cdc/releases)

English | [简体中文](README.zh-CN.md)

### Support sync database
#### source
1. mysql
2. TODO sqlserver
3. TODO mongo

#### target

1. mysql 
2. starrocks 
3. TODO doris
4. TODO kafka json
5. TODO kafka canal
6. TODO kafka aliyun_dts_canal

### Quick start
#### 1. Install
[Download](https://github.com/sqlpub/qin-cdc/releases/latest) the latest release and extract it.

#### 2. Create a synchronization account
```sql
CREATE USER 'qin_cdc'@'%' IDENTIFIED BY 'xxxxxx';
GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'qin_cdc'@'%';
```
#### 3. Create a configuration file
mysql-to-starrocks.toml
```toml
# name is required and must be globally unique when multiple instances are running
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
source-schema = "mysql_test"
source-table = "tb1"
target-schema = "sr_test"
target-table = "ods_tb1"

[[output.config.routers]]
source-schema = "mysql_test"
source-table = "tb2"
target-schema = "sr_test"
target-table = "ods_tb2"
# mapper column, optional, if empty, same name mapping
# [output.config.routers.columns-mapper]
# source-columns = []
# target-columns = []
```

#### 4. View Help
```shell
[sr@ ~]$ ./qin-cdc-linux-xxxxxx -h
```

#### 5. Startup
```shell
[sr@ ~]$ ./qin-cdc-linux-xxxxxx -config mysql-to-starrocks.toml -log-file mysql2starrocks.log -level info -daemon
```

#### 6. View logs
```shell
[sr@ ~]$ tail -f mysql2starrocks.log
```

#### TODO AI functional points
1. Intelligent data synchronization and migration
2. Data security and monitoring
3. Intelligent operation and maintenance management
4. User experience optimization
5. Automated data mapping
6. Natural language processing
