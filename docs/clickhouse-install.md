# ClickHouse 安装教程（Alibaba Cloud Linux 3.2104 LTS）

> 目标：在 hadoop1 单机装一套 ClickHouse 26.8 LTS，给 mes-production-monitor 的
> DWD/DWS/告警作业当分析存储。每一步都有命令，从零到能跑 `01-ddl.sql`。

---

## 0. 为什么是单机、为什么是 26.8 LTS

- 本项目所有表都是普通 `MergeTree` / `ReplacingMergeTree`，没有任何 `Replicated*` /
  `Distributed` 引擎，**不需要 ClickHouse 集群**（集群要额外 Zookeeper + 全表改 Replicated）。
- 数据量是 MES 报工明细，单机绰绰有余。
- `26.8` 是 LTS 长期支持版，DDL 与 JDBC 驱动（`clickhouse-jdbc 0.6.0`）都按这个版本验证过。
- JDBC 走的是 `jdbc:ch://` = **HTTP 协议 8123 端口**，不是 native 9001。所以放行端口时 8123 才是关键。

---

## 1. 前置检查

```bash
# 1) 确认发行版与版本（应为 Alibaba Cloud Linux 3）
cat /etc/os-release

# 2) 确认 CPU 架构（x86_64 / aarch64，影响后续包选择）
uname -m

# 3) 确认 SELinux 状态（ALinux3 默认 Enforcing，后面若起不来要先查这里）
getenforce

# 4) 确认 8123/9001 未被占用
ss -tlnp | grep -E '8123|9001' || echo "端口空闲"
```

---

## 2. 安装（官方 RPM 仓库）

Alibaba Cloud Linux 3 是 RHEL8/CentOS8 体系，包管理器用 `dnf`。

```bash
# 1) 装 dnf 插件（提供 config-manager）
sudo dnf install -y dnf-plugins-core

# 2) 加官方 ClickHouse 仓库
sudo dnf config-manager --add-repo https://packages.clickhouse.com/rpm/clickhouse.repo

# 3) 刷新并查看可用版本（确认 26.8 在列）
sudo dnf makecache
sudo dnf list --showduplicates clickhouse-server | tail -30
```

**两种装法，二选一：**

```bash
# ---- 方式 A：装最新稳定版（学习用最省事）----
sudo dnf install -y clickhouse-server clickhouse-client

# ---- 方式 B：精确 pin 到 26.8 LTS（推荐，和线上版本一致）----
# 从上面 list 结果里抄一个 26.8.x.y 的具体版本号，例如 26.8.26.12（以 list 实际输出为准）
sudo dnf install -y \
  clickhouse-common-static-26.8.26.12 \
  clickhouse-server-26.8.26.12 \
  clickhouse-client-26.8.26.12
```

> 说明：`clickhouse-server` 依赖 `clickhouse-common-static`（真正的二进制，很大）。
> 直接 `install clickhouse-server-<ver>` 一般也会自动带对版本，但显式写全三个更稳，避免混版本。

---

## 3. 基础配置

配置文件在 `/etc/clickhouse-server/`。主要改三处：监听地址、时区、业务用户。

### 3.1 允许远程连接（listen_host）

```bash
sudo cp /etc/clickhouse-server/config.xml /etc/clickhouse-server/config.xml.bak
sudo vi /etc/clickhouse-server/config.xml
```

找到下面这行（默认是被注释掉的，只监听 localhost）：

```xml
<!-- <listen_host>::</listen_host> -->
```

改成（放开注释并监听所有网卡）：

```xml
<listen_host>0.0.0.0</listen_host>
```

> 不改这个，Flink TaskManager 从别的节点连 8123 会被拒——这是远程连不上的头号原因。

### 3.2 时区设为 Asia/Shanghai

同一个 `config.xml`，找到（或新增）`<timezone>`，改成：

```xml
<timezone>Asia/Shanghai</timezone>
```

> 与 Java 侧、docker 镜像的 `TZ=Asia/Shanghai` 保持一致。`DateTime` 列和
> `toYYYYMMDD()` 分区都依赖服务器时区，不统一会出现 8 小时偏移。

### 3.3 配置 default 用户（密码 + 远程访问）

本项目应用侧直接用 `default` 用户。裸装下 `default` 默认**空密码、仅 localhost**，要给它设密码并放开远程登录。

```bash
sudo mkdir -p /etc/clickhouse-server/users.d
sudo vi /etc/clickhouse-server/users.d/default.xml
```

内容：

```xml
<clickhouse>
  <users>
    <default>
      <password>TMClickhouse0204</password>
      <networks>
        <ip>::/0</ip>
      </networks>
      <profile>default</profile>
      <quota>default</quota>
    </default>
  </users>
</clickhouse>
```

- `<password>` 里写的是**明文**，ClickHouse 读到后会自己存成哈希。
- `<networks><ip>::/0</ip></networks>` = 允许来自任意 IP 的 `default` 登录；不写默认只允许 localhost。
- `default` 是 ClickHouse 超级管理员用户，学习/内网环境直接用没问题；生产若要收权限再另建受限业务用户。

### 3.4 调大文件句柄上限（nofile）

```bash
sudo mkdir -p /etc/systemd/system/clickhouse-server.service.d
sudo tee /etc/systemd/system/clickhouse-server.service.d/override.conf > /dev/null <<'EOF'
[Service]
LimitNOFILE=262144
EOF
sudo systemctl daemon-reload
```

> ClickHouse 大量并发打开文件做 merge，262144 是官方建议值（仓库 docker 里也设了这个）。

---

## 4. 启动 + 开机自启

```bash
sudo systemctl start clickhouse-server
sudo systemctl enable clickhouse-server
sudo systemctl status clickhouse-server --no-pager
```

若起不来，先看日志再往下走：

```bash
sudo tail -n 100 /var/log/clickhouse-server/clickhouse-server.log
# 或
sudo journalctl -u clickhouse-server -n 100 --no-pager
```

**SELinux 提示**：如果日志报权限/端口错误且 `getenforce` 是 `Enforcing`，
先临时放行验证是不是它的问题：

```bash
sudo setenforce 0      # 临时 permissive，重启失效
```

确认是 SELinux 后，再决定永久改（`/etc/selinux/config` 改 `permissive`）还是写策略。

---

## 5. 防火墙 + 云安全组（两层都要放行）

### 5.1 系统防火墙（firewalld）

```bash
sudo firewall-cmd --permanent --add-port=8123/tcp   # HTTP，JDBC 用这个，必须
sudo firewall-cmd --permanent --add-port=9001/tcp   # native，仅本机 clickhouse-client 用，可不对外
sudo firewall-cmd --reload
sudo firewall-cmd --list-ports
```

> 如果 `firewall-cmd` 提示 `FirewallD is not running`，说明系统层没开防火墙，
> 端口是否可达就完全由云安全组决定，跳过本节即可。

### 5.2 阿里云安全组（在 ECS 控制台操作，不是命令行）

在 hadoop1 实例所在的安全组，**入方向**加一条规则：

| 方向 | 协议 | 端口 | 授权对象 | 说明 |
|---|---|---|---|---|
| 入方向 | TCP | 8123 | Flink 集群内网网段（如 `172.16.0.0/12`） | JDBC 连接 |

> 只放行内网网段即可，不必 `0.0.0.0/0` 公网暴露。9001 若只在服务器本机用，安全组不用开。

---

## 6. 验证安装

```bash
# 1) 版本号
clickhouse-client --query "SELECT version()"

# 2) HTTP 存活检查（期望返回 Ok.）
curl http://127.0.0.1:8123/ping

# 3) 用 default 用户登录（成功则能列出 default / information_schema 等库）
clickhouse-client -u default --password TMClickhouse0204 \
  --query "SHOW DATABASES"

# 4) 从同网段另一台机器验证远程可达（可选）
curl http://hadoop1:8123/ping
```

---

## 7. 初始化本项目 DDL

把仓库里的 `docker/clickhouse/init/01-ddl.sql` 传到 hadoop1，然后：

```bash
# 建库 + 建表 + 建视图（--multiquery 允许一条命令执行多段 SQL）
clickhouse-client -u default --password TMClickhouse0204 \
  --multiquery < docker/clickhouse/init/01-ddl.sql

# 验证：应能看到 5 张表 + 5 个视图
clickhouse-client -u default --password TMClickhouse0204 \
  --query "SHOW TABLES FROM mes_production_monitor"
```

预期表：`dwd_mes_report`、`dwd_mes_report_deleted_audit`、`dws_report_1min`、
`dws_order_achievement`、`ads_alert`；视图：`v_` 前缀的对应 5 个。

---

## 8. 接入本项目（改配置）

三份 `properties` 里 ClickHouse URL 已统一为 `hadoop1:8123`。作业跑在 YARN 上时
`localhost` 会指向 TaskManager 所在节点而非 CK 节点，所以不能写 localhost。涉及文件：

- `mes-production-monitor-dwd/src/main/resources/dwd.properties`
- `mes-production-monitor-dws/src/main/resources/dws.properties`
- `mes-production-monitor-alert/src/main/resources/alert.properties`

最终配置（用 hadoop1 内网 IP 更稳，避免 hosts 解析问题）：

```properties
clickhouse.jdbc.url = jdbc:clickhouse:http://hadoop1:8123/mes_production_monitor?compress=false
```

`username/password` 用 `default / TMClickhouse0204`。

---

## 9. 常用运维命令速查

```bash
# 状态 / 重启
sudo systemctl status clickhouse-server --no-pager
sudo systemctl restart clickhouse-server

# 实时看日志
sudo tail -f /var/log/clickhouse-server/clickhouse-server.log

# 进客户端（本机 native）
clickhouse-client -u default --password TMClickhouse0204

# 数据目录（默认）
ls /var/lib/clickhouse/

# 卸载（学习重装时用）
sudo systemctl stop clickhouse-server
sudo dnf remove -y 'clickhouse-*'
# 数据目录不会自动删，重装前如需清空：
sudo rm -rf /var/lib/clickhouse/
```

---

## 10. 常见坑速查

| 现象 | 原因 | 处理 |
|---|---|---|
| 远程 curl 8123 超时 | `listen_host` 还是 localhost | 改 3.1，重启 |
| 本机能连、别的机器不能连 | 云安全组没放行 8123 | 加安全组规则（5.2） |
| 起不来，日志报 SELinux/权限 | ALinux3 SELinux Enforcing | `setenforce 0` 验证后处理 |
| JDBC 连上但时间差 8 小时 | 服务器时区是 UTC | 设 `<timezone>Asia/Shanghai</timezone>` |
| `Too many open files` | nofile 太小 | 按 3.4 调 262144 |
| 用户认证失败 | default 用户没设密码 / 没放开远程 | 按 3.3 配置 |
