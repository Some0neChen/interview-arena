# 交易服务验收脚本

以下命令在 `src/InterviewArena` 目录执行。先启动 MySQL，保持 13401 空闲，准备好已配置 Boost 1.88 的 `build`；脚本不停止已有服务，不运行 ASAN/TSAN。

## 普通四表回归

```bash
bash test/script/submit_order_acceptance.sh
```

71 项检查覆盖四表下单、幂等、参数拒绝、账户更新故障、派发前撤单与撤单冲突。默认使用 `arena` 和应用账号 `a`，要求四张业务表已经存在。只创建并清理自己的临时行，不修改表结构、不重置自增计数。

`TRADE_MYSQL_DATABASE` 可以覆盖数据库名；服务端和验收程序读取同一个变量。没有设置或为空仍使用 `arena`。不要拿这份普通回归代替下面的下单 INSERT 故障专项。

## M1.5 下单写入故障专项

```bash
bash test/script/submit_order_write_fault_acceptance.sh
```

脚本先用管理账号创建独立的 `tradewritefault...` 库，导入项目四份建表文件，并仅为这个库临时授权现有应用账号。测试前通过 GetAccount 读取独立库的标记账户，确认服务端没有错误地连回 `arena`，随后运行原 71 项回归与 27 项专项。

| 故障 | 构造方式 | 检查结果 |
|---|---|---|
| 冻结流水 INSERT 失败 | 仅在测试库给 fund_ledger 加 CHECK，禁止 entry_type=1 | RPC INTERNAL；本次订单回滚，四表行快照不变 |
| 接收事件 INSERT 失败 | 解除前一故障后，仅在测试库给 outbox_events 加 CHECK，禁止 event_type=1 | RPC INTERNAL；本次订单、冻结流水一起回滚，四表行快照不变 |
| 解除故障后重试原业务键 | 删除测试约束，再发送同参数请求 | 成功且只冻结一次，仅一条订单、流水和事件；继续重试不重复追加 |

每次先直连执行一次受限 INSERT，确认数据库报 3819，再发 RPC。RPC 错误后，另一个会话用锁定读取等待该业务键的事务结束，再核对快照；服务日志还须出现两次对应 CHECK 错误，不能用无关 INTERNAL 冒充目标故障已触发。

当前下单顺序是 `orders -> fund_ledger -> outbox_events -> accounts -> COMMIT`。这两个故障都在账户 UPDATE 之前，因此账户不变不代表已经验证“扣款后退款”；账户 UPDATE 出错后的三个 INSERT 回滚由原 71 项覆盖。

### 管理权限

优先尝试本机 MySQL 管理账号 TCP 连接，默认沿用 Compose 的开发账号 `root`。也支持环境变量 `TRADE_TEST_MYSQL_ADMIN_USER` 和 `TRADE_TEST_MYSQL_ADMIN_PASSWORD`；密码不作为 mysql 命令行参数传入。这个入口仅用于本机验收环境，不应放入业务服务。

TCP 不可用时尝试 Docker Compose，再尝试无交互 sudo。如果提示无权限，在自己的终端执行：

```bash
sudo -v
bash test/script/submit_order_write_fault_acceptance.sh
```

`sudo -v` 是提前完成本机身份验证；脚本本身仍用普通用户运行，不修改 Docker 用户组、代理或守护进程配置。没有权限时在建库前退出。2026-10-27 实际运行使用 TCP 管理入口，Docker 回退路径未实跑。

结束时先停止自建服务，再撤销临时库授权、删除自己创建的库。MySQL 删除库不会自动删除数据库级授权，所以两步分别执行；原 `arena` 只做前后只读快照比较。[MySQL DROP DATABASE 说明](https://dev.mysql.com/doc/refman/8.0/en/drop-database.html)

### 报告与边界

```text
/tmp/trade_submit_write_fault.随机后缀/
  regression-results.json    原 71 项
  write-fault-results.json   新 27 项，含失败前后快照与恢复响应
  arena-before.json          原学习库四表行快照
  arena-after.json
  database.json              本次临时库名称，不含密码
  server.log                 RPC 对应 SQL 错误
  fault-evidence.txt         服务端 CHECK 错误数量
  lifecycle.txt              服务退出码与收尾耗时
  admin-setup.log            建库、导入与授权的错误线索
  admin-cleanup.log          撤权与删库的错误线索
```

脚本退出码：0 全部通过，1 用例或清理失败，2 环境准备失败。清理不成功会保留临时库名称并返回失败，不假装已经清理。

可把现有构建目录作为唯一位置参数，例如 `bash test/script/submit_order_write_fault_acceptance.sh build`。不需要另配 CMake 目标：专项复用现有验收程序的 `--write-faults` 模式，普通回归的 71 项口径不变。

本阶段仅验证池容量 1 下的非重复键 INSERT 错误、回滚与恢复。它不证明流水/Outbox 的 1062 分支、COMMIT 应答丢失、写请求统一取消、多连接竞争或吞吐。不要直接对 `arena` 运行 `--write-faults`，程序会拒绝；日常只使用上面的脚本入口。

## M1.6 至 M1.8 边界专项

```bash
bash test/script/m1_remaining_acceptance.sh
```

沿用上面的建库、授权、原库只读快照与清理流程。需要 Python 3，额外保持 13307、13308 空闲；不安装新中间件，不修改 Docker 配置。上面不带选项的命令保留历史客户端期限口径，不代表当前写请求契约。2026-10-27 最新复跑仍退出 1，但只剩两项与新契约不符的客户端期限断言；原停机检查已通过。新契约的验收入口见下一节。

| 阶段 | 做什么 | 两轮结果 |
|---|---|---|
| M1.6 | COMMIT 转发前断连；收到 MySQL OK 后丢弃；按业务键查单、重试 | 12/12 |
| M1.7 期限 | 300ms 客户端 deadline；账户 UPDATE 锁等待与 COMMIT OK 暂扣 | 11 项中 2 失败：SQL 等待没有按剩余预算结束 |
| M1.7 停机 | COMMIT OK 暂扣期间 SIGTERM，等待 3 秒宽限加 500ms 观察余量 | 5 项中 1 失败：依赖未释放时进程仍在；释放后正常退出 |
| M1.8 池 2 | 同时阻塞两笔独立账户 UPDATE，记录两个真实 MySQL 连接，再跑原回归 | 3/3 重叠验证 + 71/71 |
| M1.8 池 4 | 同时阻塞四笔独立账户 UPDATE，记录四个真实 MySQL 连接，再跑原回归 | 3/3 重叠验证 + 71/71 |

### 验收入口和生产入口的区别

`trade_fault_server` 只在 ARENA_BUILD_ACCEPTANCE_TESTS 开启时提供显式构建目标，复用真实 TradeAccountServiceImpl、各 Reactor 和 TradeAccountRepository.cpp，没有重写业务逻辑。新增的 CMake 配置仅位于 test/CMakeLists.txt；生产 main 仍默认池容量 1、MySQL 3306。

验收入口用位置参数选择池大小 1/2/4 与 MySQL 3306/13307，连接重试间隔为 100ms、建连期限 2 秒，以缩短故障复现等待。13307 是本机测试代理，仅此场景关闭 MySQL TLS 以识别报文；普通直连和生产配置没有关闭 TLS。gRPC/Asio/Repository 的取消和停机处理没有为测试另写实现。

这证明复用业务类在已测配置下的正确性，不是生产启动参数已改、生产重连延迟已测或性能已经提升。

### 故障代理只拦截 COMMIT

`mysql_commit_fault_proxy.py` 仅监听 127.0.0.1:13307，控制入口 127.0.0.1:13308，向 3306 转发。它只处理本轮用到的小型经典 MySQL 报文，拒绝 TLS、查询属性和大报文，不是正式代理或完整 SQL 解析器。

- before_commit：识别到 COM_QUERY 的 COMMIT 后，不向 MySQL 转发，关闭该测试连接；之前未提交的事务回滚。
- drop_commit_ack：转发 COMMIT，读到 MySQL 成功 OK，但不送回服务端便断开；独立数据库会话可看到已提交订单。
- hold_commit_ack：读到成功 OK 后暂扣，用于客户端 deadline 和停机；控制入口显式释放后才继续送回。

代理只记录断点、时间和连接编号，不记录认证报文。proxy-events.jsonl 必须有准确的 commit_not_forwarded / mysql_commit_ok_seen / commit_ok_dropped 等证据；不能拿随意断连充当提交结果未知。

### 查看失败场景

报告保存在 `/tmp/trade_m1_remaining.随机后缀/`：

```text
commit-results.json        两个精确故障落点和同键恢复
cancel-results.json        两项期限传播失败、原子性和恢复
shutdown-results.json      一项退出超时、解除故障后退出和数据检查
pool2-overlap.json         两条不同连接同时执行 UPDATE 的证据
pool2-regression.json      池 2 的原 71 项
pool4-overlap.json
pool4-regression.json
proxy-events.jsonl
boundaries-server.log      提交、deadline、停机阶段的服务日志
pool2-server.log
pool4-server.log
arena-before.json
arena-after.json
```

先看失败检查的 details，不只看 SUMMARY。预提交 deadline 结束后 SQL 仍可继续提交；提交后超时也不能退款。测试会先释放自己设置的锁或暂扣，再验证同键恢复、停机与删库；没有因当前失败而强改业务预期。显式 TryCancel 与其他停机落点还需随 M1.7 修复补验，不能只修到这三项变绿就宣称任意取消均安全。

## 服务端独立预算专项

```bash
bash test/script/m1_remaining_acceptance.sh --server-budget
```

当前契约是：已开始的写请求按服务端独立的 3 秒总预算收束；客户端 deadline 或 TryCancel 不等于 CancelOrder。借连接和各条 SQL 共用同一截止时刻。取消本端网络等待不保证数据库也立即停止，未提交事务最终回滚，已提交结果仍靠原业务键确认。

这个选项增加新用例，不修改或删除旧断言和旧报告：

| 报告 | 修复后两轮结果 | 场景 |
|---|---|---|
| commit-results.json | 12/12 | 原有 COMMIT 前断连、成功 OK 丢失与同键恢复 |
| server-budget-results.json | 18/18 | 直连 3306；客户端 300ms 超时、显式 TryCancel 后继续完成；持锁超过 3 秒时本端等待结束、回滚和立即重试 |
| server-budget-commit-results.json | 7/7 | 暂扣已成功的 COMMIT OK 超过服务端预算，之后确认原订单并重试 |
| shutdown-results.json | 5/5 | 新启动的服务在 COMMIT OK 被暂扣时停机 |
| pool2/4-overlap.json | 各 3/3 | 真实多连接重叠与原子性 |
| pool2/4-regression.json | 各 71/71 | 原四表回归 |

修复后两轮目录为 `/tmp/trade_m1_server_budget.lHpG0D`、`/tmp/trade_m1_server_budget.tTx9oJ`，各 190/190，退出 0。关闭连接的独立预算为 300ms，因此观察到的请求完成约 3.3 秒可包含业务超时后的清理，不是整个调用严格 3 秒内结束。

历史目录 `/tmp/trade_m1_server_budget.iuh4a6`、`/tmp/trade_m1_server_budget.L6mdmG` 各 188/190、退出 1，保留不覆盖：SQL 被取消后立即同键重试返回 INTERNAL；COMMIT 等待被取消后首次 GetOrder 返回 NOT_FOUND，但独立数据库会话能看到订单。这两个恢复断言未修改，已在修复后通过；没有发现额外冻结或部分账本提交。

直连锁等待、COMMIT 等待和停机分别使用新测试进程，防止一个坏连接污染后续场景；每个恢复断言仍在产生故障的同一个进程内执行，不能用重启冒充恢复成功。生产配置不变。日志分别为 server-budget-server.log、commit-budget-server.log、shutdown-server.log。

正常 SQL 错误后的会话 reset 不等于网络取消后的安全恢复。Boost 1.88 规定被取消的连接状态不再保证可用，需关闭或销毁后再重建，不能仅把租约归还当作已处理。[Boost.MySQL 取消说明](https://www.boost.org/doc/libs/1_88_0/libs/mysql/doc/html/mysql/async.html)

本轮普通写入故障入口仍为 71/71 + 27/27，通过目录 `/tmp/trade_submit_write_fault.fr5nlF`。所有临时库和授权均已清理，原 arena 四表快照不变。首版新用例曾在同一个进程串行执行后续故障，报告 `/tmp/trade_m1_server_budget.ioXdVT` 保留作调试记录，不作为两轮独立结果。

连接回收修复后的兼容复跑为 `/tmp/trade_submit_write_fault.hE30dK`，71/71 + 27/27、退出 0。脚本日志探针改为匹配错误名和 mysql.mysql-server:3819，不再依赖 mysql smoke failed 前缀，仍要求恰好两次真实服务端 CHECK 错误。此前仅因旧前缀误报失败的 `/tmp/trade_submit_write_fault.4EuG6f` 保留，不能抹成脚本曾通过。最新验收与 M1 范围见[阶段记录第十六节](../../docs/学习计划/02_2026-10-21_阶段验收与面试自测清单.md#十六2026-10-27-连接回收修复后-m1-阶段验收通过)。

最终 grep -E 版本的完整复跑为 `/tmp/trade_submit_write_fault.KYJZ9A`，仍为 71/71 + 27/27、退出 0。修复前后关键报告和日志已长期复制到[M1 案例归档](../../docs/C++大型工程调包侠独门秘籍/M1_事务与故障案例/README.md)，附来源与 SHA-256 清单，不再只依赖 /tmp。
