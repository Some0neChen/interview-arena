# 交易服务验收脚本

以下命令在 `src/InterviewArena` 目录执行。先启动 MySQL，保持 13401 空闲，准备好已配置 Boost 1.88 的 `build`；脚本不停止已有服务，不运行 ASAN/TSAN。

## M2 封板复测入口

在同一构建目录**依次**运行（共用端口）。每轮创建专属测试库，结束时清理，终端最后一行给出 JSON 报告路径。

```bash
node test/script/m2_mainline_acceptance.js
node test/script/m2_mainline_acceptance.js --lost-reply
node test/script/m2_mainline_acceptance.js --cancel-fill
node test/script/m2_mainline_acceptance.js --remaining
node test/script/m2_mainline_acceptance.js --snapshot-rules
node test/script/m2_mainline_acceptance.js --backoff
```

`--backoff` 使用真实等待（约两分钟，不含编译）。隔离库触发器记录派发器实际写入的时间：持续失联时的七次退避、重启保留截止点、其他订单独立推进、恢复清零、合法重复快照降频，以及一单结算和退避更新都报错时是否影响同批下一单。它不测吞吐。

## M2 主链与重启验收

```bash
node test/script/m2_mainline_acceptance.js
```

要求 13401、3666 都空闲，以及当前构建目录已开启 `ARENA_BUILD_ACCEPTANCE_TESTS`。脚本编译真实 `trade_server`、`trade_broker` 和测试客户端；不改业务源码或 CMake。支持 `M2_BUILD_DIR` 指定已有构建目录，`--help` 查看依赖。

沿用 `submit_order_acceptance --admin-sql` 的本机 TCP 管理入口，不使用 Docker/sudo 回退。创建两个独立临时库，分别导入订单四表和券商两表；只授权已有应用账号访问本次库，不修改 `arena`。结束或收到 SIGINT/SIGTERM 时收尾自建进程、撤权并删库；强制杀死脚本本身仍可能需要按报告中的库名人工清理。

| 场景 | 实际检查 |
|---|---|
| 正常报单、自动成交与价差 | 预留 3000 分、成交 2800 分，最终可用 7200、冻结 0、已花费 2800；同时检查订单、流水、Outbox 与券商记录 |
| 重复请求 | 同键同参返回原单，不再记账；同键异参 ALREADY_EXISTS；重复撤单不多退 |
| 不支持/禁用标的 | 券商持久拒单，本地全额解冻；成交后的撤单不能退钱 |
| 券商不可用 | DISPATCHING 与 CANCEL_PENDING 保留冻结；不以失联代替拒单 |
| 订单端 SIGKILL 后重启 | 恢复原 DISPATCHING 与 CANCEL_PENDING 订单，保持业务键；远端 NOT_FOUND 后同键补报再撤销 |
| 券商 SIGKILL 后重启 | 原 ACCEPTED 单仍在，模拟价格变化后自动成交；券商订单号不变 |
| 同账户 8 笔并发 | 初始 10000 分、每笔预留 3000 分，3 成功、5 FAILED_PRECONDITION；仅保留 3 笔订单和冻结流水 |
| 停机 | 等待订单尚存时，两端正常停机均在脚本 8 秒上限内，不强杀兜底 |

2026-11-15 首两轮均为 **15/15**；报告和进程日志位于 `/tmp/m2-mainline-AoM9ae`、`/tmp/m2-mainline-KrwbvV`，临时库和授权已清理。报告副本与剩余项见 [M2 当前进度](../../docs/学习计划/交易项目M1至M6/M2_模拟券商与订单恢复.md#当前进度)。每轮记录具体账户、订单、流水、事件快照和进程退出结果，不把两轮重复计为 30 个独立场景。

范围：每端生产池容量 1，真实两进程联调，不是性能、多连接竞争或完整 M2 封板。重复客户端请求不等于精确注入重复券商回报；普通停机不证明任意晚到回调安全。接单丢应答、提交故障、竞争和错误快照的独立验收见下文。上述首两轮版本只有冻结和成交流水，未独立记录价差返还。

2026-11-15 补充返还流水、成交字段及派发事件后，脚本新增对应断言。本轮在第一个场景失败后停止（0/1，后续未执行），报告为 `/tmp/m2-mainline-fvyiQ2/report.json`，不沿用旧版本的 15/15 结论。真实快照已记录冻结 3000、成交 2800、价差返还 200，账户为可用 7200、冻结 0、已花费 2800，成交事件包含数量、价格、花费和返还金额；失败原因为派发事件的 `client_order_id` 漏值，使整个 JSON payload 变成数组。测试进程已正常退出，临时库与授权已清理。

新增检查要求每条事件 payload 是 JSON 对象、业务键正确，远端处理订单只产生一次派发开始事件，并核对成交事件与券商成交数量、价格及本地返还金额。另有独立最小复现确认：SQL 报 1054 后 `results.has_value()` 为 false，继续调用 `affected_rows()` 会在当前 Debug 构建中断言退出。两处 Outbox INSERT 的错误处理应先判 `ec`，成功后才能读取结果；该最小复现不等于已对整个服务注入并验收此故障。

同日修复后复验 **15/15 通过**，报告为 `/tmp/m2-mainline-FXW9cb/report.json`。此次包含上述新增的返还流水、成交字段、派发事件结构及唯一性检查；业务键已在复用查询结果前复制为 `std::string`，两处 Outbox INSERT 也已调整错误判断顺序。此轮仍是主链与重启验收，不代表前述待专项故障场景已经覆盖。

## M2 接单成功但回复丢失

```bash
node test/script/m2_mainline_acceptance.js --lost-reply
```

沿用上面的隔离库、端口检查和清理规则，额外编译 `test/m2_lost_reply_broker.cpp`。测试入口使用同步 gRPC Service，但通过真实 `TradeBrokerRepository::async_submit_order` 和 MySQL 提交订单；其中的 `future.get()` 仅阻塞测试 RPC 线程，不是生产 BrokerClient 的写法。Boost 默认从 `~/.local/boost-1.88.0` 读取，非默认安装可设置 `BOOST188_ROOT`。

故障插在 Repository 返回成功之后：不发成功回复，等生产 BrokerClient 自己的 3 秒 deadline 到期；同时让后续 GetOrder 暂时返回 UNAVAILABLE，以便稳定观察结果未知状态。此阶段不启动模拟成交 worker。随后强制退出本次测试启动的订单端，关闭测试券商入口，切换回真实 `trade_broker` 并重启 `trade_server`；两端继续使用各自原来的隔离库。

| 检查 | 预期证据 |
|---|---|
| 已提交但回复不可用 | 券商有 ACCEPTED 记录；本地 DISPATCHING、可用 7000、冻结 3000，只有冻结流水和接收/派发事件 |
| 结果未知时用户重试 | 同键返回原订单，不再冻结，不产生第二笔券商订单 |
| 订单端强制退出后恢复 | 券商单号和业务键不变；真实 worker 成交后，本地可用 7200、冻结 0、花费 2800 |
| 结算后重试和重复扫描 | 冻结 3000、成交 2800、返还 200 三条流水不变；事件、账本不重复追加 |

2026-11-15 实测 **4/4**，报告 `/tmp/m2-mainline-wdh2qy/report.json`，其中记录约 3005 ms 后 RPC 被取消、随后尝试确认，以及故障前后的四表和券商快照。`lost-reply-events.jsonl` 保存注入点证据；测试进程、临时库与授权已清理。报告副本见 [M2 存档](../../docs/学习计划/交易项目M1至M6/验收记录/2026-11-15_M2主链/lost-reply.json)。

这是**应用层抑制成功回复**，不是抓包证明真实网络丢包，也没有覆盖生产 SubmitOrderReactor 的全部取消分支。订单端使用未修改的生产程序，券商提交使用真实 Repository，恢复后使用真实券商程序。没有向业务 C++、SQL、proto 或 CMake 增加故障开关；本地提交故障、竞争与快照验收另列，不由这 4 项替代。

## M2 撤单与成交行锁竞争

```bash
node test/script/m2_mainline_acceptance.js --cancel-fill
```

新增测试入口 `test/m2_broker_cancel_fill_race.cpp`，直接复用真实券商 Repository 的 `async_cancel_order` 和 `async_try_fill_order`。生产配置不变；测试入口单独使用池容量 2，让两条 SQL 真正同时在途，不把池容量 1 的排队误当作数据库行锁竞争。编译依赖和 `BOOST188_ROOT` 与前一个专项一致。

每种顺序先由真实两端服务提交一笔 ACCEPTED 订单，然后停掉本次自建券商，让真实网关接受撤单并进入 CANCEL_PENDING。账户仍冻结 3000 分。测试入口接着执行真实券商 SQL，最后恢复真实券商，由原派发器确认结果并结算四表。

竞争顺序通过隔离库内的临时 `BEFORE UPDATE` 触发器和 `GET_LOCK` 控制：先暂停已持有订单行锁的一方，再启动另一方；必须从 `performance_schema.data_lock_waits` 观察到后者等待前者在 `broker_orders` 上的 RECORD 锁，才释放测试屏障。触发器只暂停执行，不替代任何业务更新；没有手工把订单改成 FILLED/CANCELLED。管理账号需有创建测试触发器和读取 performance_schema 的权限，MDL 观测需启用；缺少锁等待证据直接失败，不按通过跳过。

| 固定顺序 | 实际检查 |
|---|---|
| 撤单先拿锁 | 撤单提交 CANCELLED，随后继续的成交读取终态后退出；券商版本只从 1 变成 2；本地最终可用 10000、冻结 0、花费 0，仅冻结/撤单两条流水 |
| 成交先拿锁 | 成交提交 FILLED，等待中的撤单不能覆盖它，而是返回 OK + FILLED 快照；券商版本仍为 2；本地从 CANCEL_PENDING 收口到 FILLED，可用 7200、冻结 0、花费 2800，流水为冻结 3000/成交 2800/返还 200 |
| 两种结果之后 | 再次撤单、同键重试和重复扫描都不能产生第二次记账；原券商单号不变 |

2026-11-15 实测 **8/8**，报告 `/tmp/m2-mainline-WYcPS6/report.json`，含两组不同线程间的 `X,REC_NOT_GAP` 等待、券商版本和四表快照；[报告副本](../../docs/学习计划/交易项目M1至M6/验收记录/2026-11-15_M2主链/cancel-fill.json)。临时触发器删除已检查，测试进程、库与授权已清理。

边界：竞争期间直接调用真实 Repository，不走 gRPC Reactor 或后台 worker 的调度；恢复结算则使用原生产程序。该结果覆盖两种 SQL 先后顺序，不覆盖所有 RPC 调度窗口、任意连接池规模、本地撤单与取得派发权的竞争，也不是性能数据。没有改业务 C++、SQL、proto 或 CMake。

同日测试驱动将带 `cancel_at` 的协程统一投递到显式 strand 后复跑，仍 **8/8**，报告 `/tmp/m2-mainline-DkUogd/report.json`。两个连接的 SQL 仍实际重叠并产生行锁等待；strand 只串行执行协程片段，不把异步事务改成一个接一个完成。

## M2 整体验收剩余项

```bash
node test/script/m2_mainline_acceptance.js --remaining
```

额外需要空闲的 13307/13308，供本机测试 MySQL 协议代理及控制端口使用；需要 Python 3、触发器和 performance_schema 观测权限。不改 Docker、MySQL 全局配置或业务源码。测试驱动通过手工编译命令加入现有构建目录，不修改生产 CMake。

| 场景 | 注入方式与判定 |
|---|---|
| 本地撤单先/取得派发权先 | `m2_local_repository_probe.cpp` 使用真实 Repository、两个连接及显式 strand；触发器控制先后，观察真实行锁等待；不能退款后仍取得派发权 |
| 结算 COMMIT 前断连/提交 OK 丢失 | 复用 `mysql_commit_fault_proxy.py`；前者四表完全不变，后者四表已经完整提交；再次应用快照只结算一次 |
| 重复结算和冲突目标状态 | 直接重复调用真实结算入口，检查账户、订单、流水、事件完全一致；不冒充 broker_version 校验 |
| DISPATCHING 已落库但没有发过 RPC | 驱动准备后退出，原服务重启按原业务键恢复；不是逐指令强杀 |
| 已收到真实 FILLED、本地结算尚未提交时崩溃 | 临时触发器固定网关进入结算 UPDATE 的位置；只对自建网关 SIGKILL，确认本地未半提交，重启结算原券商单 |
| 在途 RPC 停机 | 已提交接单但成功回复被抑制，SIGTERM 网关；8 秒内正常退出，冻结保留，重启确认同一笔 |
| 错误券商快照 | `m2_snapshot_broker.cpp` 通过真实 gRPC 故意改错业务键、原参数、券商编号/版本或成交信息；生产网关应拒收、不改变资金 |

`m2_remaining_checks.js` 是上述入口的场景模块，不单独执行。错误快照用例会保留失败继续运行其余独立用例，最终有失败则退出码非零，不自动放宽预期。基础设施或前置步骤失败会终止并清理，不能把中断前的通过数当作整套完成。

首轮完整执行 **16/27**，报告 `/tmp/m2-mainline-YvCgYx/report.json`：竞争/故障/停机 11 项、合法及已有保护的快照 5 项通过；11 个错误快照被错误结算。主要是回报身份/参数和成交约束未校验，不是 11 套新业务。版本持久化、旧版本/同版本矛盾及按单退避尚未实现，未计入通过项。详细反例与修改入口见 [整体验收报告](../../docs/学习计划/交易项目M1至M6/验收记录/2026-11-15_M2主链/README.md)。

最终复跑增加 CANCELLED/REJECTED 两项重复结算与冲突目标保护，为 **18/29**，报告 `/tmp/m2-mainline-Thm24Q/report.json`。原 11 个反例稳定复现；在途 RPC 停机约 2988 ms，恢复与重复结算仍通过。`audit_cases_completed=true` 表示本入口的场景已执行完，不表示 M2 全绿。

输出报告保留 `snapshot_observations`、故障事件、数据库快照和停机耗时；临时进程、触发器、数据库及授权清理结果也会记录。本轮只保留证据，不修改 `arena` 或用户原有服务。

专项完成后再次回归：主链 **15/15**（`/tmp/m2-mainline-skChxw/report.json`），接单丢应答 **4/4**（`/tmp/m2-mainline-kex8Pw/report.json`），券商竞争 **8/8**（`/tmp/m2-mainline-DkUogd/report.json`）。这些通过不抵消错误快照的失败；完整 M2 仍未封板。

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
