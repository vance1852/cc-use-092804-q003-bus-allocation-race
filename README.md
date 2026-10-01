# 修复控制总线并发分配泄露存储异常基础平台

本项目是一套可离线运行的 Python 服务端平台，服务于具身智能机器人控制域、AI 计算域和国产电子部件质量域。平台管理控制计算节点、实时控制总线、资源时隙、机器人构建、验证测量、分析租约、部件批次与质量决定，业务状态、幂等结果和审计事件保存在 SQLite 中。

## 目录

- `src/robot_control/`：控制节点、实时总线、资源批次、时隙申请、容量分配和架构情景；
- `src/embodied_ai/`：机器人构建、验证协议、测量导入、排除复核、分析任务和准入决定；
- `src/component_qualification/`：国产电子部件批次、信号测量、统计分析、账号权限和质量审批；
- `fixtures/`：离线验收使用的验证协议与结构化测量；
- `tests/`：领域规则、事务、权限、HTTP API 和命令行验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅使用 Python 标准库和 SQLite

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 构建检查

```bash
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m robot_control.acceptance --workspace .
PYTHONPATH=src python3 -m embodied_ai.acceptance --workspace .
PYTHONPATH=src python3 -m component_qualification.acceptance
```

三条命令会在临时 SQLite 数据库中完成控制资源分配、AI 验证分析和国产电子部件质量流程，不访问外部网络。

## 实时总线并发裁决

左右机械臂控制器在同一调度周期争抢实时总线时，平台以控制周期（总线 + 服务日）为单位作出**唯一、原子、可幂等重放**的业务裁决：

- “读取申请集合 / 总线版本 / 降级窗口 → 计算 → 冻结”在单个 `BEGIN IMMEDIATE` 事务内完成，并由服务内进程锁串行化，同一周期至多一份决定落库；提交失败整体回滚，不会留下单侧动作或半更新申请。
- 裁决冻结的内容包含：本次采用的申请集合摘要（`request_set_sha256`）、降级窗口（`degraded` / `degradation_windows` 及其摘要，快照入 `freeze_outage_snapshots`）和总线版本（`bus_revision`）。
- 内容相同的重试原样返回首次裁决（`replayed=true`），执行侧不会收到第二份决定；依据已变化（总线版本变更、降级窗口变更或冻结后又有新申请）的后来者收到 `409 schedule_conflict`，响应同时给出冻结版本与当前版本及落选原因，绝不泄露 SQLite 存储异常。

联调复现（多个独立连接在同一屏障后并发发起同一周期申请）：

```bash
PYTHONPATH=src python3 -m robot_control.arbitration_demo --racers 8
```

输出稳定给出赢家时隙、落选原因（未满足量）、实际占用的控制周期，并可从 `execution_side_audit` 确认 `allocation_runs` 与 `allocation.frozen` 审计事件都只有一份。


## HTTP 服务

```bash
PYTHONPATH=src python3 -m robot_control.api --database robot-control.sqlite3 --host 127.0.0.1 --port 8080
PYTHONPATH=src python3 -m embodied_ai.api --database embodied-ai.sqlite3 --host 127.0.0.1 --port 8081
PYTHONPATH=src python3 -m component_qualification.api --database component.sqlite3 --host 127.0.0.1 --port 8082
```

服务提供 JSON 接口与健康检查。进程重启后可以继续读取 SQLite 中的业务状态和审计历史。
