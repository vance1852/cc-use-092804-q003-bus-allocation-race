# 修复控制总线并发分配泄露存储异常基础平台

本项目是一套可离线运行的 Python 服务端平台，服务于具身智能机器人控制域、AI 计算域和国产电子部件质量域。平台管理控制计算节点、实时控制总线、资源时隙、机器人构建、验证测量、分析租约、部件批次与质量决定，业务状态、幂等结果和审计事件保存在 SQLite 中。

## 目录

- `src/robot_control/`：控制节点、实时总线、资源批次、时隙申请、容量分配和架构情景；
- `src/embodied_ai/`：机器人构建、验证协议、测量导入、排除复核、分析任务和准入决定；
- `src/component_qualification/`：国产电子部件批次、信号测量、统计分析、账号权限和质量审批；
- `fixtures/`：离线验收使用的验证协议与结构化测量；
- `tests/`：领域规则、事务、权限、HTTP API 和命令行验收测试。

## 调度周期裁决契约

同一调度周期内，左右机械臂控制器对实时总线的并发申请按以下规则裁决：

- `GET /routes/{id}/snapshot?service_date=YYYY-MM-DD` 读取容量快照（申请集合、降级窗口、总线版本）及其版本 `snapshot_sha256`；
- `POST /routes/{id}/allocate` 携带 `service_date` 与可选的 `expected_snapshot` 申请裁决，整个读取-裁决-提交在单个事务内完成；
- 一个周期只产生一份有效决定，申请集合、降级窗口和总线版本随决定冻结在 `allocation_runs` 中，并写入一条 `allocation.completed` 审计事件（`GET /audit/events` 可查）；
- 内容相同的并发申请或重试拿回首次裁决（`replayed: true`，同一个 `allocation_id`）；依据已经变化的申请收到 409 冲突，响应 `error.details` 中包含当前版本；
- 裁决结果逐项给出 `outcome`（allocated/partial/rejected）与落选原因 `reason`，提交失败不会留下单侧动作或半更新申请，接口不泄露存储异常。

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

## HTTP 服务

```bash
PYTHONPATH=src python3 -m robot_control.api --database robot-control.sqlite3 --host 127.0.0.1 --port 8080
PYTHONPATH=src python3 -m embodied_ai.api --database embodied-ai.sqlite3 --host 127.0.0.1 --port 8081
PYTHONPATH=src python3 -m component_qualification.api --database component.sqlite3 --host 127.0.0.1 --port 8082
```

服务提供 JSON 接口与健康检查。进程重启后可以继续读取 SQLite 中的业务状态和审计历史。
