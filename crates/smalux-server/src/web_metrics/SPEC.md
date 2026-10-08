---
id: server-web-metrics
type: submodule-design
title: CPU 与内存只读指标投影
status: active
parent: server-web-login
tags: [metrics, websocket]
---

# CPU/内存投影

## 边界与输入

`web_metrics.rs` 为 HTTP `metrics.latest` 与 WS metrics topic 提供同一只读服务。输入最多 100 个唯一 Agent ID、1–2 个唯一 CPU/memory 指标；拒绝未知字段与指标。授权范围来自 Server 启动时解析的 `SMALUX_WEB_METRICS_BINDINGS`，不是客户端筛选，不调用 CLI、不创建采集 Job、无迁移或绑定写 API。

绑定 JSON 最多 1000 项、256 KiB；每项 agentId 为 1–128 可打印非空白 ASCII，cpuJobId/memoryJobId 可选且须规范 UUID，拒绝重复 Agent、同 Job 两组和未知字段。排序后的规范映射 SHA-256 前 16 字节按大端 u128 转十进制字符串生成 bindingRevision；配置变更需重启。stale 阈值默认 60 秒，允许 1–86400。

## 来源与一致性

在单个读事务中检查 Agent 存在且 active、未吊销，再读取当前 agent_jobs。数据库支持时指定 RepeatableRead/ReadOnly；SQLite 使用事务快照。每组核对 Job 归属、enabled、revision、task_kind 和解码 JobDefinition；缺失、停用、不匹配为 unavailable/sourceUnavailable。

仅查询 Agent + 绑定 Job + 当前 revision + result_kind 的 task_reports，按 started_at DESC NULLS LAST、received_at DESC、report_id DESC，LIMIT 1。解码完整 TaskReport，核对身份、时间和 result oneof。不让旧版本、迟到旧报告或其他 Job 覆盖当前来源，不因坏样本回退旧值。

时间校验基准在读取报告后取当前时间，避免查询等待期间新入库样本被错误标为未来。CPU/内存报告独立提交，订阅可以先看到合法中间快照；消费端按持续递增的 sequence 更新，不能假定两组总在同一通知变更。

## 输出与质量

MetricsLatest[] 只含请求组与 bindingRevision。每组包含 value、独立 sampledAtMs/receivedAtMs、quality、sourceJobId/sourceJobRevision。微秒转毫秒，revision 使用十进制字符串，JSON 数值不超过 JS 安全整数。

- valid：CPU warmed_up 且百分比有限、0..100；内存 total>0、used<=total、字节安全、百分比有限且0..100。真实0保留，未知逻辑核数为null。
- warmingUp/firstSample：CPU 尚未完成预热，value=null。
- unknown/notConfigured、noSamples 或 missingSampleTime：值与缺失时间为null。
- unavailable：来源不可用，载荷/时间/数值无效；value=null，不暴露原始payload。
- stale/sampleExpired：采样超阈值，保留真实值并明确质量。原因稳定，单纯时钟变化不会逐秒产生不同快照。

未配置、不存在、已吊销 Agent 都返回同一 Forbidden。数据库错误映射 Internal，不伪造成功或降级 Mock。调用边界对查询施加 5 秒超时，WS 另外持续验证会话与订阅授权。

## 验收与非目标

`tests.rs` 验证配置/参数约束、稳定指纹、0/NaN/越界/JS安全整数、独立时间、未知/过期/未来、旧revision/迟到、来源丢失/禁用/不兼容、损坏payload及授权拒绝。`web_auth/ws_tests.rs` 覆盖真实TCP从入库Proto到HTTP/WS快照、更新、恢复和失效断连。

本批仅 Rust；不接前端，不提供历史、网络、operation 通知、Job CRUD 或可恢复事件日志。PostgreSQL/MySQL 实机与生产容量需独立验证。
