# v3.21.0

## 功能改进

- 新增 SQLite 单网关存储后端，不依赖 PostgreSQL 或 Redis；提供独立配置、Compose 部署、安装器和备份恢复支持
- 用量页支持按部署时区选择自然日范围，并按 OpenAI OAuth 账号和 Client Key 展示 Token 用量及占比；管理端与 Key 用量页展示首字等待和输出吞吐
- 新增 Codex Live 语音通话代理，支持 OAuth 账号的通话创建、sideband WebSocket 与挂断；创建时沿用 Client Key 的账号范围和模型权限，后续控制固定使用创建通话的账号
- 统一同一对话的账号绑定：根线程、后代线程及可关联的 Search、Images、Live 创建请求共享当前账号；根线程保留既有重试与换号策略，后代线程只排队等待并跟随根线程迁移，不自行换号。官方 Images 通过已观测的 Responses `turn_id` 关联会话，未知轮次不推测归属；插件显式选号沿用原有行为
- 分组 Fast 策略新增「默认／开启／关闭」三种模式，作用于 OpenAI Responses；多分组冲突时关闭优先，其次开启，默认跟随客户端；Fast 档位统一使用黄色标识
- 调度新增单请求换号次数限制，最多换号 3 次；OAuth 刷新与额度观测错峰执行，客户端身份支持终端类型和自动版本滞后配置
- 管理员会话支持无感续期，并同步多标签页 Cookie；续期仍受不活动期限和最长有效期限制
- 插件版本或能力不匹配时持续显示兼容性提醒，保留启用配置并允许尝试启动；系统升级与回滚不再因兼容性提醒批量停用插件
- 新增可选的 Linux/glibc 小内存优化配置，通过 `GLIBC_TUNABLES` 调整空闲内存回收，默认关闭

## 问题修复

- 修复同一会话并发首次请求可能分配到不同账号，以及旧请求完成后覆盖新账号绑定的问题；后代线程等待根线程首次认领，账号忙碌或不可用时沿用有界排队
- 移除模型请求默认 600 秒总执行时限，避免长任务被固定时限中断；显式请求截止时间与上游传输空闲超时仍生效，并修复取消等待者释放不及时造成的资源积累
- OpenAI 重试正确遵循 `Retry-After` 的秒数、HTTP 日期与零延迟，不再被本地退避上限截断；`flex_unavailable` 保留原始拒绝，不自动重试或冷却账号
- Live 通话固定创建时的客户端身份画像，后续连接和挂断使用当前凭据及出站代理，避免配置变更混用画像
- 修复账号用量诊断遗漏请求、长代理出口 IP 溢出，以及图表时间标签附加时区偏移的问题
- 修复 PostgreSQL 启动时未加载迁移 0022 的问题

## 升级说明

- PostgreSQL 启动时自动执行 `0022_refresh_margin_and_group_fast_mode.sql`。已发布迁移保持原字节并追加该迁移；OAuth 刷新提前量默认由 3,600 秒调整为 300 秒，现有值等于旧默认时一并迁移，其他自定义值保持不变
- 已在源码部署中执行过原 `0022_narrow_refresh_margin.sql` 或 `0023_group_fast_mode.sql` 的实例，与合并后的迁移历史不兼容，不适用直接升级路径
- SQLite 使用独立数据库文件与迁移集，仅适用于单网关和本地持久卷；从 PostgreSQL 切换到 SQLite 不会自动导入数据，也不会删除或覆盖原数据库
- 分组管理 API 的 `disableFast` 替换为 `fastMode`，取值为 `default`、`enabled`、`disabled`；已有禁用设置迁移为 `disabled`，其他分组迁移为 `default`，自定义管理客户端需同步字段
- 使用新版 Compose 时，将本版 `config.example.yaml` 中 `services.app-runtime.environment.GLIBC_TUNABLES: ''` 合并到现有配置，即使不开启小内存优化也需保留此桥接段；保留原有凭据配置，修改启动环境后重建应用容器
- 旧管理员认证接口或 Cookie 客户端需改用 `/api/auth/*` 并重新登录
- 插件 `middleware v3` 与 `upstream_adapter v1` 仍在兼容窗口内且当前可用；插件作者可分别迁移至 `middleware v4` 与 `upstream_adapter v2`
