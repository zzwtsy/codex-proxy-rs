# v3.21.2

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
- OpenAI 账号亲和支持 `relaxed`、`preferred`、`strict` 三种模式，可配置单请求换号次数与会话绑定保留时长；管理页面提供对应设置，无显式配置的实例默认采用严格模式

## 问题修复

- 修复同一会话并发首次请求可能分配到不同账号，以及旧请求完成后覆盖新账号绑定的问题；后代线程等待根线程首次认领，账号忙碌或不可用时沿用有界排队
- 移除模型请求默认 600 秒总执行时限，避免长任务被固定时限中断；显式请求截止时间与上游传输空闲超时仍生效，并修复取消等待者释放不及时造成的资源积累
- OpenAI 重试正确遵循 `Retry-After` 的秒数、HTTP 日期与零延迟，不再被本地退避上限截断；`flex_unavailable` 保留原始拒绝，不自动重试或冷却账号
- Live 通话固定创建时的客户端身份画像，后续连接和挂断使用当前凭据及出站代理，避免配置变更混用画像
- 修复账号用量诊断遗漏请求、长代理出口 IP 溢出，以及图表时间标签附加时区偏移的问题
- 修复 OpenAI 严格亲和下，子线程或独立记忆整理任务因会话尚未绑定账号而等待超时的问题：任何有可靠会话身份的请求均可按调度策略原子首绑，并发首请求最终共用同一绑定；已有绑定的子线程仍跟随当前账号，迁移权限保持不变
- xAI 模型目录对齐当前 CLI proxy 的模型、推理档位及上下文窗口字段；推理档位按请求保留，不再根据旧模型名称降档或删除，缺失能力与默认值保持未知
- 修复 xAI 托管搜索工具与客户端函数同名时的调用及跨轮重放映射，支持 `web_search.filters.excluded_domains`，校验允许与排除域名列表不能同时非空
- xAI 额度判断采用当前账单字段，保留预付余额的原始符号，不再凭过期快照或旧字段恢复账号；上游报告零费用时视为费用未提供，只有具备完整计价依据时才使用本地估算
- 修复备份取消或失败时的导出进程、分片上传与暂存文件清理，恢复已完成上传的任务后及时清理本地归档，并记录清理失败
- 修复服务关闭时请求排空与后台写入的先后顺序，以及 WebSocket 关闭握手可能被未完成写入阻塞的问题
- 修复 Key 预算重置后，应用与数据库时钟差异可能使未开启窗口被误判为活跃窗口的问题
- 账号状态读取改为有界分批并复用查询结果，避免账号池规模直接形成 Redis 并发峰值；完善 OpenAI 重置卡消费的账号锁回收，保持取消后的同账号串行约束
- 修复 PostgreSQL 启动时未加载迁移 0022 的问题

## 升级事项

- 从本地 v3.21.0 升级时，PostgreSQL 自动执行 `0023_error_details_and_account_affinity.sql` 与 `0024_preferred_account_affinity.sql`，SQLite 自动执行 `0015_error_details_and_account_affinity.sql`；错误详情数据保留，revision 为 1 的初始配置采用严格亲和，已有保存配置沿用兼容模式
- 管理 API 的 `openaiAccountAffinity` 支持 `relaxed`、`preferred`、`strict`；自定义客户端还应识别 `maxAccountRotations` 与 `openaiSessionAffinityTtlHours`
- 严格亲和不再要求根线程必须先发请求才能建立会话绑定
- SQLite 使用独立数据库文件与迁移集，仅适用于单网关和本地持久卷；从 PostgreSQL 切换到 SQLite 不会自动导入数据，也不会删除或覆盖原数据库
- 分组管理 API 的 `disableFast` 替换为 `fastMode`，取值为 `default`、`enabled`、`disabled`；已有禁用设置迁移为 `disabled`，其他分组迁移为 `default`，自定义管理客户端需同步字段
- 使用新版 Compose 时，将本版 `config.example.yaml` 中 `services.app-runtime.environment.GLIBC_TUNABLES: ''` 合并到现有配置，即使不开启小内存优化也需保留此桥接段；保留原有凭据配置，修改启动环境后重建应用容器
- 旧管理员认证接口或 Cookie 客户端需改用 `/api/auth/*` 并重新登录
- 插件 `middleware v3` 与 `upstream_adapter v1` 仍在兼容窗口内且当前可用；插件作者可分别迁移至 `middleware v4` 与 `upstream_adapter v2`
- xAI 请求保留路由选定的完整模型名，不再自动解释 `grok`、`grok-latest` 等旧别名或剥离模型前缀；依赖这些名称的配置应使用显式模型映射
- xAI 搜索域名白名单应放在 `web_search.filters.allowed_domains`，旧顶层 `allowed_domains` 返回请求错误；推理档位未知或类型错误时也会明确返回字段错误
- 内建 xAI 适配器对 `generate=false` 预热返回 `400 unsupported_prewarm`，避免误发生成请求；客户端应跳过该预热并继续正式请求
