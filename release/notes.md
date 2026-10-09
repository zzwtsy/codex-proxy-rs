# v3.23.1

## 新增功能

- 系统设置新增 Codex 隐私策略，支持按字段路径进行正则替换、固定值设置、键名改写和条件删除，覆盖工作区 metadata、Desktop Git 上下文、环境文本以及 OpenAI 请求正文与请求头
- 提供工作区路径、Git 远端等规则预设，支持规则排序、单条启停和样本预览；执行失败可选择跳过规则或阻止发送
- 用量与错误诊断统一展示请求观测，增加上游响应耗时、引擎性能指标与恢复关联，便于区分网关等待、上游生成和交付阶段
- Guardian 审批请求使用独立的账号并发容量池，不占用普通推理容量；两种存储模式均支持，并继续共享账号请求间隔
- OpenAI API Key 账号的上游地址支持 HTTP 内网 IP 和容器名，便于接入可信网络中的上游服务

## 修复与改进

- 同步上游 v3.23.0 的 Codex 协议与配置兼容处理，保留不透明请求字段和 WebSocket 消息，支持显式启用 V2 远程压缩，并适配 CC Switch 配置导入
- 请求位置覆盖适配 Codex Desktop 自动附加的时间上下文，CLI 与 Desktop 使用一致的覆盖规则
- Images 生图与编辑请求检查图片模型访问权限并排除已知 Free 套餐 OAuth 账号；插件改写模型后仍复核权限
- 优化分组选项查询与插件实例生命周期，增强健康检查、后台任务和请求失败的受控诊断
- 数据库迁移期间输出等待提示并延长容器启动健康检查宽限期；统一重启与停止策略，避免将长迁移误判为启动失败
- 保留本 fork 的 SQLite、账号分摊和自定义日期范围功能，重整部署与开发指南，新增账号、分组、密钥、隐私和用量诊断使用指南

## 升级事项

- 本次说明以本 fork 的 v3.21.4 为升级基线；升级前备份数据库与部署配置，使用本发行的镜像、Compose 和配置模板，不覆盖已有凭据
- PostgreSQL 追加 `0025_request_observation.sql`、`0026_codex_privacy_policy.sql`；SQLite 追加 `0016_request_observation.sql`、`0017_codex_privacy_policy.sql`、`0018_reserved_account_leases.sql`，启动时自动迁移，旧迁移保持不变
- 两种存储模式都要求单网关副本；SQLite 使用本地持久卷。切换模式不会迁移数据，数据库迁移不能通过回滚二进制撤销
- 隐私策略默认关闭，仅处理经网关发送的 OpenAI 请求，不控制客户端直连遥测。规则没有字段保护名单，认证、会话和工具字段改写可能影响调用；跳过失败规则表示该条规则未完成脱敏
- 调用运行设置更新 API 的脚本需提供新增必填对象 `codexPrivacyPolicy`，默认值为 `{ "enabled": false, "onError": "skip_rule", "rules": [] }`
- 移除旧插件 `middleware v3`、`upstream_adapter v1` 合同，相关插件须升级至 `middleware v4`、`upstream_adapter v2`，以 `fast_mode` 三态替代 `disable_fast`；升级前核对当前启用的插件
- 最低受支持的在线升级起点为 v3.18.1，更旧版本需手动安装完整发行包或镜像。管理 API 移除 `restartConfirmationSupported`，调用方统一通过 `restart/check` 检查并按需提交重启确认
- 本 fork 的安装使用发行附件与 `ghcr.io/zzwtsy/codex-proxy-rs` 镜像；仓库安装脚本的下载来源仍为上游。原生二进制使用 fork 在线更新时显式设置 `CPR_UPDATE_REPOSITORY=zzwtsy/codex-proxy-rs`
- HTTP 上游会明文传输 API Key 和请求内容，仅用于可信网络，公网连接使用 HTTPS
