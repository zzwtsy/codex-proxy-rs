# v3.23.0

## 新增功能

- 系统设置新增 Codex 隐私策略，支持按字段路径进行正则替换、固定值设置、键名改写和条件删除；可处理工作区 metadata、Desktop Git 上下文、环境文本及请求正文和请求头
- 提供工作区路径、Git 远端等隐私规则预设，支持调整执行顺序、单条启停和样本测试预览；执行失败时可选择跳过当前规则或阻止请求发送
- OpenAI API Key 账号的上游地址支持 HTTP 内网 IP 和容器名，便于接入可信网络中的上游服务

## 修复与改进

- 请求位置覆盖适配 Codex Desktop 自动附加的时间上下文，使设置中的日期、时区覆盖同时作用于 CLI 与 Desktop 的对应上下文
- Images 生图与编辑请求按图片模型执行账号模型限制，并排除已知为 Free 套餐的 OAuth 账号；插件改写模型后也会复核，避免绕过模型访问规则

## 升级事项

- 新增数据库迁移 `0026_codex_privacy_policy.sql`，隐私策略默认关闭；只处理经过网关的请求，不控制客户端直连发送的遥测
- 隐私规则没有字段保护名单，认证、会话等字段也可改写，配置者需自行确认对请求行为的影响；选择跳过失败规则时，该条规则的脱敏不会生效
- 调用运行设置更新 API 的脚本或客户端需提供新增的必填对象 `codexPrivacyPolicy`；默认值为 `{ "enabled": false, "onError": "skip_rule", "rules": [] }`
- 移除已结束兼容期的插件 `middleware v3`、`upstream_adapter v1` 接口；相关插件需升级至 `middleware v4`、`upstream_adapter v2`，以 `fast_mode` 三态替代 `disable_fast`
- 最低受支持的在线升级起点为 v3.18.1；更旧版本需手动安装 v3.18.1 或新版完整发行包／镜像。管理 API 移除 `restartConfirmationSupported` 字段，调用方应统一通过 `restart/check` 检查并按需提交重启确认
- HTTP 上游会明文传输 API Key 和请求内容，仅用于可信网络；公网连接仍建议使用 HTTPS
