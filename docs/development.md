# 开发与源码联调

本文说明开发环境、服务启动与跨仓库联调，命令均从宿主仓库根目录执行

协作与检查要求见 [贡献与审查](../CONTRIBUTING.md)，部署实例的配置与数据维护见 [部署与运维](../deploy/README.md)

| 任务 | 入口 |
| --- | --- |
| 修改宿主前后端 | [环境与依赖](#环境与依赖) → [启动宿主](#启动宿主) |
| 定位模块与共享实现 | [源码入口](#源码入口) |
| 同时修改 UI 或插件 | [源码联调](#源码联调) → [启动与检查](#启动与检查) |
| 验证本地 SDK | [本地 SDK 验证](#本地-sdk-验证) |
| 提交跨仓库改动 | [编辑与提交](#编辑与提交) |

## 环境与依赖

需要 Node.js 24、pnpm 12.6.0 和 Rust 1.97 工具链。服务可使用 PostgreSQL + Redis，也可用单文件 SQLite 本地运行

各前端的 `packageManager` 固定 pnpm 版本，宿主 CI 和 Docker 从 `frontend/package.json` 读取

Rust 工具链声明位于 `backend/rust-toolchain.toml`，从仓库根目录运行 Cargo 时显式选择该工具链；
`--manifest-path` 只选择 manifest，不切换 rustup 按当前目录选定的工具链

```bash
git clone https://github.com/zzwtsy/codex-proxy-rs.git
cd codex-proxy-rs
pnpm --dir frontend install --frozen-lockfile
```

前端依赖、锁文件与 ESLint 配置由 `frontend/` 管理，根目录不维护 Node 包

- **版本固定**：`@codex-proxy/ui` 使用固定 GitHub 标签或提交，锁文件记录实际提交、源码归档地址及完整性摘要
- **安装与构建**：pnpm 按 UI 自身锁文件准备依赖，执行 `prepack` 生成 JS、CSS 和类型声明，普通 `dev`、`build`、发行 CI 和 Docker 使用编译产物，无需初始化子模块
- **构建许可**：`frontend/pnpm-workspace.yaml` 的 `allowBuilds` 按仓库放行自有 UI 包脚本，升级时核对源码并更新锁文件，无需逐次修改许可，不开放全部依赖脚本

## 启动宿主

命令从仓库根目录执行，不覆盖已有 `deploy/config.yaml`。本地开发优先使用 SQLite 快速启动；验证 PostgreSQL + Redis 行为时选择对应模式。两种模式的数据库互不迁移

### SQLite 本地启动

```bash
set -e
test ! -e deploy/config.yaml
mkdir -p .runtime/data .runtime/logs
cp deploy/config.example.sqlite.yaml deploy/config.yaml
chmod 0600 deploy/config.yaml
```

在配置中填写符合策略的 `admin.default_password`：至少 12 个字符，不含 `$`，不能使用常见弱口令。由当前用户运行后端，数据库保存在 `.runtime/data/codex-proxy.sqlite3`

```bash
cargo +1.97.0 run --manifest-path backend/Cargo.toml -p codex-proxy-rs --locked
```

后端从当前目录向上查找 `deploy/config.yaml`，数据、日志和静态资源的相对路径以配置目录解析，SQLite 数据库相对路径以运行数据目录解析。可写启动自动建库并执行迁移，不需外部服务

### PostgreSQL + Redis 本地启动

使用源码仓库自带模板准备一个新的开发环境，需要 Docker Compose 和 OpenSSL。以下命令不下载发行版模板，只启动依赖服务：

```bash
set -e
test ! -e deploy/config.yaml
test ! -e .env
cp deploy/config.example.yaml deploy/config.yaml
chmod 0600 deploy/config.yaml
mkdir -p .runtime/postgres .runtime/redis .runtime/data .runtime/logs
(umask 077; printf 'CPR_DATABASE_PASSWORD=%s\nCPR_REDIS_PASSWORD=%s\n' \
  "$(openssl rand -hex 24)" "$(openssl rand -hex 24)" > .env)
docker compose --env-file .env -f deploy/compose.yaml up -d --wait postgres redis
```

原生后端不读取 `.env`，在 `deploy/config.yaml` 的 `store.database.password`、`store.redis.password` 填写相同服务密码，或由启动终端提供 `CPR_DATABASE_PASSWORD`、`CPR_REDIS_PASSWORD`。确认数据库与 Redis URL 指向 Compose 发布的本机端口，不使用容器主机名，填写管理员初始密码后运行上面的 Cargo 命令

### 管理端与启动验收

另开终端运行：

```bash
curl -i http://127.0.0.1:8080/healthz
pnpm --dir frontend dev
```

`204` 表示服务健康，不证明上游账号可用。访问 Vite 输出的本机地址登录管理端，后端代理由 `frontend/vite.config.ts` 配置；验证 WebSocket 时直接连接后端。纯后端开发不需先构建静态页面，正式页面验证使用构建后的资源

首次使用按[使用指南](usage.md)添加账号和密钥。前端开发服务不应暴露到公网，联调完成后停止开发进程

## 验证命令

按变更范围选择检查，完整交付要求见[贡献与审查](../CONTRIBUTING.md#验证)。仓库根目录没有 Cargo manifest，`--manifest-path` 不切换 rustup 工具链

```bash
cargo +1.97.0 fmt --all --manifest-path backend/Cargo.toml -- --check
RUST_MIN_STACK=16777216 cargo +1.97.0 clippy --manifest-path backend/Cargo.toml --all-targets --all-features --locked -- -D warnings
RUST_MIN_STACK=16777216 cargo +1.97.0 test --manifest-path backend/Cargo.toml --test main --locked
pnpm --dir frontend lint
pnpm --dir frontend build
```

线程栈设置与 CI 一致。PostgreSQL / Redis 测试使用[专用测试库](../backend/migrations/postgres/README.md#本地测试库)，不得指向部署数据；缺少变量时相关本地测试会跳过，不能报告为已验证。SQLite 测试使用隔离临时数据库

插件 Runtime 的真实子进程与持久化测试使用 `CPR_PLUGIN_TEST_DATABASE_URL` 和 `CPR_PLUGIN_TEST_REDIS_URL`；未提供时复用 `CPR_TEST_DATABASE_URL` 和 `CPR_TEST_REDIS_URL`，密码与隔离要求同 Store 测试，CI 缺少配置直接失败

测试归档缓存在 Cargo 临时目录的 `plugin-packages-v1/`，按含 worker 摘要的清单复用；每项测试仍独立创建会话、子进程和 Store，缓存不承载可变运行状态。独立插件验证按其仓库的 `examples/workbench/README.md` 执行

纯文档改动检查内容、链接、锚点与相关命令，不必重建前后端；构建通过也不能替代界面与集成验收

## 源码入口

后端职责和依赖方向见 [Workspace 边界](architecture.md#3-workspace-边界)，页面与共享逻辑见 [前端模块职责](architecture.md#34-前端模块职责)。
先按所属模块定位，再跟踪请求、状态与展示的调用关系

## 源码联调

宿主、组件库和官方插件保留独立仓库，通过 Git 子模块在同一目录开发：

| 目录 | 仓库与职责 |
| --- | --- |
| `frontend` / `backend` | 宿主管理端与 Rust 网关 |
| `modules/ui` | `codex-proxy-ui`，共用组件与主题 |
| `modules/plugins` | `codex-proxy-plugins`，官方插件示例 |

### 检出与安装

首次检出可同时下载子模块：

```bash
git clone --recurse-submodules https://github.com/zzwtsy/codex-proxy-rs.git
cd codex-proxy-rs
```

已有仓库运行 `git submodule update --init --recursive`。子模块固定到具体提交；拉取宿主更新后，同样用此命令对齐。执行前先保存子模块中的改动，避免切换正在开发的分支

在宿主根目录安装各项目依赖：

```bash
pnpm --dir modules/ui install --frozen-lockfile
pnpm --dir frontend install --frozen-lockfile
pnpm --dir modules/plugins/examples/workbench/frontend install --frozen-lockfile
```

### 启动与检查

在各自终端启动所需服务：

```bash
pnpm --dir frontend dev:source                            # 管理端，引用本地 UI
pnpm --dir modules/plugins/examples/workbench/frontend dev:source  # 插件页面，引用本地 UI
pnpm --dir modules/ui dev                                 # 组件库交互示例
```

管理端需要[已启动的网关](#启动宿主)。插件独立预览的宿主桥使用模拟数据；已安装插件读取包内静态资源，修改后需重新构建、打包和安装

`dev:source` 使用 Vite 的 `source` 模式，由 `@codex-proxy/ui/vite` 的 `CodexProxyUI` 适配器将公开入口解析到 `modules/ui`，并统一处理共享依赖、源码预构建排除与开发服务的文件访问范围。入口映射以 UI 包 `exports` 中的 `codex-proxy-source` 条件为准，由 Vite 热更新。插件单独检出时仍用普通 `dev`；`source` 模式需要上述子模块目录结构

```bash
pnpm --dir frontend build:source
pnpm --dir modules/plugins/examples/workbench/frontend build:source
```

`build:source` 用各项目的 `tsconfig.source.json` 检查源码类型，管理端构建到 `frontend/node_modules/.vite/source-dist`，示例构建到其前端项目的 `.vite/source-dist`，不改写正式依赖、锁文件或 `dist`。
源码联调 CI 显式检出子模块、核对 UI 提交锁定并验证这两个命令

### 本地 SDK 验证

插件正式 SDK 依赖仍由其 Cargo 锁文件固定。需要验证尚未发布的 SDK 修改时，在临时副本运行 Cargo patch，避免改写插件锁文件。以下命令从宿主根目录执行：

```bash
(
  set -e
  cpr_sdk_check_dir=$(mktemp -d)
  trap 'rm -rf "$cpr_sdk_check_dir"' EXIT
  mkdir "$cpr_sdk_check_dir/backend"
  cp modules/plugins/examples/workbench/backend/Cargo.{toml,lock} "$cpr_sdk_check_dir/backend/"
  cp -R modules/plugins/examples/workbench/backend/{src,tests} "$cpr_sdk_check_dir/backend/"
  cp modules/plugins/examples/workbench/plugin.json "$cpr_sdk_check_dir/"
  RUST_MIN_STACK=16777216 CARGO_TARGET_DIR="$PWD/backend/target/plugin-development" \
    cargo +1.97.0 test --manifest-path "$cpr_sdk_check_dir/backend/Cargo.toml" \
    --config "patch.\"https://github.com/zyycn/codex-proxy-rs.git\".gateway-plugin-sdk.path=\"$PWD/backend/crates/gateway-plugin/sdk\""
)
```

## 编辑与提交

直接打开宿主目录即可开发，分别在对应仓库目录操作 Git

1. 在要修改的子模块内先创建分支，例如 `git -C modules/ui switch -c feat/select`。初次初始化的子模块通常处于 detached HEAD
2. 在子模块运行自身检查，并在宿主根目录运行所需联调检查
3. 分别提交、推送并合并组件库或插件 PR。宿主只记录子模块提交号，不能替代子仓库的提交与推送
4. UI 更新后，将管理端和插件页面的 UI 依赖同步到同一固定标签或提交，重新生成各自锁文件，再更新宿主的子模块指针；CI 会核对三处是否一致
5. 提交宿主改动及 `modules/ui`、`modules/plugins` 指针。指针必须指向远程可取得的提交，不能只存在于开发机

更新子模块指针不会自动升级消费方依赖。正式依赖不能写成开发机路径或浮动分支；SDK 仍由插件仓库固定到包含所需合同的宿主提交
