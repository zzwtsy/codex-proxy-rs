<!-- prettier-ignore -->
<div align="center">

<img src="frontend/public/favicon.svg" alt="Codex Proxy RS" width="80" height="80" />

# Codex Proxy RS

面向 Codex 的自托管多账号 AI 网关

[![CI](https://github.com/zzwtsy/codex-proxy-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/zzwtsy/codex-proxy-rs/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/zzwtsy/codex-proxy-rs?display_name=tag&sort=semver&style=flat-square)](https://github.com/zzwtsy/codex-proxy-rs/releases)
[![GHCR](https://img.shields.io/badge/GHCR-codex--proxy--rs-2496ED?logo=docker&logoColor=white&style=flat-square)](https://github.com/zzwtsy/codex-proxy-rs/pkgs/container/codex-proxy-rs)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=flat-square)](LICENSE)

[快速开始](#快速开始) · [客户端接入](#客户端接入) · [文档](#文档)

</div>

本仓库是 [zyycn/codex-proxy-rs](https://github.com/zyycn/codex-proxy-rs) 的 fork，发行制品与镜像由 `zzwtsy/codex-proxy-rs` 提供。源码分支可能领先正式发行，安装时使用同一 Release 的镜像、部署文件和配置模板

网关集中管理 OpenAI / xAI 账号、代理、账号分组和客户端密钥，提供请求调度、额度控制、用量诊断、隐私规则与插件扩展。管理端支持管理员操作和密钥持有者自助查询

> [!NOTE]
> 项目提供 Responses API，不支持 `/v1/chat/completions`。两种存储模式都要求单网关副本，不能通过复制容器扩容

## 快速预览

[上游公开预览](https://codex-proxy-rs.ainz.cc) 用于了解管理端，登录信息与服务说明见[上游 README](https://github.com/zyycn/codex-proxy-rs#快速预览)。该环境由上游维护，不代表本 fork 的运行版本，也不提供真实模型调用；请勿导入真实账号或密钥

## 快速开始

先选择存储模式，再按部署文档完成安装：

| 模式 | 依赖与适用场景 | 安装入口 |
| --- | --- | --- |
| SQLite | 单网关、本地持久卷，无外部数据库服务 | [SQLite 新安装](deploy/README.md#sqlite-新安装) |
| PostgreSQL + Redis | 使用独立数据库与协调服务，仍须保持单副本 | [手动安装](deploy/README.md#手动安装) |

已有实例先阅读[镜像升级与源码构建](deploy/README.md#镜像升级与源码构建)，不要覆盖配置或将切换存储模式当作数据迁移

### 一键安装

仓库内的 [install.sh](deploy/install.sh) 下载来源仍固定为上游 `zyycn/codex-proxy-rs`，不能通过环境变量切换仓库。安装本 fork 请使用下面的手动安装入口；脚本的 `CPR_RELEASE_TAG` 只选择版本，不改变制品来源

### 手动安装

Linux amd64/arm64 的完整命令、密码与文件权限设置见[部署与运维](deploy/README.md#手动安装)。其他平台可从 [fork Releases](https://github.com/zzwtsy/codex-proxy-rs/releases) 获取对应二进制，运行源码见[开发指南](docs/development.md#启动宿主)

### 登录管理端

打开 `http://127.0.0.1:8080`，使用 `admin@cpr.local` 和安装时设置的初始密码登录。从其他设备访问时配置 [HTTPS 反向代理](deploy/README.md#公网访问)

API Key 持有者可在登录页切换身份，查看自己的用量和额度，通过「密钥配置」复制客户端配置

### 添加账号与客户端密钥

1. 添加上游账号，完成授权或导入并检查账号状态
2. 按需建立账号分组；创建客户端密钥并绑定分组，**不选分组表示可使用全部账号**
3. 打开密钥的「使用密钥」，复制配置并接入客户端

完整操作与授权边界见[使用指南](docs/usage.md)

## 客户端接入

Codex CLI / 桌面端使用「使用密钥」生成的配置，合并后重启客户端；生图、WebSocket 与排障见[客户端配置](deploy/README.md#客户端配置)

其他 Responses 客户端使用部署地址下的 `/v1` 和管理端创建的客户端密钥。管理员密码、管理 API Key 和上游账号 Token 都不能替代该密钥

```bash
curl http://127.0.0.1:8080/v1/models \
  -H 'Authorization: Bearer <client-api-key>'
```

模型列表查询成功仅说明密钥可查询目录，实际推理还取决于账号状态、模型权限及上游可用性

## 文档

| 目标 | 入口 |
| --- | --- |
| 安装、升级、备份或恢复 | [部署与运维](deploy/README.md) |
| 管理账号、分组、密钥和用量 | [使用指南](docs/usage.md) |
| 接入客户端或调用接口 | [客户端配置](deploy/README.md#客户端配置) · [API 参考](docs/api.md) |
| 安装与使用插件 | [插件使用](docs/plugins.md) |
| 编写与打包插件 | [插件 SDK](backend/crates/gateway-plugin/sdk/README.md) · [打包工具](backend/apps/plugin-cli/README.md) |
| 运行源码与维护宿主 | [开发与源码联调](docs/development.md) · [系统架构](docs/architecture.md) · [管理端主题](docs/theme.md) · [数据库迁移](backend/migrations/README.md) |
| 参与协作 | [贡献与审查](CONTRIBUTING.md) |

## 社区

本 fork 的问题可在[本仓库 Issues](https://github.com/zzwtsy/codex-proxy-rs/issues)反馈，说明运行版本、存储模式和复现条件，提交日志前先脱敏

通用使用讨论可参考[上游 Discussions](https://github.com/zyycn/codex-proxy-rs/discussions)。感谢 [LINUX DO](https://linux.do) 社区提供技术交流平台

## 许可证

本项目基于 [Apache License 2.0](LICENSE) 开源
