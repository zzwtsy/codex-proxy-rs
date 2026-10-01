# v3.20.0-beta.1

## 功能改进

- Compose 部署将 PostgreSQL 和 Redis 密码移至安装目录根目录的 `.env`，安装器将文件权限设为 `0600`

## 问题修复

- 修正用量图表的时间标签，不再额外显示时区偏移

## 升级说明

- 旧版 Compose 部署需手动将 `deploy/config.yaml` 中现有的 PostgreSQL 和 Redis 密码原样迁移到安装目录根目录 `.env` 的 `CPR_DATABASE_PASSWORD` 和 `CPR_REDIS_PASSWORD`，清空 YAML 中的密码字段并将 `.env` 权限设为 `0600`。无需修改数据库密码或删除 Redis 数据；一键安装器不会自动迁移已有部署
