# SQLite 迁移

SQLite 使用独立于 PostgreSQL 的迁移集，以单文件空库作为初始化目标。迁移编号只在本目录内递增；PostgreSQL 的迁移历史不会被复制或修改。

已冻结迁移按字节不可变。后续 schema 变更必须新增编号 SQL，并将其 SHA-256 追加到 `.frozen-sha256`。CI 会检查已登记迁移未变、目录中的 SQL 均已登记，以及冻结清单相对 base 只追加。

```bash
cd backend/crates/gateway-store/src/sqlite/migrations
sha256sum --check --strict .frozen-sha256
```
