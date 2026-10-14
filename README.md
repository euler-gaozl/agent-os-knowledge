# Agent OS Knowledge

独立 Go 知识库服务，提供固定版本上传、原始文件与历史正文留存、有界词法检索，以及 Milvus / LightRAG 适配。独立 Indexer 负责后台调度；Windows 已完成独立部署与更新/回滚检查，完整平台交付仍在进行。

## 数据所有权

Knowledge 独占 `knowledge` schema 和迁移程序。文件、文档、原始字节和分块在本域原子提交。导出的 `indexing` 包负责文件互斥、外部导入状态、回执和增量补齐，不暴露数据库句柄、DDL、上传/检索路由或调度器；Indexer 固定依赖该包。

LightRAG 上传中断进入 `unknown` 等待核对，已有 track ID 只轮询；Milvus 重试使用稳定 chunk ID。供应商配置固定在文件上。互斥为会话 advisory lock，不承诺持久化 fence 租约或外部 API 恰好一次。每次文件处理需一条连接持锁、另一条查询，连接池至少 2、默认 3。

## 检索与原文

模型检索返回独立文档句柄和短预览，明确全词/部分覆盖限制；完整文本及来源通过授权的 `document_read` 读取。默认八条本地命中下，编码预览控制在 5,500 字节以内，避免整个结果超过 Runtime 6,000 字节后被归档隐藏。Web 检索仍保留完整片段，旧工具回执不改写。

检索命中、图谱关系和字节哈希都不自动构成“事实已核验”。Milvus、LightRAG 和 Embedding 为外部能力，资源预算单独计算；百万文档/千万分块目标尚未完成容量验收。

## 来源档案与兼容性

`POST /v1/import-capture` 将 Sources 已保存的采集导入固定 Knowledge 版本。文档、分块、原始响应、正文、来源元数据及永久最低读取版本要求原子提交。新档案为 format 2，历史人工附件为 format 1；编译能力为 reader 2 / writer 2，数据契约 `knowledge-v0.7-source-archives`。

升级应先部署 reader 2 / writer 1 的桥接版本 `794780c`。一旦保存 format 2，回滚到桥接版后仍不能继续回滚到旧 reader 1。永久标记 `{"format":1,"minimum_reader":2}` 保留在 `knowledge-protocol:source-archives` 根，不能随普通资料 GC；移除 manifest 不能降低读取要求。缺失、冲突或损坏标记会拒绝兼容性检查。

`--source-archive-profile` 可在不连接数据库时报告编译能力，发布元数据必须匹配实际程序。Windows 维护器验证启动、升级、回滚和中断恢复；其他部署方式也需实现相同门槛。`SOURCES_URL` 默认回环 8101，多机须显式配置并授予 Knowledge 身份访问。

中台可通过 `POST /v1/files/{file-id}/source-archives` 为既有不可变上传附加原始字节、历史 UTF-8 提取与匹配 Unicode 范围。支持 identity/gzip，保留压缩字节并限制解压；存储字节、展开字节、正文各最多 1 MiB，完整 JSON 请求最多 2 MiB。raw/text/manifest 及文件引用原子提交，确定性 manifest 使重试幂等。列表每页 20 条，可按 manifest 下载原始、正文和清单。

人工提供的 URL、时间和提取方法仍是提交者陈述；哈希和相等片段只证明留存一致性。备份必须覆盖文档、产物和引用，不能只备份搜索索引。目前不启用自动删除。

## 构建、迁移与部署

需要 Go 1.27.2、PostgreSQL 18，独立使用本仓与提交的 `vendor`。

```powershell
$env:GOWORK='off'; $env:GOPROXY='off'
./scripts/Build.ps1
go test -mod=vendor ./...
go vet -mod=vendor ./...
```

也可 `go build -mod=vendor ./cmd/knowledge` 或 `docker build -t agent-os-knowledge .`。根据 [配置示例](deploy/service.example.json) 创建受保护配置，默认回环端口 8095、HTTP 并发 4。Registry/Runtime 默认回环 8096/8094，多机显式设置。服务启动不执行 DDL。

先创建不同的 Knowledge / Indexer 应用角色，再用独立 `MIGRATION_DATABASE_URL` 执行 `migrate --role <knowledge账号> --indexer-role <indexer账号>`。迁移不自动导入旧表、不撤销已有授权、不生成凭证，也不反向迁移。旧单仓 Indexer 回滚窗口内保留其 readiness 所需 artifacts/artifact_refs SELECT；新 Indexer 只需 documents/files/chunks SELECT 和 files/chunks UPDATE。

## 验证与容量工具

`TEST_DATABASE_URL` 创建隔离测试库/角色，不迁移指定原库；外部 Milvus/LightRAG/Embedding 测试显式 opt-in。旧/新 Indexer 可用 `TEST_OLD_INDEXER_BINARY`、`TEST_NEW_INDEXER_BINARY` 验证真实程序接力，供应商为模拟。真实进程恢复不能替代机器故障高可用验收。

手动 `retrieval.yml` 覆盖真实 Milvus、LightRAG 和固定 Qwen Embedding；拆仓后的工作流等待最终统一 Linux 验收。`cmd/capacity` 要求全新的可丢弃 `agentos_capacity` 库、`CAPACITY_DISPOSABLE=agentos-capacity-fixtures-only`、`CAPACITY_DATABASE_URL` 和 1,000..1,000,000（千的整数倍）文档数，不截断/复用已有 Knowledge 数据。`CAPACITY_REPORT` 指向新的绝对路径，默认 `/evidence/capacity.json`。按受限角色查询，失败夹具保留诊断。

本地 1,000 文档/10,000 分块检查已通过，但未施加 2 核/4 GiB 限制，只是迁移与查询冒烟测试。大量分块需要显式选择足够资源的 runner。Actions 日常关闭，[历史详细验收](docs/history/service-notes-20261014.en.md) 与 [集成计划](https://github.com/euler-gaozl/agent-os/blob/main/docs/windows-single-machine-plan.md) 保留具体通过、跳过及未验收项。
