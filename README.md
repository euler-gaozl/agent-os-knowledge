# Agent OS Knowledge

Independent Go service for versioned knowledge uploads, retained source text,
bounded lexical retrieval, and Milvus/LightRAG adapters. Source extraction is
verified in an independent offline clone and deployed on Windows with separate
Knowledge/Indexer upgrade and rollback checks. Full platform delivery continues.

## Ownership and Indexer contract

Knowledge owns the `knowledge` schema and its sole migration binary. Files,
documents, original bytes and chunks still commit atomically in this domain.
The exported `indexing` package owns file locking, durable external-import
states, receipts and incremental backfill. It exports no database field, DDL,
upload/retrieval routes, or scheduler. The separately deployed Indexer consumes
a fixed release of this package with committed vendor. Provider encoding and
lexical rules are private shared code within this owner, not platform SDK SQL.

`ProcessFile` retains the existing session advisory lock across provider work.
LightRAG interrupted uploads become `unknown`, awaiting receipt reconciliation;
polling a saved track ID does not re-upload. Milvus retries retain stable chunk
IDs. Provider configuration is the immutable config saved with the file.
This is not a persistent fenced lease or an exactly-once external API claim.
Indexer processes files sequentially and requires at least two pool connections:
one holds the file lock while the other serves queries. Do not increase file
concurrency to the pool limit without redesigning that connection ownership.

## Build, migration and deployment

Go 1.27.2; `GOWORK=off GOPROXY=off go build -mod=vendor ./cmd/knowledge`.
PowerShell: `./scripts/Build.ps1`. Docker: `docker build -t agent-os-knowledge .`.
Only this repository and its committed vendor are needed.

Provision two distinct database roles first. Run `migrate --role <knowledge>
--indexer-role <indexer>` with the privileged connection supplied exclusively as
`MIGRATION_DATABASE_URL`. Migration owns DDL and explicit grants in one
transaction, retaining the existing advisory migration lock. It does not
automatically import old public tables, revoke pre-existing grants, provision
credentials, or reverse schema changes on rollback. Fresh Indexer roles get
only SELECT on documents/files/chunks and UPDATE on files/chunks. Legacy roles
may retain earlier broader grants until separately audited and retired.
The archived monorepo Indexer checks `artifacts` and `artifact_refs` in its
startup readiness probe. Keep SELECT on those two tables while that binary is
an allowed rollback target; only retire those legacy grants after ending that
rollback window. New Indexer-only deployments do not need them. Do not revoke
them and then claim the old executable remains rollback-compatible.

Use the template in `deploy/service.example.json` with a protected real config.
Knowledge defaults to `127.0.0.1:8095`. Empty Registry/Runtime addresses retain
the existing local ports 8096/8094, applied after configuration loading.
Set explicit container addresses when deploying remotely. HTTP concurrency
defaults to four; database pool defaults to three connections. Normal startup
uses the application account and never runs DDL.

Health is process liveness; authenticated readiness checks this domain's
schema. Downstream/model availability is separate. Keep
`deploy/release-contract.json`, the fixed SDK version, and the Indexer module
version in the integration release lock. Updates/rollback must preserve old
files and pending import states; rollback does not roll back database DDL.

## Verification

`go test -mod=vendor ./...` runs local tests. PostgreSQL tests require an
explicit `TEST_DATABASE_URL` for fixture administration and create independent
disposable databases and NOLOGIN roles; they never migrate that URL's database.
Optional live Milvus/LightRAG/Embedding tests remain explicitly opt-in.
No real credential or paid model call belongs in fixtures or CI.

Actual cross-version executable handoff is opt-in with absolute
`TEST_OLD_INDEXER_BINARY` and `TEST_NEW_INDEXER_BINARY` paths plus the disposable
PostgreSQL fixture URL. It creates isolated databases, two local Indexer
processes in sequence, and a synthetic provider. The Windows check passed old
to new interrupted-upload recovery (`unknown`, one upload) and new to old saved
track polling (`ready`, one upload/one poll). This is process recovery on one
machine; it does not prove real LightRAG availability or machine-failure HA.
