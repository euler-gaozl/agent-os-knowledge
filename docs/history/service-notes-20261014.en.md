# 历史实现与验收记录

以下为 2026-10-14 整理中文首页前的原始英文记录，保留具体参数、证据编号及历史状态。相对路径以仓库根目录为基准；当前状态以根目录 README 和集成计划为准。

---

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

The opt-in `retrieval.yml` workflow now belongs to this repository, with the
Milvus configuration and controlled LightRAG model fixture. It uses real
Milvus/LightRAG and pinned Qwen embedding inference, rejects skipped tests,
and has no commit trigger. These newly transferred workflows await the final
unified Linux run; old monorepo results are historical evidence only.

`cmd/capacity` runs the owner's real migration and lexical retrieval against
SQL-generated fixtures. It requires `CAPACITY_DISPOSABLE=agentos-capacity-fixtures-only`,
`CAPACITY_DOCUMENTS` (1000..1000000, multiple of 1000), and a privileged
`CAPACITY_DATABASE_URL` targeting a **new disposable `agentos_capacity` database**.
Use an absolute new `CAPACITY_REPORT` path (default `/evidence/capacity.json`).
It refuses existing Knowledge/marker schemas and never drops or truncates them.
Failed fixtures and their uniquely named NOLOGIN roles remain in the disposable
cluster for diagnosis; dispose the entire dedicated cluster when no longer needed.
Measured queries run under the migrated Knowledge role via SET ROLE, not the
administrator. The report records revision isolation, present/absent queries,
latencies and explicit scope. It does not claim enforced resource limits on its
own, upload throughput, ranking quality or whole-platform capacity.

Manual/reusable `capacity.yml` defaults to 1,000 documents; ten million chunks
require explicitly selecting one million documents on a sufficiently large
disposable runner. There is no push/PR trigger. The workflow retains actual
container limits and resource samples. New owner-scale Linux execution remains
pending; the local 1,000-document check is a migration smoke test only.

Local proof `p2-owner-coverage-58d357e0cd7b4e71bb4ea51cf7661743` used a new
PostgreSQL 18 cluster: 1,000 documents/10,000 chunks, zero query errors,
absent-query zero hits, revision isolation, and rejection of fixture reuse.
The query path and migrator are this owner's code; no paid model calls or
external retrieval backend were involved. The dedicated cluster stopped after
the check and its evidence is retained. No resource limits were imposed on this
Windows smoke run, so its latency is not the 2 CPU/4 GiB acceptance result.

Local model retrieval now returns distinct document handles and short previews,
with explicit all-terms and partial-coverage caveats. The Web query retains full
snippets; `document_read` remains the authorized path to complete text and source
metadata. With the existing eight-hit/local-ID limits, the encoded preview stays
under 5,500 bytes, below Runtime's 6,000-byte whole-result archival threshold.
This avoids hiding later document IDs inside paged JSON. Committed old tool
receipts are unchanged. Local PostgreSQL tests: 20 pass, five explicit opt-in
skips (external backends and Indexer binary/helper); vet/build pass. Real-model
delivery evidence and limitations belong to the integration delivery ledger.

## Retained source archives

The capture importer reads and writes archive format 2, which records an
authenticated Sources capture ID, requested/final URL, extractor version and
deterministic import format. Historic operator-supplied archives remain format 1.
The production `/v1/import-capture` route imports a retained Sources capture
into a fixed Knowledge revision. Document, chunks, original bytes, extracted
text, provenance and the permanent reader requirement commit atomically.
The compile-time profile is reader 2 / writer 2, with data contract
`knowledge-v0.7-source-archives`. Deploy reader bridge `794780c` first; its
profile is reader 2 / writer 1 and its capture route remains disabled.
`SOURCES_URL` defaults to loopback port 8101; remote deployments must configure
the fixed service URL and authorize the Knowledge identity on Sources. Both
capability discovery and bundle reads forbid redirects and remain bounded.

Deployments must enforce the retained-data reader requirement during startup,
update, rollback and interrupted-release recovery, including writer → bridge →
legacy rollback. A format-2-capable bridge does not make legacy readers safe
after format 2 has been persisted. The Windows integration enforces these gates
and tests actual service binaries. Other deployment managers must implement the
same contract; a direct binary launch cannot validate its rollback package.
Restore verification must understand both formats and the permanent marker.

`--source-archive-profile` reports the compiled capabilities without opening a
database. The release descriptor must match the actual executable. The importer retains
the canonical `{"format":1,"minimum_reader":2}` artifact under the permanent
`knowledge-protocol:source-archives` root in the same transaction as format 2.
That protocol root is not ordinary uploaded content and must never be garbage
collected. Removing manifests cannot lower the floor. Missing, conflicting or
corrupt markers and unknown manifest formats fail compatibility checks;
idempotent retry cannot silently repair ambiguous protocol state. Startup audits
retained formats; normal readiness checks the indexed fixed marker only.

Local reader-floor candidate checks: 30 top-level tests and 24 subtests passed,
five existing external-provider/Indexer opt-ins skipped; vet and build passed.
The Windows integration separately tests its real database probes and manager
entrypoints, including a killed client whose PostgreSQL COMMIT is still pending.
The actual writer process has also passed import-then-release-failure recovery
to the reader bridge, member downloads, incompatible legacy rollback refusal
and same-request retry without duplicate files. That test uses controlled HTTP
peers and an isolated database. Sources migration, UI and actual new-source
acceptance are separate integration deliverables.


Control callers can attach original source bytes and their historical UTF-8
extraction to an existing immutable upload using
`POST /v1/files/{file-id}/source-archives`. The caller supplies the uploaded file
SHA-256, source metadata, exact raw/text hashes and matching Unicode ranges in
both the extraction and the uploaded document. Raw storage supports identity or
gzip; compressed bytes are preserved and decompression is bounded. Raw stored
bytes, expanded bytes and extraction are each limited to 1 MiB; the **entire JSON
request is limited to 2 MiB**, including base64 and escaping overhead.

Raw/text/manifest artifacts and their file-scoped retention references commit in
one transaction. Deterministic manifests make per-file retries idempotent. The
manifest root is distinct from the payload root, independent of globally
deduplicated artifact MIME types. No file, document, index or schema is rewritten.
Listing uses `GET` on the same endpoint (20 results, `after` hash cursor).
`GET /v1/files/{file-id}/source-archives/{manifest-sha256}/{raw|text|manifest}`
checks the complete association and returns a safe attachment with its hash.
Other service identities cannot call these management routes.

URL, capture time and extraction method remain operator assertions. Byte hashes
and equal spans establish retention consistency, not factual accuracy or proof
that the extracted text was derived from the supplied source. Automatic deletion
is not enabled. Standard backups must include the Knowledge artifacts, references
and immutable target documents; restoring only a search index is insufficient.

Local PostgreSQL suite: 22 top-level tests and 16 subtests passed, five explicit
opt-in tests skipped; vet and independent build passed. New coverage includes
concurrent retries, commit-stage rollback, corruption, scoped member downloads,
authorization, gzip bounds and Unicode association. This does not establish
HTTP lost-reply or process-kill recovery. Deployment/real archive restore evidence
belongs to the integration ledger.
