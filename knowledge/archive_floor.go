package knowledge

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	. "github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/jackc/pgx/v5"
)

const archiveFloorRoot = "knowledge-protocol:source-archives"
const archiveFloorBody = `{"format":1,"minimum_reader":2}`

type SourceArchiveProfile struct {
	Format int `json:"format"`
	Reader int `json:"reader"`
	Writer int `json:"writer"`
}

func ArchiveProfile() SourceArchiveProfile {
	writer := 1
	if CaptureImportWritesEnabled {
		writer = 2
	}
	return SourceArchiveProfile{Format: 1, Reader: 2, Writer: writer}
}

type archiveFloorReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// One SQL snapshot prevents a concurrent first import from appearing as a
// format-2 manifest without its marker. Mode 0 checks only the immutable marker,
// mode 1 also examines manifests if the marker is absent, mode 2 audits all.
// Startup and operations use mode 2. Normal readiness uses the indexed marker;
// ingestion audits an absent marker before creating either new value.
const archiveFloorQuery = `WITH marker AS MATERIALIZED (
 SELECT r.hash,encode(substring(a.body FROM 1 FOR 257),'hex') body,octet_length(a.body) bytes
 FROM artifact_refs r LEFT JOIN artifacts a ON a.hash=r.hash
 WHERE r.root='knowledge-protocol:source-archives' ORDER BY r.hash LIMIT 2
), manifests AS MATERIALIZED (
 SELECT CASE WHEN a.body IS NULL OR octet_length(a.body)>16384 OR encode(sha256(a.body),'hex')<>a.hash THEN 'invalid'
 ELSE (convert_from(a.body,'UTF8')::json->'format')::text END format
 FROM artifact_refs r LEFT JOIN artifacts a ON a.hash=r.hash
 WHERE ($1::int=2 OR ($1::int=1 AND NOT EXISTS(SELECT 1 FROM marker)))
 AND r.root COLLATE "C" >= 'knowledge-source:' AND r.root COLLATE "C" < 'knowledge-source;' AND r.root ~ '^knowledge-source:[a-f0-9]{32}:manifests$'
)
SELECT json_build_object('markers',(SELECT coalesce(json_agg(marker),'[]'::json) FROM marker),
 'has_manifest',EXISTS(SELECT 1 FROM manifests WHERE format IN ('1','2')),
 'invalid_manifest',EXISTS(SELECT 1 FROM manifests WHERE format IS NULL OR format NOT IN ('1','2')))::text`

func archiveRequirement(ctx context.Context, db archiveFloorReader, mode int) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var raw []byte
	if err := db.QueryRow(ctx, archiveFloorQuery, mode).Scan(&raw); err != nil {
		return 0, errors.New("source archive compatibility probe failed")
	}
	var facts struct {
		Markers []struct {
			Hash  string `json:"hash"`
			Body  string `json:"body"`
			Bytes int    `json:"bytes"`
		} `json:"markers"`
		HasManifest     bool `json:"has_manifest"`
		InvalidManifest bool `json:"invalid_manifest"`
	}
	if json.Unmarshal(raw, &facts) != nil || facts.InvalidManifest || len(facts.Markers) > 1 {
		return 0, errors.New("invalid source archive protocol state")
	}
	if len(facts.Markers) == 0 {
		if facts.HasManifest {
			return 0, errors.New("source archive exists without its persistent reader requirement")
		}
		return 1, nil
	}
	m := facts.Markers[0]
	if m.Hash != Hash([]byte(archiveFloorBody)) || m.Body != hex.EncodeToString([]byte(archiveFloorBody)) || m.Bytes != len(archiveFloorBody) {
		return 0, errors.New("source archive reader requirement is corrupt or unknown")
	}
	return 2, nil
}

func CheckArchiveCompatibility(ctx context.Context, db archiveFloorReader) error {
	_, err := archiveRequirement(ctx, db, 2)
	return err
}

func retainArchiveFloor(ctx context.Context, tx pgx.Tx) error {
	hash, err := putArtifact(ctx, tx, []byte(archiveFloorBody), "application/vnd.agentos.archive-reader-requirement+json", archiveFloorRoot)
	if err != nil {
		return err
	}
	var saved []byte
	if err = tx.QueryRow(ctx, "SELECT body FROM artifacts WHERE hash=$1", hash).Scan(&saved); err != nil {
		return err
	}
	if !bytes.Equal(saved, []byte(archiveFloorBody)) {
		return errors.New("source archive reader requirement artifact is corrupt")
	}
	return nil
}
