package knowledge

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	. "github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"github.com/jackc/pgx/v5"
)

var archiveID = regexp.MustCompile(`^[a-f0-9]{32}$`)
var archiveHash = regexp.MustCompile(`^[a-f0-9]{64}$`)

// Ranges are zero-based Unicode code points, end-exclusive, in the submitted
// extraction and the immutable Knowledge document respectively.
type SourceSpan struct {
	Start         int `json:"start"`
	End           int `json:"end"`
	DocumentStart int `json:"document_start"`
	DocumentEnd   int `json:"document_end"`
}

type SourceArchiveInput struct {
	FileHash         string       `json:"file_sha256"`
	URL              string       `json:"url"`
	CapturedAt       string       `json:"captured_at"`
	MediaType        string       `json:"media_type"`
	ExtractionMethod string       `json:"extraction_method"`
	RawHash          string       `json:"raw_sha256"`
	RawEncoding      string       `json:"raw_encoding"`
	TextHash         string       `json:"text_sha256"`
	Raw              []byte       `json:"raw"`
	Text             string       `json:"text"`
	Spans            []SourceSpan `json:"spans"`
}

type SourceArchive struct {
	Format           int          `json:"format"`
	FileID           string       `json:"file_id"`
	DocumentID       string       `json:"document_id"`
	Knowledge        Ref          `json:"knowledge"`
	FileHash         string       `json:"file_sha256"`
	URL              string       `json:"url"`
	CapturedAt       string       `json:"captured_at"`
	MediaType        string       `json:"media_type"`
	ExtractionMethod string       `json:"extraction_method"`
	RawHash          string       `json:"raw_sha256"`
	RawEncoding      string       `json:"raw_encoding"`
	StoredRawHash    string       `json:"stored_raw_sha256"`
	StoredRawBytes   int          `json:"stored_raw_bytes"`
	RawBytes         int          `json:"raw_bytes"`
	TextHash         string       `json:"text_sha256"`
	TextBytes        int          `json:"text_bytes"`
	Spans            []SourceSpan `json:"spans"`
	Provenance       string       `json:"provenance"`
}

type SourceArchiveRecord struct {
	Hash    string        `json:"manifest_sha256"`
	Archive SourceArchive `json:"archive"`
}

type SourceArchivePage struct {
	Items      []SourceArchiveRecord `json:"items"`
	NextCursor string                `json:"next_cursor"`
}

const archiveProvenance = "operator_imported_archive: URL, capture time and extraction method are submitter assertions; hashes and matching spans verify retention consistency, not raw-to-text derivation, completeness or factual accuracy"

func sourceArchiveError(status int, message string) error {
	return &servicehttp.StatusError{Status: status, Message: message}
}

func decodeArchivedRaw(in SourceArchiveInput) ([]byte, error) {
	if len(in.Raw) == 0 || len(in.Raw) > 1<<20 {
		return nil, sourceArchiveError(400, "stored raw archive must contain 1 byte to 1 MiB")
	}
	if in.RawEncoding == "identity" {
		return in.Raw, nil
	}
	if in.RawEncoding != "gzip" {
		return nil, sourceArchiveError(400, "raw_encoding must be identity or gzip")
	}
	r, err := gzip.NewReader(bytes.NewReader(in.Raw))
	if err != nil {
		return nil, sourceArchiveError(400, "invalid gzip source archive")
	}
	defer r.Close()
	b, err := io.ReadAll(io.LimitReader(r, (1<<20)+1))
	if err != nil || len(b) == 0 || len(b) > 1<<20 {
		return nil, sourceArchiveError(400, "invalid gzip or expanded raw archive exceeds 1 MiB")
	}
	return b, nil
}

func validateSourceArchive(in SourceArchiveInput) error {
	if !archiveHash.MatchString(in.FileHash) || !archiveHash.MatchString(in.RawHash) || !archiveHash.MatchString(in.TextHash) {
		return sourceArchiveError(400, "SHA-256 hashes required")
	}
	if len(in.Raw) == 0 || len(in.Raw) > 1<<20 || len(in.Text) == 0 || len(in.Text) > 1<<20 || !utf8.ValidString(in.Text) || strings.ContainsRune(in.Text, 0) {
		return sourceArchiveError(400, "raw and extracted text must each contain 1 byte to 1 MiB; text must be UTF-8 without NUL")
	}
	raw, err := decodeArchivedRaw(in)
	if err != nil {
		return err
	}
	if Hash(raw) != in.RawHash || Hash([]byte(in.Text)) != in.TextHash {
		return sourceArchiveError(400, "source archive hash mismatch")
	}
	u, err := url.Parse(in.URL)
	if err != nil || len(in.URL) > 2048 || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" || (u.Port() != "" && u.Port() != "443") {
		return sourceArchiveError(400, "source requires an HTTPS URL without credentials, fragment or custom port")
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return sourceArchiveError(400, "invalid source URL query")
	}
	for key := range query {
		for _, forbidden := range []string{"token", "api_key", "apikey", "password", "secret", "signature"} {
			if strings.Contains(strings.ToLower(key), forbidden) {
				return sourceArchiveError(400, "credential-like URL parameters are forbidden")
			}
		}
	}
	when, err := time.Parse(time.RFC3339Nano, in.CapturedAt)
	if err != nil || when.IsZero() || when.After(time.Now().Add(5*time.Minute)) {
		return sourceArchiveError(400, "valid historical capture time required")
	}
	media, _, err := mime.ParseMediaType(in.MediaType)
	if err != nil || media == "" || len(in.MediaType) > 200 || strings.ContainsAny(in.MediaType, "\r\n") || len(in.ExtractionMethod) > 1000 || strings.TrimSpace(in.ExtractionMethod) == "" {
		return sourceArchiveError(400, "media type and bounded extraction method required")
	}
	if len(in.Spans) == 0 || len(in.Spans) > 64 {
		return sourceArchiveError(400, "1 to 64 matching source/document spans required")
	}
	return nil
}

func (s *Store) AttachSourceArchive(ctx context.Context, fileID string, in SourceArchiveInput) (SourceArchiveRecord, error) {
	var out SourceArchiveRecord
	if !archiveID.MatchString(fileID) {
		return out, sourceArchiveError(400, "invalid file ID")
	}
	if err := validateSourceArchive(in); err != nil {
		return out, err
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	var fileHash, documentID, document string
	var ref Ref
	err = tx.QueryRow(ctx, `SELECT f.artifact_hash,f.document_id,d.content,f.kb_id,f.kb_revision FROM knowledge_files f JOIN documents d ON d.id=f.document_id WHERE f.id=$1 FOR SHARE OF f,d`, fileID).Scan(&fileHash, &documentID, &document, &ref.ID, &ref.Revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, sourceArchiveError(404, "knowledge file not found")
	}
	if err != nil {
		return out, err
	}
	if fileHash != in.FileHash {
		return out, sourceArchiveError(409, "target file hash differs")
	}
	if !sourceSpansMatch(in.Text, document, in.Spans) {
		return out, sourceArchiveError(400, "selected Unicode source span differs from retained document")
	}
	when, _ := time.Parse(time.RFC3339Nano, in.CapturedAt)
	raw, _ := decodeArchivedRaw(in)
	out.Archive = SourceArchive{Format: 1, FileID: fileID, DocumentID: documentID, Knowledge: ref, FileHash: fileHash, URL: in.URL, CapturedAt: when.UTC().Format(time.RFC3339Nano), MediaType: in.MediaType, ExtractionMethod: in.ExtractionMethod, RawHash: in.RawHash, RawEncoding: in.RawEncoding, StoredRawHash: Hash(in.Raw), StoredRawBytes: len(in.Raw), RawBytes: len(raw), TextHash: in.TextHash, TextBytes: len([]byte(in.Text)), Spans: in.Spans, Provenance: archiveProvenance}
	manifest := JSON(out.Archive)
	out.Hash = Hash(manifest)
	if len(manifest) > 16384 {
		return out, sourceArchiveError(400, "source manifest exceeds 16 KiB")
	}
	root := "knowledge-source:" + fileID
	// One transaction retains all three immutable values. No cross-service SQL,
	// file/document mutation, indexing or provider call occurs during attachment.
	items := []struct {
		body        []byte
		media, root string
	}{
		{in.Raw, "application/octet-stream", root + ":payloads"},
		{[]byte(in.Text), "text/plain; charset=utf-8", root + ":payloads"},
		{manifest, "application/vnd.agentos.source-archive+json", root + ":manifests"},
	}
	// Stable lock order also covers different files sharing swapped raw/text bytes.
	sort.Slice(items, func(i, j int) bool { return Hash(items[i].body) < Hash(items[j].body) })
	for _, item := range items {
		hash, e := putArtifact(ctx, tx, item.body, item.media, item.root)
		if e != nil {
			return out, e
		}
		var saved []byte
		if e = tx.QueryRow(ctx, "SELECT body FROM artifacts WHERE hash=$1", hash).Scan(&saved); e != nil {
			return out, e
		}
		if !bytes.Equal(saved, item.body) {
			return out, sourceArchiveError(409, "stored archive artifact is corrupt")
		}
	}
	return out, tx.Commit(ctx)
}

func (s *Store) SourceArchives(ctx context.Context, fileID, after string) (SourceArchivePage, error) {
	out := SourceArchivePage{Items: []SourceArchiveRecord{}}
	if !archiveID.MatchString(fileID) || (after != "" && !archiveHash.MatchString(after)) {
		return out, sourceArchiveError(400, "invalid file ID or archive cursor")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var fileHash, documentID string
	var ref Ref
	if err := s.DB.QueryRow(ctx, "SELECT artifact_hash,document_id,kb_id,kb_revision FROM knowledge_files WHERE id=$1", fileID).Scan(&fileHash, &documentID, &ref.ID, &ref.Revision); errors.Is(err, pgx.ErrNoRows) {
		return out, sourceArchiveError(404, "knowledge file not found")
	} else if err != nil {
		return out, err
	}
	rows, err := s.DB.Query(ctx, `SELECT a.hash,substring(a.body FROM 1 FOR 16385) FROM artifact_refs r JOIN artifacts a ON a.hash=r.hash WHERE r.root=$1 AND ($2='' OR a.hash<$2) ORDER BY a.hash DESC LIMIT 21`, "knowledge-source:"+fileID+":manifests", after)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var item SourceArchiveRecord
		var body []byte
		if err = rows.Scan(&item.Hash, &body); err != nil {
			return out, err
		}
		if !decodeSourceManifest(body, &item, fileID, fileHash, documentID, ref) {
			return out, sourceArchiveError(409, "source manifest integrity mismatch")
		}
		out.Items = append(out.Items, item)
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	if len(out.Items) > 20 {
		out.Items = out.Items[:20]
		out.NextCursor = out.Items[19].Hash
	}
	return out, nil
}

func decodeSourceManifest(body []byte, item *SourceArchiveRecord, fileID, fileHash, documentID string, ref Ref) bool {
	if len(body) > 16384 || Hash(body) != item.Hash || json.Unmarshal(body, &item.Archive) != nil {
		return false
	}
	a := item.Archive
	return a.Format == 1 && a.FileID == fileID && a.FileHash == fileHash && a.DocumentID == documentID && a.Knowledge == ref &&
		a.Provenance == archiveProvenance && archiveHash.MatchString(a.RawHash) && archiveHash.MatchString(a.StoredRawHash) && archiveHash.MatchString(a.TextHash) &&
		a.StoredRawBytes > 0 && a.StoredRawBytes <= 1<<20 && a.RawBytes > 0 && a.RawBytes <= 1<<20 && a.TextBytes > 0 && a.TextBytes <= 1<<20 &&
		(a.RawEncoding == "identity" || a.RawEncoding == "gzip") && len(a.Spans) > 0 && len(a.Spans) <= 64
}

// SourceArchiveMember follows file -> manifest root -> declared payload -> payload
// root, never a caller-supplied arbitrary artifact hash. Every read checks the
// whole retained association and bounded bytes before returning any member.
func (s *Store) SourceArchiveMember(ctx context.Context, fileID, manifestHash, member string) ([]byte, string, error) {
	if !archiveID.MatchString(fileID) || !archiveHash.MatchString(manifestHash) || (member != "raw" && member != "text" && member != "manifest") {
		return nil, "", sourceArchiveError(400, "invalid source archive member")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var fileHash, documentID, document string
	var ref Ref
	var manifest, raw, text []byte
	root := "knowledge-source:" + fileID
	err := s.DB.QueryRow(ctx, `SELECT f.artifact_hash,f.document_id,f.kb_id,f.kb_revision,d.content,substring(a.body FROM 1 FOR 16385)
FROM knowledge_files f JOIN documents d ON d.id=f.document_id
JOIN artifact_refs r ON r.root=$2 AND r.hash=$3 JOIN artifacts a ON a.hash=r.hash WHERE f.id=$1`, fileID, root+":manifests", manifestHash).Scan(&fileHash, &documentID, &ref.ID, &ref.Revision, &document, &manifest)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, "", sourceArchiveError(404, "source archive not attached to file")
	}
	if err != nil {
		return nil, "", err
	}
	item := SourceArchiveRecord{Hash: manifestHash}
	if !decodeSourceManifest(manifest, &item, fileID, fileHash, documentID, ref) {
		return nil, "", sourceArchiveError(409, "source manifest integrity mismatch")
	}
	a := item.Archive
	err = s.DB.QueryRow(ctx, `SELECT substring(a.body FROM 1 FOR 1048577),substring(b.body FROM 1 FOR 1048577)
FROM artifact_refs r JOIN artifacts a ON a.hash=r.hash
JOIN artifact_refs s ON s.root=r.root AND s.hash=$3 JOIN artifacts b ON b.hash=s.hash
WHERE r.root=$1 AND r.hash=$2`, root+":payloads", a.StoredRawHash, a.TextHash).Scan(&raw, &text)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, "", sourceArchiveError(409, "source payload retention reference missing")
	}
	if err != nil {
		return nil, "", err
	}
	if len(raw) != a.StoredRawBytes || Hash(raw) != a.StoredRawHash || len(text) != a.TextBytes || Hash(text) != a.TextHash {
		return nil, "", sourceArchiveError(409, "source payload integrity mismatch")
	}
	in := SourceArchiveInput{FileHash: fileHash, URL: a.URL, CapturedAt: a.CapturedAt, MediaType: a.MediaType, ExtractionMethod: a.ExtractionMethod, RawHash: a.RawHash, RawEncoding: a.RawEncoding, TextHash: a.TextHash, Raw: raw, Text: string(text), Spans: a.Spans}
	if err = validateSourceArchive(in); err != nil {
		return nil, "", sourceArchiveError(409, "source archive metadata or bytes invalid")
	}
	decoded, _ := decodeArchivedRaw(in)
	if len(decoded) != a.RawBytes || !sourceSpansMatch(in.Text, document, a.Spans) {
		return nil, "", sourceArchiveError(409, "source archive document association invalid")
	}
	switch member {
	case "raw":
		return raw, a.StoredRawHash, nil
	case "text":
		return text, a.TextHash, nil
	default:
		return manifest, manifestHash, nil
	}
}

func sourceSpansMatch(text, document string, spans []SourceSpan) bool {
	textRunes, documentRunes := []rune(text), []rune(document)
	for _, span := range spans {
		if span.Start < 0 || span.End <= span.Start || span.End > len(textRunes) || span.DocumentStart < 0 || span.DocumentEnd <= span.DocumentStart || span.DocumentEnd > len(documentRunes) || span.End-span.Start != span.DocumentEnd-span.DocumentStart {
			return false
		}
		if string(textRunes[span.Start:span.End]) != string(documentRunes[span.DocumentStart:span.DocumentEnd]) || strings.TrimSpace(string(textRunes[span.Start:span.End])) == "" {
			return false
		}
	}
	return len(spans) > 0
}

func (s *Server) sourceArchiveRoutes(m *http.ServeMux) {
	m.HandleFunc("GET /v1/files/{id}/source-archives/{manifest}/{member}", servicehttp.Wrap(func(w http.ResponseWriter, r *http.Request) error {
		if r.URL.RawQuery != "" {
			return sourceArchiveError(400, "source member query not supported")
		}
		body, hash, err := s.Store.SourceArchiveMember(r.Context(), r.PathValue("id"), r.PathValue("manifest"), r.PathValue("member"))
		if err != nil {
			return err
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", "attachment")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-SHA256", hash)
		_, err = w.Write(body)
		return err
	}))
	m.HandleFunc("POST /v1/files/{id}/source-archives", servicehttp.Wrap(func(w http.ResponseWriter, r *http.Request) error {
		var in SourceArchiveInput
		if err := servicehttp.Decode(w, r, &in); err != nil {
			return err
		}
		out, err := s.Store.AttachSourceArchive(r.Context(), r.PathValue("id"), in)
		if err != nil {
			return err
		}
		w.Header().Set("Cache-Control", "no-store")
		servicehttp.JSONResponse(w, 200, out)
		return nil
	}))
	m.HandleFunc("GET /v1/files/{id}/source-archives", servicehttp.Wrap(func(w http.ResponseWriter, r *http.Request) error {
		q, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			return sourceArchiveError(400, "invalid archive query")
		}
		for key, values := range q {
			if key != "after" || len(values) != 1 || values[0] == "" {
				return sourceArchiveError(400, "invalid archive query")
			}
		}
		out, err := s.Store.SourceArchives(r.Context(), r.PathValue("id"), q.Get("after"))
		if err != nil {
			return err
		}
		w.Header().Set("Cache-Control", "no-store")
		servicehttp.JSONResponse(w, 200, out)
		return nil
	}))
}
