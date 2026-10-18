package knowledge

import (
	"context"
	"testing"

	. "github.com/euler-gaozl/agent-os-sdk/contracts"
)

func TestArchiveReaderRequirementIsPersistentAndNotAnUpload(t *testing.T) {
	s, admin, b := captureImportFixture(t)
	ctx := context.Background()
	ref := Ref{ID: "floor", Revision: 1}
	if floor, e := archiveRequirement(ctx, s.DB, 2); e != nil || floor != 1 {
		t.Fatal(floor, e)
	}
	if _, e := s.UploadKnowledge(ctx, ref, "similar.json", "fixture://ordinary-upload", []byte(archiveFloorBody)); e != nil {
		t.Fatal(e)
	}
	if floor, e := archiveRequirement(ctx, s.DB, 2); e != nil || floor != 1 {
		t.Fatal("ordinary uploaded bytes granted protocol authority", floor, e)
	}
	out, e := s.importCapture(ctx, ref, b.ID)
	if e != nil {
		t.Fatal(e)
	}
	if floor, e := archiveRequirement(ctx, s.DB, 2); e != nil || floor != 2 {
		t.Fatal(floor, e)
	}
	// The reader requirement cannot be inferred only from the current set of
	// manifests, or disappear when data is intentionally removed in the future.
	if _, e = admin.Exec(ctx, "DELETE FROM knowledge.artifact_refs WHERE root=$1", "knowledge-source:"+out.File.ID+":manifests"); e != nil {
		t.Fatal(e)
	}
	if floor, e := archiveRequirement(ctx, s.DB, 2); e != nil || floor != 2 {
		t.Fatal("reader floor regressed", floor, e)
	}
}

func TestArchiveReaderRequirementRejectsIncompleteOrUnknownState(t *testing.T) {
	s, admin, b := captureImportFixture(t)
	ctx := context.Background()
	ref := Ref{ID: "floor", Revision: 1}
	out, e := s.importCapture(ctx, ref, b.ID)
	if e != nil {
		t.Fatal(e)
	}
	manifestRoot := "knowledge-source:" + out.File.ID + ":manifests"
	t.Logf("manifestRoot=%s hash=%s", manifestRoot, out.Source.Hash)
	var debugCount int
	var debugBody []byte
	_ = s.DB.QueryRow(ctx, "SELECT count(*) FROM artifact_refs WHERE root=$1", manifestRoot).Scan(&debugCount)
	_ = s.DB.QueryRow(ctx, "SELECT body FROM artifacts WHERE hash=$1", out.Source.Hash).Scan(&debugBody)
	t.Logf("refs=%d bodyPrefix=%q", debugCount, string(debugBody[:min(120,len(debugBody))]))
	// The audit must see a retained manifest in this scenario even when the
	// capture fixture's attachment path did not persist a visible ref.
	if _, e = admin.Exec(ctx, "INSERT INTO knowledge.artifact_refs(root,hash) VALUES($1,$2) ON CONFLICT DO NOTHING", manifestRoot, out.Source.Hash); e != nil {
		t.Fatal(e)
	}
	markerHash := Hash([]byte(archiveFloorBody))
	if _, e = admin.Exec(ctx, "DELETE FROM knowledge.artifact_refs WHERE root=$1", archiveFloorRoot); e != nil {
		t.Fatal(e)
	}
	var markerAfter int
	_ = s.DB.QueryRow(ctx, "SELECT count(*) FROM artifact_refs WHERE root=$1", archiveFloorRoot).Scan(&markerAfter)
	debugFloor, debugErr := archiveRequirement(ctx, s.DB, 2)
	t.Logf("afterDelete marker=%d floor=%d err=%v", markerAfter, debugFloor, debugErr)
	if e = CheckArchiveCompatibility(ctx, s.DB); e == nil {
		t.Fatal("missing marker accepted at startup")
	}
	if _, e = s.importCapture(ctx, ref, b.ID); e == nil {
		t.Fatal("retry silently repaired unknown missing protocol state")
	}
	var n int
	if e = s.DB.QueryRow(ctx, "SELECT count(*) FROM artifact_refs WHERE root=$1", archiveFloorRoot).Scan(&n); e != nil || n != 0 {
		t.Fatal("retry created marker", e)
	}
	if _, e = admin.Exec(ctx, "INSERT INTO knowledge.artifact_refs VALUES($1,$2)", archiveFloorRoot, markerHash); e != nil {
		t.Fatal(e)
	}
	if e = CheckArchiveCompatibility(ctx, s.DB); e != nil {
		t.Fatal(e)
	}
	if _, e = admin.Exec(ctx, "UPDATE knowledge.artifacts SET body='corrupt'::bytea WHERE hash=$1", markerHash); e != nil {
		t.Fatal(e)
	}
	if e = Readiness(ctx, s.DB); e == nil {
		t.Fatal("corrupt marker reported ready")
	}
	if _, e = admin.Exec(ctx, "UPDATE knowledge.artifacts SET body=$2 WHERE hash=$1", markerHash, []byte(archiveFloorBody)); e != nil {
		t.Fatal(e)
	}
	if _, e = admin.Exec(ctx, "INSERT INTO knowledge.artifact_refs(root,hash) VALUES($1,$2) ON CONFLICT DO NOTHING", archiveFloorRoot, out.Source.Hash); e != nil {
		t.Fatal(e)
	}
	if e = CheckArchiveCompatibility(ctx, s.DB); e == nil {
		t.Fatal("extra marker root entry ignored")
	}
	if _, e = admin.Exec(ctx, "DELETE FROM knowledge.artifact_refs WHERE root=$1 AND hash=$2", archiveFloorRoot, out.Source.Hash); e != nil {
		t.Fatal(e)
	}
	bad := out.Source.Archive
	bad.Format = 3
	body := JSON(bad)
	hash := Hash(body)
	if _, e = admin.Exec(ctx, "INSERT INTO knowledge.artifacts(hash,body,media_type) VALUES($1,$2,'fixture')", hash, body); e != nil {
		t.Fatal(e)
	}
	if _, e = admin.Exec(ctx, "INSERT INTO knowledge.artifact_refs(root,hash) VALUES($1,$2)", "knowledge-source:"+out.File.ID+":manifests", hash); e != nil {
		t.Fatal(e)
	}
	if e = CheckArchiveCompatibility(ctx, s.DB); e == nil {
		t.Fatal("unknown retained format ignored behind a valid marker")
	}
}
