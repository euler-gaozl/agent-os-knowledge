package knowledge

import (
	"encoding/json"

	"github.com/euler-gaozl/agent-os-sdk/contracts"
)

// The Web query keeps full snippets. Model retrieval keeps every document handle
// visible without spilling a multi-document JSON response into paged artifacts.
// The source document remains authoritative and is read through /v1/document.
func retrievalForModel(result contracts.Retrieval) any {
	if result.Engine != "local" {
		return result
	}
	hits := make([]map[string]any, 0, len(result.Hits))
	seen := map[string]bool{}
	for _, hit := range result.Hits {
		id, _ := hit["document_id"].(string)
		if seen[id] {
			continue
		}
		seen[id] = true
		hits = append(hits, map[string]any{
			"document_id": id, "chunk_id": hit["chunk_id"],
			"title": previewText(hit["title"], 64), "content": previewText(hit["content"], 96),
		})
	}
	view := map[string]any{
		"engine": result.Engine, "hits": hits, "preview_only": true,
		"query_mode": "all_terms", "coverage": "matching_candidates_only",
		"note": "关键词必须同时匹配；空结果先缩短查询。最多8个片段，非全量目录。按document_id使用document_read回读正文、来源和日期；预览不是完整证据。",
	}
	// JSON escaping (including control characters) can expand otherwise short
	// snippets. Retain handles, reduce only previews, and stay below 6000 bytes.
	for _, limit := range []int{48, 0} {
		encoded, _ := json.Marshal(view)
		if len(encoded) <= 5500 {
			break
		}
		for _, hit := range hits {
			hit["content"] = previewText(hit["content"], limit)
		}
	}
	return view
}

func previewText(value any, limit int) string {
	text, _ := value.(string)
	runes := []rune(text)
	if len(runes) <= limit {
		return text
	}
	return string(runes[:limit])
}
