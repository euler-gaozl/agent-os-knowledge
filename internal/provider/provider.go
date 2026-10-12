package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	. "github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"io"
	"math"
	"net/http"
	"strings"
)

type SecretReader interface {
	Secret(context.Context, string) (string, error)
}
type Client struct{ SecretReader }

func JSONRequest(ctx context.Context, method, endpoint, key, header string, body any) ([]byte, error) {
	if e := ValidateEndpoint(endpoint); e != nil {
		return nil, e
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(JSON(body))
	}
	req, e := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if e != nil {
		return nil, e
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		if header == "X-API-Key" {
			req.Header.Set(header, key)
		} else {
			req.Header.Set("Authorization", "Bearer "+key)
		}
	}
	resp, e := servicehttp.Client.Do(req)
	if e != nil {
		return nil, e
	}
	defer resp.Body.Close()
	b, e := io.ReadAll(io.LimitReader(resp.Body, 4<<20+1))
	if e != nil {
		return nil, e
	}
	if len(b) > 4<<20 {
		return nil, errors.New("provider response exceeds 4 MiB")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("provider HTTP %d", resp.StatusCode)
	}
	if !json.Valid(b) {
		return nil, errors.New("provider returned invalid JSON")
	}
	return b, nil
}
func (s Client) Embed(ctx context.Context, kb KnowledgeBase, text []string, query bool) ([][]float64, error) {
	if query && kb.QueryPrefix != "" {
		for i := range text {
			text[i] = kb.QueryPrefix + text[i]
		}
	}
	key, e := s.Secret(ctx, kb.Embedding.KeyEnv)
	if e != nil {
		return nil, e
	}
	b, e := JSONRequest(ctx, "POST", strings.TrimRight(kb.Embedding.BaseURL, "/")+"/embeddings", key, "", map[string]any{"model": kb.Embedding.Model, "input": text, "encoding_format": "float"})
	if e != nil {
		return nil, e
	}
	var response struct {
		Data []struct {
			Index     int        `json:"index"`
			Embedding []*float64 `json:"embedding"`
		} `json:"data"`
	}
	if e = json.Unmarshal(b, &response); e != nil {
		return nil, e
	}
	if len(response.Data) != len(text) {
		return nil, errors.New("embedding count mismatch")
	}
	out := make([][]float64, len(text))
	for _, item := range response.Data {
		if item.Index < 0 || item.Index >= len(out) || out[item.Index] != nil || len(item.Embedding) != kb.Dimensions {
			return nil, errors.New("embedding index/dimension mismatch")
		}
		vector := make([]float64, len(item.Embedding))
		norm := 0.0
		for i, value := range item.Embedding {
			if value == nil || math.IsNaN(*value) || math.IsInf(*value, 0) || math.Abs(*value) > math.MaxFloat32 {
				return nil, errors.New("embedding contains a null or non-finite float32 value")
			}
			vector[i] = *value
			indexed := float64(float32(*value))
			norm += indexed * indexed
		}
		if norm == 0 {
			return nil, errors.New("embedding is a zero vector")
		}
		out[item.Index] = vector
	}
	return out, nil
}
func Collection(ref Ref) string { return "kb_" + Hash(JSON(ref))[:24] }
func (s Client) Milvus(ctx context.Context, kb KnowledgeBase, path string, body any) ([]byte, error) {
	key, e := s.Secret(ctx, kb.KeyEnv)
	if e != nil {
		return nil, e
	}
	b, e := JSONRequest(ctx, "POST", strings.TrimRight(kb.Endpoint, "/")+"/v2/vectordb/"+path, key, "", body)
	if e != nil {
		return nil, e
	}
	var result struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if e = json.Unmarshal(b, &result); e != nil {
		return nil, e
	}
	if result.Code != 0 {
		return nil, fmt.Errorf("Milvus error %d: %s", result.Code, servicehttp.Truncate(result.Message, 300))
	}
	return b, nil
}
