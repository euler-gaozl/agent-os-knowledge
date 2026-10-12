package knowledge

import "strings"

func Authorize(caller, method, path string) bool {
	if path == "/readyz" || path == "/metrics" {
		return caller == "knowledge" || caller == "control"
	}
	if caller == "control" {
		return strings.HasPrefix(path, "/v1/") && path != "/v1/retrieve" && path != "/v1/document"
	}
	if caller == "futures" {
		return method == "GET" && path == "/v1/status"
	}
	return caller == "runtime" && method == "POST" && (path == "/v1/retrieve" || path == "/v1/document")
}
