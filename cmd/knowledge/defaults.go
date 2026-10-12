package main

import "os"

// Called after configuration loading and before any client or background task.
// These are this service's legacy local addresses, never SDK-wide role policy.
func applyLocalDefaults() error {
	for key, value := range map[string]string{"RUNTIME_URL": "http://127.0.0.1:8094", "REGISTRY_URL": "http://127.0.0.1:8096"} {
		if os.Getenv(key) == "" {
			if err := os.Setenv(key, value); err != nil {
				return err
			}
		}
	}
	return nil
}
