package ops

import (
	"encoding/json"
	"net/http"

	"gmountie/pkg"
)

// LivenessHandler always returns 200 — the binary is alive enough to
// answer HTTP. Per the layering-service-features skill, this is the
// documented "pure passthrough" exception (no business logic).
func LivenessHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
}

// ReadinessHandler defers to a ReadinessChecker; 200 on Ready, 503
// otherwise. Decoder/encoder is thin; the checker is the service.
func ReadinessHandler(rc ReadinessChecker) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := rc.Ready(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready"))
	})
}

// VersionHandler returns the build info as JSON. Pure passthrough.
func VersionHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(pkg.GetBuildInfo())
	})
}
