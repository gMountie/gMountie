package ops

import (
	"context"
	"encoding/json"
	"net/http"

	"go.gmountie.dev/gmountie/pkg/server/config"
	"go.gmountie.dev/gmountie/pkg/server/principal"
	"go.gmountie.dev/gmountie/pkg/server/service"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"go.uber.org/zap"
)

// ReloadHandler handles POST /ops/acl/reload. It re-reads the config file,
// validates it, atomically swaps the ACL + cert-serial blocklist, then reaps
// sessions that the new state revokes. A bad config returns 400 and changes
// nothing (fail-safe). Only auth state is hot-reloaded.
func ReloadHandler(cfg *config.Config, vs service.VolumeService, sm service.SessionManager, rs *service.RevocationStore) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if cfg.ConfigPath == "" {
			http.Error(w, "no config file to reload (config came from defaults/env)", http.StatusBadRequest)
			return
		}
		newCfg, err := config.ReloadFromFile(cfg.ConfigPath)
		if err != nil {
			log.Log.Warn("acl reload rejected: bad config", zap.Error(err))
			http.Error(w, "reload rejected: "+err.Error(), http.StatusBadRequest)
			return
		}

		// Swap auth state.
		vs.ReloadAuth(newCfg)
		rs.Set(revokedSerials(newCfg))

		// Warn about the revoke-by-deletion footgun: under default_allow=true a
		// principal removed from users[] falls through to "allowed", so deleting
		// a user does NOT revoke access. To actually revoke: set volumes:[],
		// default_allow:false, or add the cert serial to revoked_serials.
		if bac, ok := newCfg.Auth.(*config.BasicAuthConfig); ok && bac.DefaultAllowOrTrue() {
			log.Log.Warn("acl reload: default_allow=true — removing a user does NOT revoke access; " +
				"use volumes:[], default_allow:false, or revoked_serials to revoke")
		}

		// Reap sessions the new state revokes.
		reaped := sm.ReapIf(reapPredicate(newCfg, vs, rs))
		log.Log.Info("acl reloaded", zap.Int("reaped", reaped))

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int{"reaped": reaped})
	})
}

// revokedSerials pulls the blocklist out of the auth config (basic + mtls both
// use *BasicAuthConfig). Nil when auth isn't of that type.
func revokedSerials(cfg *config.Config) []string {
	if bac, ok := cfg.Auth.(*config.BasicAuthConfig); ok {
		return bac.RevokedSerials
	}
	return nil
}

// reapPredicate reaps a session iff its serial is now blocked OR its principal
// can no longer access any configured volume. An additive reload (no serial
// blocked, no access removed) matches nothing.
func reapPredicate(cfg *config.Config, vs service.VolumeService, rs *service.RevocationStore) func(principalName, serial string) bool {
	return func(principalName, serial string) bool {
		if rs.IsBlocked(serial) {
			return true
		}
		ctx := principal.WithPrincipal(context.Background(), principalName)
		for _, v := range cfg.Volumes {
			if vs.PrincipalCanAccess(ctx, v.Name) == nil {
				return false // still has at least one volume → keep
			}
		}
		return true // no accessible volume → reap
	}
}
