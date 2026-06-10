package service

import "go.gmountie.dev/gmountie/pkg/server/config"

type staticResolver struct{ m config.MappingConfig }

func NewStaticResolver(m config.MappingConfig) IdentityResolver { return &staticResolver{m} }

// ConstantIdentity: each principal's identity is frozen in the volume's
// mapping config for the process lifetime — eligible for the executor fast
// path.
func (r *staticResolver) ConstantIdentity() bool { return true }

func (r *staticResolver) Resolve(principal string) (Identity, error) {
	u, ok := r.m.Users[principal]
	if !ok {
		return Identity{}, ErrPrincipalNotFound
	}
	gids := []uint32{u.Gid}
	groupNames := map[uint32]string{}
	for _, g := range u.Groups {
		if gid, ok := r.m.Groups[g]; ok {
			if gid != u.Gid {
				gids = append(gids, gid)
			}
			groupNames[gid] = g
		}
	}
	return Identity{
		Principal: principal,
		Uid:       u.Uid, Gid: u.Gid, Gids: gids, Caps: u.Caps,
		UserName:   principal,
		GroupNames: groupNames,
	}, nil
}
