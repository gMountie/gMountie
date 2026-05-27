package service

import "gmountie/pkg/server/config"

type staticResolver struct{ m config.MappingConfig }

func NewStaticResolver(m config.MappingConfig) IdentityResolver { return &staticResolver{m} }

func (r *staticResolver) Resolve(principal string) (Identity, error) {
	u, ok := r.m.Users[principal]
	if !ok {
		return Identity{}, ErrPrincipalNotFound
	}
	gids := []uint32{u.Gid}
	for _, g := range u.Groups {
		if gid, ok := r.m.Groups[g]; ok && gid != u.Gid {
			gids = append(gids, gid)
		}
	}
	return Identity{Principal: principal, Uid: u.Uid, Gid: u.Gid, Gids: gids, Caps: u.Caps}, nil
}
