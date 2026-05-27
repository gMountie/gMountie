package service

// squashResolver maps every principal to one fixed identity (NFS all_squash).
type squashResolver struct{ uid, gid uint32 }

func NewSquashResolver(uid, gid uint32) IdentityResolver { return &squashResolver{uid, gid} }

func (r *squashResolver) Resolve(principal string) (Identity, error) {
	return Identity{Principal: principal, Uid: r.uid, Gid: r.gid, Gids: []uint32{r.gid}}, nil
}
