package identity_test

import (
	"context"
	"testing"

	clientio "go.gmountie.dev/gmountie/pkg/client/io"
	"go.gmountie.dev/gmountie/pkg/client/io/identity"
	"go.gmountie.dev/gmountie/pkg/client/io/memfs"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
)

// serverUID/serverGID is the identity the server resolved for the caller;
// localUID/localGID is the mounting user. The rewriter maps between them. A
// supplementary group (suppGID) is carried so the shared-group pass-through
// path is exercised, and a foreign id (foreignID) proves the nobody fallback.
const (
	serverUID = 1001
	serverGID = 1001
	suppGID   = 2000
	localUID  = 500
	localGID  = 500
	foreignID = 9999
	nobodyID  = 65534
)

// LayerSuite exercises the identity decorator over a real memfs inner with a
// NON-identity rewriter, so a bug that drops the rewrite (or applies it in the
// wrong direction) is visible. It migrates the parity coverage previously held
// by pkg/client/io/node_idrewrite_test.go into the layer's own home.
type LayerSuite struct {
	suite.Suite
	ctx   context.Context
	inner clientio.FileSystemBackend // raw memfs (server-id namespace)
	layer clientio.FileSystemBackend // identity.NewLayer(inner, rw)
	rw    *clientio.IDRewriter
}

func TestLayerSuite(t *testing.T) { suite.Run(t, new(LayerSuite)) }

func (s *LayerSuite) SetupTest() {
	s.ctx = context.Background()
	s.inner = memfs.New()
	s.rw = clientio.NewIDRewriter(
		&clientio.Identity{Uid: serverUID, Gid: serverGID, Gids: []uint32{serverGID, suppGID}},
		localUID, localGID,
	)
	s.layer = identity.NewLayer(s.inner, s.rw)
}

// setOwnership stamps server uid/gid directly onto a path via the INNER backend
// (bypassing the layer's Outbound) so a subsequent inbound read through the
// layer must translate server→local.
func (s *LayerSuite) setOwnership(path string, uid, gid uint32) {
	_, st := s.inner.SetAttr(s.ctx, path, clientio.SetAttrIn{
		Valid: fuse.FATTR_UID | fuse.FATTR_GID, Uid: uid, Gid: gid,
	})
	s.Require().Equal(proto.FsError_FS_OK, st)
}

func (s *LayerSuite) createFile(name string, uid, gid uint32) {
	s.createFileIn("", name, uid, gid)
}

// createFileIn creates parent/name (parent "" == root) on the inner backend and
// stamps server ownership onto it.
func (s *LayerSuite) createFileIn(parent, name string, uid, gid uint32) {
	fh, _, st := s.inner.Create(s.ctx, parent, name, 0, 0o644)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().Equal(proto.FsError_FS_OK, s.inner.Release(s.ctx, fh))
	full := name
	if parent != "" {
		full = parent + "/" + name
	}
	s.setOwnership(full, uid, gid)
}

// --- Inbound: each of the 7 inbound methods rewrites server→local ---

func (s *LayerSuite) TestStatInboundRewrite() {
	s.createFile("f", serverUID, serverGID)
	a, st := s.layer.Stat(s.ctx, "f")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Equal(uint32(localUID), a.Uid, "own server uid -> local")
	s.Equal(uint32(localGID), a.Gid, "own server gid -> local")
}

func (s *LayerSuite) TestStatForeignMapsToNobody() {
	s.createFile("foreign", foreignID, foreignID)
	a, st := s.layer.Stat(s.ctx, "foreign")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Equal(uint32(nobodyID), a.Uid, "foreign uid -> nobody")
	s.Equal(uint32(nobodyID), a.Gid, "foreign gid -> nobody")
}

func (s *LayerSuite) TestGetAttrIfChangedInboundRewrite() {
	s.createFile("rev", serverUID, serverGID)
	// knownVersion 0 never matches -> returns fresh attrs (notModified=false).
	a, notModified, st := s.layer.GetAttrIfChanged(s.ctx, "rev", 0)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().False(notModified)
	s.Require().NotNil(a)
	s.Equal(uint32(localUID), a.Uid)
	s.Equal(uint32(localGID), a.Gid)
}

func (s *LayerSuite) TestGetAttrIfChangedNotModifiedNilAttr() {
	s.createFile("rev2", serverUID, serverGID)
	cur, _, st := s.layer.GetAttrIfChanged(s.ctx, "rev2", 0)
	s.Require().Equal(proto.FsError_FS_OK, st)
	// Same version -> not modified -> nil attr; rewrite must not panic on nil.
	got, notModified, st := s.layer.GetAttrIfChanged(s.ctx, "rev2", cur.Version)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.True(notModified)
	s.Nil(got)
}

func (s *LayerSuite) TestLookupInboundRewrite() {
	s.createFile("look", serverUID, serverGID)
	a, st := s.layer.Lookup(s.ctx, "", "look")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Equal(uint32(localUID), a.Uid)
	s.Equal(uint32(localGID), a.Gid)
}

func (s *LayerSuite) TestListDirEntryAttrInboundRewrite() {
	_, st := s.inner.Mkdir(s.ctx, "d", 0o755)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.createFileIn("d", "child", serverUID, serverGID)

	entries, st := s.layer.ListDir(s.ctx, "d")
	s.Require().Equal(proto.FsError_FS_OK, st)
	var found bool
	for _, e := range entries {
		if e.Name == "child" {
			found = true
			s.Require().NotNil(e.Attr, "memfs plus listing carries per-entry attr")
			s.Equal(uint32(localUID), e.Attr.Uid, "ListDir entry attr rewritten")
			s.Equal(uint32(localGID), e.Attr.Gid)
		}
	}
	s.True(found, "child entry present")
}

func (s *LayerSuite) TestCreateInboundRewrite() {
	// memfs stamps a created node's owner from the SetAttr path, not Create; to
	// exercise Create's inbound rewrite, pre-set ownership won't apply (new
	// node). Instead create through the layer, then set server ownership on the
	// inner and re-stat through the layer is covered elsewhere. Here we assert
	// the Create reply attr is run through Inbound: a fresh memfs node defaults
	// to uid/gid 0, which is foreign -> nobody.
	fh, a, st := s.layer.Create(s.ctx, "", "newf", 0, 0o644)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().NotNil(a)
	s.Require().Equal(proto.FsError_FS_OK, s.layer.Release(s.ctx, fh))
	s.Equal(uint32(nobodyID), a.Uid, "uid 0 is foreign -> nobody (Inbound applied)")
	s.Equal(uint32(nobodyID), a.Gid)
}

func (s *LayerSuite) TestMkdirInboundRewrite() {
	a, st := s.layer.Mkdir(s.ctx, "newd", 0o755)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().NotNil(a)
	// Fresh dir owner 0 -> foreign -> nobody, proving Inbound ran.
	s.Equal(uint32(nobodyID), a.Uid)
	s.Equal(uint32(nobodyID), a.Gid)
}

func (s *LayerSuite) TestSymlinkInboundRewrite() {
	a, st := s.layer.Symlink(s.ctx, "target/path", "newlink")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().NotNil(a)
	s.Equal(uint32(nobodyID), a.Uid)
	s.Equal(uint32(nobodyID), a.Gid)
}

// --- Outbound: SetAttr rewrites local→server before delegating ---

func (s *LayerSuite) TestSetAttrOutboundRewriteBothBits() {
	s.createFile("chown", serverUID, serverGID)
	// Caller sets owner to LOCAL ids; layer must Outbound them to SERVER ids
	// before the inner sees the request, and Inbound the reply back to local.
	a, st := s.layer.SetAttr(s.ctx, "chown", clientio.SetAttrIn{
		Valid: fuse.FATTR_UID | fuse.FATTR_GID, Uid: localUID, Gid: localGID,
	})
	s.Require().Equal(proto.FsError_FS_OK, st)
	// Inner now stores server ids (Outbound applied).
	innerAttr, ist := s.inner.Stat(s.ctx, "chown")
	s.Require().Equal(proto.FsError_FS_OK, ist)
	s.Equal(uint32(serverUID), innerAttr.Uid, "inner stored server uid (Outbound)")
	s.Equal(uint32(serverGID), innerAttr.Gid, "inner stored server gid (Outbound)")
	// Reply surfaced to the caller is back in local ids (Inbound on reply).
	s.Equal(uint32(localUID), a.Uid, "reply rewritten back to local")
	s.Equal(uint32(localGID), a.Gid)
}

// TestSetAttrOutboundUIDOnlyStillRewrites mirrors node.go's gidOK semantics:
// only the UID valid bit is set, but Outbound runs on BOTH halves. The GID
// half has no valid bit so the inner ignores its value — proving the partial
// case is correct.
func (s *LayerSuite) TestSetAttrOutboundUIDOnlyStillRewrites() {
	s.createFile("chuid", foreignID, serverGID) // start uid foreign, gid server
	a, st := s.layer.SetAttr(s.ctx, "chuid", clientio.SetAttrIn{
		Valid: fuse.FATTR_UID, Uid: localUID, // only uid bit set
	})
	s.Require().Equal(proto.FsError_FS_OK, st)
	innerAttr, ist := s.inner.Stat(s.ctx, "chuid")
	s.Require().Equal(proto.FsError_FS_OK, ist)
	s.Equal(uint32(serverUID), innerAttr.Uid, "uid rewritten local->server and applied")
	s.Equal(uint32(serverGID), innerAttr.Gid, "gid untouched (no valid bit)")
	// Reply uid back to local; gid was server -> local.
	s.Equal(uint32(localUID), a.Uid)
}

// TestSetAttrNonOwnerBitsNoRewrite confirms a size/mode-only SetAttr does not
// touch ownership: the gate is (uid|gid) bits, so Outbound never runs.
func (s *LayerSuite) TestSetAttrNonOwnerBitsNoRewrite() {
	s.createFile("sz", serverUID, serverGID)
	_, st := s.layer.SetAttr(s.ctx, "sz", clientio.SetAttrIn{Valid: fuse.FATTR_SIZE, Size: 7})
	s.Require().Equal(proto.FsError_FS_OK, st)
	innerAttr, ist := s.inner.Stat(s.ctx, "sz")
	s.Require().Equal(proto.FsError_FS_OK, ist)
	s.Equal(uint32(serverUID), innerAttr.Uid, "ownership untouched by a size-only SetAttr")
	s.Equal(uint32(serverGID), innerAttr.Gid)
	s.Equal(uint64(7), innerAttr.Size)
}

// --- Forwarded methods pass through untouched ---

func (s *LayerSuite) TestForwardedXAttrPassthrough() {
	s.createFile("xf", serverUID, serverGID)
	const key = "user.k"
	val := []byte("v")
	s.Require().Equal(proto.FsError_FS_OK, s.layer.SetXAttr(s.ctx, "xf", key, val, 0))
	got, st := s.layer.GetXAttr(s.ctx, "xf", key)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Equal(val, got)
}

func (s *LayerSuite) TestForwardedReadlinkPassthrough() {
	_, st := s.inner.Symlink(s.ctx, "the/target", "ln")
	s.Require().Equal(proto.FsError_FS_OK, st)
	got, st := s.layer.Readlink(s.ctx, "ln")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Equal("the/target", got)
}

// --- nil rewriter is a transparent pass-through (returns inner) ---

func (s *LayerSuite) TestNilRewriterIsTransparent() {
	inner := memfs.New()
	got := identity.NewLayer(inner, nil)
	s.Same(inner, got, "nil rewriter -> NewLayer returns inner unchanged (no decorator)")
}

func (s *LayerSuite) TestNilRewriterNoInboundRewrite() {
	inner := memfs.New()
	be := identity.NewLayer(inner, nil) // == inner
	fh, _, st := be.Create(s.ctx, "", "raw", 0, 0o644)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().Equal(proto.FsError_FS_OK, be.Release(s.ctx, fh))
	_, st = inner.SetAttr(s.ctx, "raw", clientio.SetAttrIn{
		Valid: fuse.FATTR_UID | fuse.FATTR_GID, Uid: foreignID, Gid: foreignID,
	})
	s.Require().Equal(proto.FsError_FS_OK, st)
	a, st := be.Stat(s.ctx, "raw")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Equal(uint32(foreignID), a.Uid, "no rewrite: server ids pass through verbatim")
	s.Equal(uint32(foreignID), a.Gid)
}
