package observer

import (
	"context"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/io"
	"go.gmountie.dev/gmountie/pkg/client/metrics"
	"go.gmountie.dev/gmountie/pkg/proto"
)

// metricsLayer is an OBSERVER layer: it times a representative set of ops and
// emits via the injected Recorder, forwarding everything else unchanged. It
// embeds PassthroughBackend (observer base) so ops it does not time still work;
// it adds NO retry and changes NO behavior (contract: observers are transparent).
type metricsLayer struct {
	io.PassthroughBackend
	rec metrics.Recorder
}

// NewMetricsLayer wraps inner with op-level boundary metrics.
func NewMetricsLayer(inner io.FileSystemBackend, rec metrics.Recorder) io.FileSystemBackend {
	return &metricsLayer{PassthroughBackend: io.PassthroughBackend{Inner: inner}, rec: rec}
}

func (l *metricsLayer) Stat(ctx context.Context, path string) (*io.Attr, proto.FsError) {
	start := time.Now()
	attr, st := l.Inner.Stat(ctx, path)
	l.rec.ObserveOp("Stat", time.Since(start).Seconds(), st.String())
	return attr, st
}

func (l *metricsLayer) Read(ctx context.Context, fh io.FileHandle, off int64, dest []byte) (int, proto.FsError) {
	start := time.Now()
	n, st := l.Inner.Read(ctx, fh, off, dest)
	l.rec.ObserveOp("Read", time.Since(start).Seconds(), st.String())
	return n, st
}

func (l *metricsLayer) Write(ctx context.Context, fh io.FileHandle, off int64, data []byte) (uint32, proto.FsError) {
	start := time.Now()
	n, st := l.Inner.Write(ctx, fh, off, data)
	l.rec.ObserveOp("Write", time.Since(start).Seconds(), st.String())
	return n, st
}

func (l *metricsLayer) Lookup(ctx context.Context, parent, name string) (*io.Attr, proto.FsError) {
	start := time.Now()
	attr, st := l.Inner.Lookup(ctx, parent, name)
	l.rec.ObserveOp("Lookup", time.Since(start).Seconds(), st.String())
	return attr, st
}

func (l *metricsLayer) ListDir(ctx context.Context, path string) ([]io.DirEntryPlus, proto.FsError) {
	start := time.Now()
	es, st := l.Inner.ListDir(ctx, path)
	l.rec.ObserveOp("ListDir", time.Since(start).Seconds(), st.String())
	return es, st
}
