package cache

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/metrics"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// subscribeBackendOps is the subset of cachedBackend the consumer
// needs. Pulled into an interface so the consumer is unit-testable
// without spinning up a full cachedBackend.
type subscribeBackendOps interface {
	invalidateAttr(path string)
	invalidateData(path string)
	invalidateDir(path string)
	invalidateXAttr(path string)
	putNegative(path string)
}

// subscribeStream is the subset of proto.RpcFs_SubscribeClient the
// consumer needs. Lets tests inject a fake.
type subscribeStream interface {
	Recv() (*proto.SubscribeEvent, error)
}

// subscribeConsumer runs one goroutine per cachedBackend that owns
// the Subscribe stream, drains events into cache invalidations, and
// flips the validityTracker. On stream error it sleeps with
// exponential backoff (1s → 30s) and reconnects.
type subscribeConsumer struct {
	client          invalidationSource
	volume          string
	cache           subscribeBackendOps
	validity        *validityTracker
	open            func(ctx context.Context) (subscribeStream, error)
	unverifiedSince time.Time // zero when currently verified
}

func newSubscribeConsumer(client invalidationSource, volume string, cache subscribeBackendOps, validity *validityTracker) *subscribeConsumer {
	c := &subscribeConsumer{client: client, volume: volume, cache: cache, validity: validity}
	// Capture the mounter's local identity once: Subscribe is a long-lived
	// background loop with no per-op FUSE ctx (matches the WhoAmI pattern in
	// pkg/client/grpc/client.go). The server uses this Caller to bind a
	// per-principal identity and filter events by per-path access.
	caller := &proto.Caller{Owner: &proto.Owner{
		Uid: uint32(os.Getuid()),
		Gid: uint32(os.Getgid()),
	}}
	c.open = func(ctx context.Context) (subscribeStream, error) {
		return client.Subscribe(ctx, &proto.SubscribeRequest{Volume: volume, Caller: caller}, grpc.WaitForReady(true))
	}
	return c
}

func (c *subscribeConsumer) run(ctx context.Context) {
	// Consumer starts unverified; signal metrics.
	c.unverifiedSince = time.Now()
	metrics.SubscribeStreamStateChanged(false)

	backoff := time.Second
	for ctx.Err() == nil {
		err := c.runOnce(ctx)
		if err != nil && ctx.Err() == nil {
			log.Log.Warn("subscribe stream error",
				zap.String("volume", c.volume),
				zap.Error(err),
			)
		}
		// Stream is down: mark unverified and record the transition time.
		c.validity.markGlobalUnverified()
		metrics.SubscribeStreamStateChanged(false)
		if c.unverifiedSince.IsZero() {
			c.unverifiedSince = time.Now()
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

func (c *subscribeConsumer) runOnce(ctx context.Context) error {
	stream, err := c.open(ctx)
	if err != nil {
		return err
	}
	sawHeartbeat := false
	for {
		ev, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		c.handle(ev)
		if !sawHeartbeat && ev.Kind == proto.SubscribeEvent_HEARTBEAT {
			c.validity.markGlobalVerified()
			sawHeartbeat = true
			// Record time spent unverified before this first heartbeat.
			if !c.unverifiedSince.IsZero() {
				metrics.CacheUnverifiedElapsed(time.Since(c.unverifiedSince).Seconds())
				c.unverifiedSince = time.Time{}
			}
			metrics.SubscribeStreamStateChanged(true)
		}
	}
}

// subscribePathParent is the same pathParent logic the cache layer uses
// elsewhere. Kept local so the consumer can be tested without pulling
// in cachedBackend.
func subscribePathParent(p string) string {
	d := path.Dir(p)
	if d == "." {
		return ""
	}
	return d
}

// invalidatePathAndParent drops the cached attrs, data, and parent directory
// listing for p. Any operation that modifies the directory tree — create,
// unlink, rename — bumps the parent's mtime on the server. Over-invalidating
// the parent attr on plain writes is cheap (one extra Stat RTT at most) and
// safe, so we apply the same pattern uniformly across all event kinds.
func (c *subscribeConsumer) invalidatePathAndParent(p string) {
	parent := subscribePathParent(p)
	c.cache.invalidateAttr(p)
	c.cache.invalidateData(p)
	c.cache.invalidateXAttr(p)
	c.cache.invalidateDir(parent)
	c.cache.invalidateAttr(parent)
}

func (c *subscribeConsumer) handle(ev *proto.SubscribeEvent) {
	switch ev.Kind {
	case proto.SubscribeEvent_MUTATED:
		metrics.SubscribeEventReceived("mutated")
		c.invalidatePathAndParent(ev.Path)
	case proto.SubscribeEvent_DELETED:
		metrics.SubscribeEventReceived("deleted")
		c.invalidatePathAndParent(ev.Path)
		c.cache.putNegative(ev.Path)
	case proto.SubscribeEvent_RENAMED:
		metrics.SubscribeEventReceived("renamed")
		for _, p := range []string{ev.Path, ev.NewPath} {
			c.invalidatePathAndParent(p)
		}
		c.cache.putNegative(ev.Path)
	case proto.SubscribeEvent_HEARTBEAT:
		metrics.SubscribeEventReceived("heartbeat")
		// verified-state flip is handled by runOnce after the first heartbeat
	}
}
