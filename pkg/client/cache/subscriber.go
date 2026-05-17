package cache

import (
	"context"
	"io"
	"path"
	"time"

	"gmountie/pkg/proto"
	"gmountie/pkg/utils/log"

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
	client   proto.RpcFsClient
	volume   string
	cache    subscribeBackendOps
	validity *validityTracker
	open     func(ctx context.Context) (subscribeStream, error)
}

func newSubscribeConsumer(client proto.RpcFsClient, volume string, cache subscribeBackendOps, validity *validityTracker) *subscribeConsumer {
	c := &subscribeConsumer{client: client, volume: volume, cache: cache, validity: validity}
	c.open = func(ctx context.Context) (subscribeStream, error) {
		return client.Subscribe(ctx, &proto.SubscribeRequest{Volume: volume}, grpc.WaitForReady(true))
	}
	return c
}

func (c *subscribeConsumer) run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		err := c.runOnce(ctx)
		if err != nil && ctx.Err() == nil {
			log.Log.Warn("subscribe stream error",
				zap.String("volume", c.volume),
				zap.Error(err),
			)
		}
		c.validity.markGlobalUnverified()
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
			if err == io.EOF {
				return nil
			}
			return err
		}
		c.handle(ev)
		if !sawHeartbeat && ev.Kind == proto.SubscribeEvent_HEARTBEAT {
			c.validity.markGlobalVerified()
			sawHeartbeat = true
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

func (c *subscribeConsumer) handle(ev *proto.SubscribeEvent) {
	switch ev.Kind {
	case proto.SubscribeEvent_MUTATED:
		c.cache.invalidateAttr(ev.Path)
		c.cache.invalidateData(ev.Path)
		c.cache.invalidateDir(subscribePathParent(ev.Path))
	case proto.SubscribeEvent_DELETED:
		c.cache.invalidateAttr(ev.Path)
		c.cache.invalidateData(ev.Path)
		c.cache.invalidateDir(subscribePathParent(ev.Path))
		c.cache.putNegative(ev.Path)
	case proto.SubscribeEvent_RENAMED:
		for _, p := range []string{ev.Path, ev.NewPath} {
			c.cache.invalidateAttr(p)
			c.cache.invalidateData(p)
			c.cache.invalidateDir(subscribePathParent(p))
		}
		c.cache.putNegative(ev.Path)
	case proto.SubscribeEvent_HEARTBEAT:
		// no-op; flip handled by runOnce after first heartbeat is seen
	}
}
