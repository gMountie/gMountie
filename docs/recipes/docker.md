---
title: Run the server in Docker or Compose
sidebar_label: Docker & Compose
description: Run gMountie's published OCI image with a single `docker run` or a full `docker-compose.yaml` stack.
---

# Run the server in Docker or Compose

**End state:** `gmountie serve` running in a container, listening on `9449` for gRPC and `9090` for health + metrics, with its data on a persistent volume.

The published image is **`ghcr.io/gmountie/gmountie-server`** — server-only, runs as **uid 1000** (non-root), Alpine-based.

## Single `docker run`

For a one-off, mount your config and the directory you want to share:

```bash
docker run -d --name gmountie \
  -p 9449:9449 -p 9090:9090 \
  -v "$(pwd)/server.yaml:/config.yaml:ro" \
  -v /srv/shared:/srv/shared \
  --user 1000:1000 \
  ghcr.io/gmountie/gmountie-server:latest \
  --config /config.yaml
```

The image runs as **uid 1000** by default, so `/srv/shared` must be owned by that uid (`sudo chown 1000:1000 /srv/shared`) — or set a different `--user`. Either way, the uid the container runs as must match what the volume's `mapping.uid` resolves to.

## Docker Compose

A full stack with a named data volume and a one-shot init container that fixes permissions. This is what ships in [`deployments/compose/`](https://github.com/gMountie/gMountie/tree/master/deployments/compose):

```yaml title="docker-compose.yaml"
name: gmountie

services:
  server:
    image: ghcr.io/gmountie/gmountie-server:${GMOUNTIE_IMAGE_TAG:-latest}
    user: "1000:1000"
    command: ["--config", "/config.yaml"]
    depends_on:
      init-permissions:
        condition: service_completed_successfully
    volumes:
      - data-volume:/data
    configs:
      - source: server-config
        target: /config.yaml
    ports:
      - "${GRPC_PORT:-9449}:9449"
      - "${METRICS_PORT:-9090}:9090"

  # One-shot: give the data volume to the non-root container user.
  init-permissions:
    image: alpine:3.20
    command: chown 1000:1000 /data
    volumes:
      - data-volume:/data
    restart: "no"

volumes:
  data-volume:

configs:
  server-config:
    file: ./config.yaml
```

With a sibling `config.yaml`:

```yaml title="config.yaml"
server:
  address: 0.0.0.0
  port: 9449
  metrics: true
auth:
  type: basic
  users:
    - username: admin
      password: change-me-before-deploy
volumes:
  - name: shared
    path: /data
    mapping:
      mode: squash
      uid: 1000
      gid: 1000
```

And an optional `.env` for port + tag overrides:

```bash title=".env"
GMOUNTIE_IMAGE_TAG=latest
GRPC_PORT=9449
METRICS_PORT=9090
```

Bring it up:

```bash
docker compose up -d
docker compose logs -f server
```

Tear it down (and wipe the data volume if you want):

```bash
docker compose down                # keeps the data volume
docker compose down --volumes      # wipes it
```

## Health and metrics

The image has a built-in `HEALTHCHECK` that hits `:9090/healthz`:

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
# gmountie    Up 5 minutes (healthy)
```

Scrape `:9090/metrics` from your Prometheus / VictoriaMetrics / equivalent.

## Image facts to know

- **Server-only.** The image runs `gmountie serve`. There's no `gmountie mount` inside the container — FUSE mounting is a client-side concern that needs the host's `/dev/fuse`, and you almost never want to mount *into* a server container.
- **Pinned base.** The Alpine base is pinned by digest; bumps are manual until dependabot is wired.
- **Non-root.** uid 1000. If you bind-mount a host directory, `chown 1000:1000` it (or set `--user`).
- **TLS.** The server serves native TLS by default (auto-generated self-signed cert; pin it client-side via TOFU). To present a CA-trusted cert instead, terminate TLS in front with [Caddy](./caddy-reverse-proxy.md) or a similar reverse proxy.

## Kubernetes

A Helm chart ships at [`deployments/charts/gmountie-server/`](https://github.com/gMountie/gMountie/tree/master/deployments/charts/gmountie-server) with sensible defaults (1 replica, `runAsUser: 1000`, `fsGroup: 1000`, ReadWriteOnce PVC, NetworkPolicy-friendly). Install with:

```bash
helm install gmountie ./deployments/charts/gmountie-server \
  --set image.tag=<tag-or-sha> \
  --values your-values.yaml
```

See the chart's `values.yaml` for the full set of overrides.

## See also

- [Recipes — systemd unit](./systemd.md) — for the host-native path.
- [Recipes — TLS via Caddy](./caddy-reverse-proxy.md) — to put TLS in front.
- [Server configuration](../server/config.md) — every field the mounted `config.yaml` accepts.
