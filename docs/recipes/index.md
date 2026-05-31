---
title: Recipes
sidebar_label: Recipes
slug: /recipes
description: Concrete, copy-paste-runnable walkthroughs for common gMountie setups — systemd, Docker, reverse proxy.
---

# Recipes

Concrete walkthroughs. Each one starts with what you'll end up with, lists what you need, and gives you the commands and configs to copy-paste. None of them invent features — every flag and every config field is documented in the [Server config](../server/config.md) / [Client config](../client/config.md) pages.

## Operations

- **[Run the server as a systemd unit](./systemd.md)** — managed by the host, restarts on crash, logs go to the journal. The recommended path for a long-lived server on a Linux host.
- **[Run the server in Docker or Compose](./docker.md)** — using the published `ghcr.io/gmountie/gmountie-server` image, single command or full `docker-compose.yaml`.

## Networking

- **[TLS via Caddy reverse proxy](./caddy-reverse-proxy.md)** — gMountie speaks plain gRPC today; until native TLS lands ([roadmap](../roadmap.md)) you can put Caddy in front to terminate TLS with an automatic Let's Encrypt cert.

## Need one that's not here?

If you're using gMountie in a setup that isn't covered here, [open an issue](https://github.com/gMountie/gMountie/issues) — recipes get added based on what people actually run.
