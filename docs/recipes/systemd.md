---
title: Run the server as a systemd unit
sidebar_label: systemd unit
description: Long-lived gMountie server on a Linux host, managed by systemd. Restart-on-crash, journald logging, runs as an unprivileged user.
---

# Run the server as a systemd unit

**End state:** `gmountie serve` runs as an unprivileged user, starts at boot, restarts if it crashes, and logs to `journalctl`. About 5 minutes.

## What you need

- A Linux host with systemd (any modern distro).
- The `gmountie` binary on the host (`/usr/local/bin/gmountie` in this recipe).
- A directory you want to share (`/srv/shared` in this recipe).

## 1. Create a dedicated user

Run the server as an unprivileged account so a process compromise doesn't get root:

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin gmountie
sudo install -d -o gmountie -g gmountie /etc/gmountie
sudo install -d -o gmountie -g gmountie /srv/shared
```

## 2. Drop in the config

```yaml title="/etc/gmountie/server.yaml"
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
    path: /srv/shared
    mapping:
      mode: squash
      uid: 1000
      gid: 1000
```

```bash
sudo chown root:gmountie /etc/gmountie/server.yaml
sudo chmod 640 /etc/gmountie/server.yaml
```

Permissions matter — `640` keeps the password out of any process's read scope except the `gmountie` user.

## 3. The unit file

```ini title="/etc/systemd/system/gmountie.service"
[Unit]
Description=gMountie server
Documentation=https://docs.gmountie.dev
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=gmountie
Group=gmountie
ExecStart=/usr/local/bin/gmountie serve -c /etc/gmountie/server.yaml
Restart=on-failure
RestartSec=2s

# Sandboxing — least privilege.
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/srv/shared
PrivateTmp=true
ProtectHome=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictSUIDSGID=true
LockPersonality=true
MemoryDenyWriteExecute=true
RestrictRealtime=true

[Install]
WantedBy=multi-user.target
```

`ReadWritePaths=/srv/shared` is the **only** path the unit can write to — adjust if you serve multiple volumes from different paths.

## 4. Enable and start

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now gmountie
sudo systemctl status gmountie
```

If the status is `active (running)`, you're done. Mount from a client per the [Quickstart](../quickstart.mdx).

## Logs

`journalctl` reads everything `gmountie serve` writes to stderr:

```bash
sudo journalctl -u gmountie -f          # tail
sudo journalctl -u gmountie --since '15 min ago'
sudo journalctl -u gmountie -p err      # only error-level entries
```

gMountie emits JSON logs in non-TTY environments by default — easy to feed into Loki / Vector / Elasticsearch if you have one.

## Health and metrics

`server.metrics: true` exposes Prometheus metrics and a health endpoint on `:9090`:

```bash
curl http://127.0.0.1:9090/healthz       # 200 = ready
curl http://127.0.0.1:9090/metrics       # Prometheus text format
```

Scrape `:9090` from your monitoring stack; alert on `up == 0` and on the per-RPC error-rate metrics. You can change the port via `server.metrics_addr` in the config.

## Upgrading

```bash
sudo install -m 755 ./gmountie /usr/local/bin/gmountie   # replace the binary
sudo systemctl restart gmountie
```

In-flight RPCs run to completion thanks to graceful shutdown (`SIGTERM` is wired); the restart should be invisible to mounted clients past the keepalive window.

## See also

- [Server CLI](../server/cli.md) · [Server configuration](../server/config.md)
- [Recipes — Docker & Compose](./docker.md) for the containerised path
- [Recipes — TLS via Caddy](./caddy-reverse-proxy.md) to put a reverse proxy in front
