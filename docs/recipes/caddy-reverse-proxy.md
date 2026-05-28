---
title: TLS via a Caddy reverse proxy
sidebar_label: TLS via Caddy
description: Terminate TLS in front of `gmountie serve` with Caddy and an automatic Let's Encrypt cert. Bridge to native TLS once it lands.
---

# TLS via a Caddy reverse proxy

**End state:** clients reach the server at `gmountie.example.com:443` over TLS with a real Let's Encrypt cert. Caddy terminates TLS and proxies the request as plain h2c (HTTP/2 cleartext) to `gmountie serve` on `127.0.0.1:9449`.

Native TLS in the server is on the [roadmap](../roadmap.md) but isn't shipped yet. Until then, this recipe is the safe way to expose the server on the public internet.

## Why Caddy specifically

- Automatic Let's Encrypt cert issuance and renewal — no extra script, no `certbot`.
- First-class `h2c` reverse-proxy support — required because gMountie speaks HTTP/2 cleartext on the wire.
- One-file config; sane defaults.

(nginx works too; the h2c support is fiddlier — `grpc_pass` works if you accept HTTP/2-over-TLS only.)

## What you need

- A Linux host running `gmountie serve` bound to **`127.0.0.1:9449`** (loopback only — the public ingress is Caddy's job, not gMountie's).
- A DNS A/AAAA record pointing `gmountie.example.com` → the host's public IP.
- Ports `80` and `443` open on the host (Caddy uses `80` for the ACME HTTP-01 challenge).
- Caddy 2.x installed ([install instructions](https://caddyserver.com/docs/install)).

## 1. Bind the server to localhost

Edit your `server.yaml` so the server **only** listens on the loopback interface — Caddy is the public face:

```yaml
server:
  address: 127.0.0.1
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
```

Restart `gmountie serve` (`sudo systemctl restart gmountie` if you followed the [systemd recipe](./systemd.md)).

## 2. The Caddyfile

```caddyfile title="/etc/caddy/Caddyfile"
gmountie.example.com {
    # gRPC requires HTTP/2 end-to-end; Caddy negotiates HTTP/2 with the
    # client over TLS and re-issues over h2c (HTTP/2 cleartext) to the
    # backend.
    reverse_proxy h2c://127.0.0.1:9449 {
        transport http {
            versions h2c
        }
    }

    # Compress text responses Caddy returns directly (does not touch the
    # already-Snappy-compressed gRPC traffic).
    encode zstd gzip

    log {
        output file /var/log/caddy/gmountie.access.log {
            roll_size 50mb
            roll_keep 5
        }
        format console
    }
}
```

## 3. Reload Caddy

```bash
sudo caddy validate --config /etc/caddy/Caddyfile     # syntax check
sudo systemctl reload caddy                            # apply
```

Caddy fetches and installs a Let's Encrypt cert automatically on first request. Watch `journalctl -u caddy` for the cert-issuance line.

## 4. Connect from the client

```bash
gmountie mount /mnt/shared \
  --server gmountie.example.com:443 \
  --auth-type basic --username admin --password '<password>' \
  --volume shared
```

That's it. The client speaks gRPC over TLS to Caddy, Caddy reverse-proxies as h2c to the local `gmountie serve`.

:::caution
Caddy fronts the gRPC traffic; the **basic-auth credentials still travel as base64 inside the gRPC frame**, just now over a TLS tunnel between client and Caddy. That's strictly better than naked plaintext — but in an attacker model where the host is compromised, the credentials remain in cleartext on the loopback hop between Caddy and `gmountie serve`. Native mTLS (planned) will close that gap.
:::

## Quick verification

```bash
# DNS + TLS works?
curl -I https://gmountie.example.com/                  # Caddy returns 200/404 — the cert is what matters
openssl s_client -connect gmountie.example.com:443 < /dev/null | grep 'subject='

# gRPC reaches the backend?
grpcurl -d '{}' gmountie.example.com:443 list          # if you have grpcurl
```

A successful TLS handshake plus the client's mount coming up confirms the chain.

## Notes for nginx users

If you must use nginx instead of Caddy, the equivalent stanza is:

```nginx
server {
    listen 443 ssl http2;
    server_name gmountie.example.com;

    ssl_certificate     /etc/letsencrypt/live/gmountie.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/gmountie.example.com/privkey.pem;

    location / {
        grpc_pass grpc://127.0.0.1:9449;
        grpc_read_timeout  300s;
        grpc_send_timeout  300s;
    }
}
```

You'll need a separate `certbot` flow to issue the cert; Caddy folds that into the config.

## See also

- [Server configuration](../server/config.md) — `server.address` to bind to localhost.
- [Recipes — systemd unit](./systemd.md) — to keep `gmountie serve` alive behind Caddy.
- [Roadmap](../roadmap.md) — when native TLS lands, you'll be able to drop Caddy for client-facing TLS (you may still want it for `:80 → :443` redirect, gzip, access logs).
