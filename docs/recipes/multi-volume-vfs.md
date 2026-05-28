---
title: Multi-volume VFS mount
sidebar_label: Multi-volume (VFS)
description: Mount every remote volume under one parent path on the client instead of running `gmountie mount` per volume.
---

# Multi-volume VFS mount

**End state:** one `gmountie mount` process exposes every remote volume under one parent path. Listing the parent shows each volume as a subdirectory; descending into a subdirectory enters that volume.

```
~/mnt/gmountie/
├── shared/        ← volume `shared`
├── documents/     ← volume `documents`
└── media/         ← volume `media`
```

This is the **VFS** mode (vs the default **single** mode, which mounts exactly one volume at one path). VFS is what the desktop app uses internally; the CLI exposes the same machinery.

## When to pick VFS

- You have multiple volumes on the same server and don't want to run one `gmountie mount` process per volume.
- You want a single mountpoint to put in your shell `cd` muscle memory (`~/mnt/gmountie/<volume>`) rather than memorising one per volume.
- You're scripting (backups, sync) and want a stable parent path the script can walk.

When NOT to pick VFS: when you only mount one volume (the overhead is zero either way, but `single` is the simpler config).

## Config

VFS is selected by `mount.type: vfs` in `client.yaml`:

```yaml title="client.yaml"
server:
  address: example.com
  port: 9449
  tls: false
auth:
  type: basic
  username: admin
  password: change-me-before-deploy

mount:
  type: vfs
  path: /home/you/mnt/gmountie
  # Pick one of:
  mount_all: true            # mount every volume the server exposes
  # volumes:                 # …or list specifically
  #   - shared
  #   - documents
  #   - media
```

- **`mount_all: true`** — the client asks the server for the volume list and mounts each one. New volumes added on the server appear after the next mount.
- **`volumes: [...]`** — explicit list; only those are mounted. Unknown names fail at mount time.

Pick one of the two — they're mutually exclusive.

## Bring it up

```bash
mkdir -p ~/mnt/gmountie       # the parent must exist and be empty
gmountie mount -c client.yaml
ls ~/mnt/gmountie             # one entry per mounted volume
```

Press `Ctrl-C` to tear the whole thing down; the parent and every subdirectory unmount together.

## How it actually works

The client creates an in-memory `MemFS` root at `mount.path` and attaches each remote volume as a subdirectory of that root. Listing the parent is served from memory (it's just the volume names); everything inside a subdirectory goes through the same gRPC pipeline as a single-volume mount.

That means the per-volume settings (cache, readahead, write coalescing) apply to each volume independently — they share `client.yaml`'s settings. If you need different tuning per volume, run separate `gmountie mount` processes with separate config files.

## Caveats

- **One auth principal for the whole VFS.** The `auth.username` / `auth.password` apply to every volume mounted under this VFS. If your server has different access lists per volume, the user has to be allowed on all of them, or use a separate mount.
- **Permission denials don't hide volumes.** If the principal is allowed on the server but not on a specific volume, the subdirectory will appear in the parent listing but operations inside it will fail. That's intentional (matches how POSIX permissions surface elsewhere); use `volumes: [...]` instead of `mount_all` if you want to suppress them.

## See also

- [Client configuration — Mount Configuration](../client/config.md#mount-configuration) — the full field reference.
- [Identity & ownership](../concepts/identity.mdx) — how the resolved identity decides what you can do inside each volume.
