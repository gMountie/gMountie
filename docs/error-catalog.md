---
id: error-catalog
title: Error catalog
sidebar_label: Error catalog
description: Every well-known gMountie error string with what it means and what to check. Greppable, no invented messages — every entry comes from `pkg/`.
---

# Error catalog

If you saw an error and landed here from a search, jump to the section that matches the **phase** it surfaced in. Every string below is real (grepped from `pkg/`) — there are no invented messages. For symptoms instead of strings, see **[Troubleshooting](./troubleshooting.mdx)**.

## Startup and config

These errors show up before the server is serving or the client is connected — usually on the first run with a bad `config.yaml`.

| Error                                                             | Component             | What it means                                                                 | What to check                                                            |
| ----------------------------------------------------------------- | --------------------- | ----------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `auth: missing 'auth' section in config`                          | `pkg/server/config`   | The server config has no top-level `auth:` block. Anonymous mode was removed. | Add an `auth:` block with at least one `users` entry. See [Server config](./server/config.md#authentication-options). |
| `invalid auth type: "X" (only 'basic' is supported)`              | server + client       | `auth.type` isn't `basic`. No other schemes exist today.                      | Set `auth.type: basic`. mTLS is on the [roadmap](./roadmap.md), not shipped. |
| `invalid mount type: <X>`                                         | `pkg/client/config`   | `mount.type` isn't `single` or `vfs`.                                         | Set `mount.type: single` (one volume → one path) or `vfs` (all volumes under one parent). See [Multi-volume VFS](./recipes/multi-volume-vfs.md). |
| `config is empty or auth config is empty`                         | `pkg/client/grpc`     | The client tried to build a gRPC connection with no auth populated.           | The CLI flags `-u/-p/-t basic` must be set, or a `client.yaml` with `auth:` must be passed with `-c`. |
| `server config is missing`                                        | `pkg/client/grpc`     | The client config has no `server:` block.                                     | Add `server: { address: …, port: … }`. See [Client config](./client/config.md). |

## Connection and authentication

These show up after startup, when the client first talks to the server — or after a network blip.

| Error                                       | Component         | What it means                                                                                                                       | What to check                                                                                                                                                            |
| ------------------------------------------- | ----------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `Unavailable: connection refused`           | gRPC              | TCP couldn't connect.                                                                                                               | Server not running, or the `--server host:port` doesn't match where it's listening, or a firewall is in the way.                                                          |
| `Unavailable: dial timeout`                 | gRPC              | TCP connected too slowly (or not at all).                                                                                           | High RTT / firewall slow-drop. Tighten or loosen [`rpc.keepalive`](./client/config.md#keepalive) and check the network path.                                              |
| `server unavailable`                        | `pkg/client/grpc` | Final unwrapped error when retries against `Unavailable` exhaust.                                                                   | Confirm the server is up; gMountie's retry budget kicked in and gave up. Restart `gmountie mount` after the server is back.                                              |
| `Unauthenticated`                           | gRPC              | The server rejected the basic-auth credentials at the connection-setup interceptor.                                                 | `--username` / `--password` (or `auth.username` / `auth.password` in `client.yaml`) must match a server-side `auth.users` entry.                                          |
| `session handshake failed; client unusable` | `pkg/client/grpc` | The client connected but the post-handshake session setup (which establishes the session ID and the WhoAmI mapping) failed.         | Check the server log for the matching session-controller error. Usually a transient auth or `principal not found` issue; restart the client after fixing the server side. |

## Mount lifecycle

These are surfaced by `gmountie mount` while bringing up or tearing down the FUSE mount.

| Error                                          | Component            | What it means                                                                                  | What to check                                                                                                                                       |
| ---------------------------------------------- | -------------------- | ---------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `attaching the volume failed: <reason>`        | `pkg/client/mount`   | FUSE attach (the kernel-side mount step) failed.                                               | Inspect `<reason>` — usually `fusermount3` is missing, the mountpoint is not empty, or the user lacks `/dev/fuse` access.                              |
| `unmounting the volume failed: <reason>`       | `pkg/client/mount`   | FUSE detach failed on shutdown.                                                                | Process holding files open in the mount? Use `lsof +D /mnt/...` to find it, kill, then `umount -l /mnt/...` as a last resort.                          |
| `volume X is already mounted`                  | `pkg/client/mount`   | A second `gmountie mount` was attempted for a volume already mounted in this client process.   | Tear the existing mount down first (`Ctrl-C` the running `gmountie mount`, or `umount` the path).                                                    |
| `volume X is not mounted`                      | `pkg/client/mount`   | An unmount or volume-detach was requested for a volume the client doesn't know about.          | Confirm the volume name matches `mount.volume` (or the `--volume` flag).                                                                            |
| `mounter not started`                          | `pkg/client/mount`   | An operation was issued before the FUSE mounter finished initialising (mostly a races-in-test failure mode). | Confirm the mount succeeded (look for "attached the volume" in the log) before issuing follow-up operations. Should not surface in normal runtime. |
| `mountpoint must be an empty directory`        | FUSE                 | The directory you're mounting at has content.                                                  | `ls /mnt/...` — it must be empty. Either pick a different path or move the contents.                                                                |
| `fusermount3: command not found`               | host                 | The FUSE userspace helper isn't installed.                                                     | `apt install fuse3` (Debian/Ubuntu) or distro equivalent.                                                                                            |

## During a working mount (runtime)

These surface as syscall return codes inside the mounted directory, or as gRPC status codes in the `gmountie mount` log.

| Symptom                            | Underlying gRPC / errno      | What it means                                                                                                              | What to check                                                                                                                                                                                                              |
| ---------------------------------- | ---------------------------- | -------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Input/output error` from `read`   | `EIO`                        | The mount is up but the server can't satisfy the request (server gone past keepalive, fd revoked).                          | Check whether the server is still alive (`nc -zv host 9449`). If brief blip, reads should stall (not `EIO`) — confirm `rpc.keepalive` matches your network. See [Sessions & reconnect](./concepts/sessions-and-reconnect.mdx). |
| `Permission denied`                | `EACCES` / `PermissionDenied`| The **resolved server-side identity** for this volume doesn't have permission on the file. Local `ls -l` rewriting masks this. | Mount with `--raw-ids` to see the server's actual uid/gid; reconcile via the volume's `mapping` block or the underlying file mode. See [Identity & ownership](./concepts/identity.mdx).                                       |
| `Operation not permitted`          | `EPERM`                      | Same as above for ops that need ownership (chmod, chown).                                                                  | Same as above. Most clients can't chmod files they don't own server-side, regardless of how the file shows up locally.                                                                                                       |
| `No such file or directory`        | `ENOENT` / `NotFound`        | The server's volume directory doesn't contain that path.                                                                   | Confirm the path exists on the server's filesystem inside the volume's `path`. The negative-cache TTL (`cache.negative_ttl`, default 2 s) may briefly remember a 404 — wait it out or restart the mount.                       |
| `principal not found`              | `PermissionDenied`           | `static` or `system` mapping mode couldn't resolve the authenticated username.                                              | Add the user to the volume's `mapping.users` (static) or to the server's NSS (system). See [Identity — static / system](./concepts/identity.mdx).                                                                          |
| `server sent more bytes than requested` | `pkg/client/io`         | A streaming Read returned more bytes than the client asked for — a protocol bug. Mount will recover but the read is aborted. | If reproducible, [open an issue](https://github.com/gMountie/gMountie/issues) with a verbose-log excerpt — this shouldn't happen.                                                                                            |

## Diagnostic checklist (when an error doesn't list here)

```bash
# 1. Verbose log from one mount session
gmountie mount -v -c client.yaml 2> /tmp/gmountie-mount.log

# 2. Reachability from the client
nc -zv <server-host> 9449

# 3. Server health endpoint
curl -fsS http://<server-host>:9090/healthz

# 4. Server-side log for the same request id
sudo journalctl -u gmountie --since '10 min ago' | grep <request-id>
```

If you've worked through this catalog and the matching [troubleshooting](./troubleshooting.mdx) entry and the mount still fails, [open an issue](https://github.com/gMountie/gMountie/issues) with the four artefacts above attached.

## See also

- [Troubleshooting](./troubleshooting.mdx) — symptom-first version of this catalog.
- [Sessions & reconnect](./concepts/sessions-and-reconnect.mdx) — how the keepalive + retry layer surfaces transient failures.
- [Identity & ownership](./concepts/identity.mdx) — for the whole `Permission denied` family.
