# CLI/Config UX overhaul — release notes

**Behavior changes (no backwards-compatibility shims):**

- `gmountie serve` first run now **binds 0.0.0.0** (was 127.0.0.1), ships a
  working **`shared`** volume at `$XDG_DATA_HOME/gmountie/shared`, and generates
  a **random admin password printed once** (was the fixed `admin`). Rotate with
  `gmountie genpass`.
- The server now **validates volume paths at startup** and refuses to start if a
  configured path is missing or not a directory (was: failed lazily at first I/O).
- `gmountie mount` accepts the shorthand **`[user@]host[:port]/volume mountpoint`**
  and resolves the password from `--password`, then the config file, then
  `$GMOUNTIE_AUTH_PASSWORD`, then an interactive prompt. `--daemon` mounts in the
  background.

**New commands:** `gmountie ls`, `gmountie config show`.
