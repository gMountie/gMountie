# Security Policy

gMountie is a network filesystem: a server exposes directories over gRPC, and clients mount them over the public internet. Security of the transport, the auth layer, and the volume/identity boundary is core to the project, and we take reports seriously.

> [!NOTE]
> gMountie is **alpha** software. Treat it accordingly: run it against data you can afford to expose, keep it current, and don't rely on it as your only safeguard yet.

## Supported versions

As an alpha project, only the **latest release** and the current `master` receive security fixes. There are no backports to older tags.

| Version | Supported |
| ------- | --------- |
| latest release / `master` | ✅ |
| older releases | ❌ |

## Reporting a vulnerability

**Please do not open a public issue, PR, or Discussion for security problems.**

Report privately via GitHub's **[Report a vulnerability](https://github.com/gMountie/gMountie/security/advisories/new)** form (Security → Advisories). This opens a private channel between you and the maintainers.

Please include, as far as you can:

- A description of the issue and its impact (what an attacker gains).
- The component: **server** (`gmountie serve`), **client** (`gmountie mount`), the wire protocol, or the auth/identity layer.
- Steps to reproduce or a proof of concept.
- Affected version (`gmountie version`) and platform (Linux / macOS).
- Any relevant config (with secrets — passwords, `password_hash`, keys — **redacted**).

### What to expect

- **Acknowledgement:** best-effort within a few days.
- **Assessment & fix:** we'll confirm the issue, agree on severity, and work a fix on the private advisory.
- **Disclosure:** coordinated. We aim to release a fix before public disclosure and will credit you (unless you prefer to remain anonymous). Please allow a reasonable window — up to **90 days** — before disclosing publicly.

## Scope

**In scope** — anything that breaks a security guarantee gMountie intends to provide, for example:

- Authentication or session bypass (basic-auth or mTLS), credential handling, argon2id verification.
- Crossing the volume/identity boundary: reading or writing outside a volume's configured path, escaping path confinement, or bypassing per-user volume ACLs / capability checks.
- TLS/transport weaknesses, TOFU-pinning bypass, or downgrade.
- Remotely triggerable crashes, memory corruption, or resource-exhaustion DoS in the server or client.

**Generally out of scope:**

- Misconfiguration of a self-hosted deployment (e.g. exposing the server with `auth: none`-style setups, weak passwords, or `server.tls.disabled: true` on a public interface).
- The known alpha limitations and roadmap items already documented in [`docs/roadmap.md`](docs/roadmap.md) and [`docs/design/security-and-transport.md`](docs/design/security-and-transport.md) (e.g. OIDC/JWT auth not yet shipped).
- Findings that require a host already compromised at a higher privilege than gMountie runs with.

If you're unsure whether something is in scope, report it anyway — we'd rather hear about it.
