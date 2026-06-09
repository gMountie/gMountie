# Pinned to a digest for reproducible builds. alpine:3.20 multi-arch index
# (linux/amd64 + linux/arm64). Bump manually until Phase 5 wires Dependabot.
FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc AS server

ARG TARGETPLATFORM

# Run unprivileged. uid 1000 matches the Helm chart's securityContext.runAsUser
# and the compose `user:` mapping, so the same data dir ownership works
# everywhere. goreleaser supplies the prebuilt static binary, so there is no
# build stage to add.
RUN adduser -D -u 1000 gmountie

COPY $TARGETPLATFORM/gmountie /opt/gmountie/gmountie

USER gmountie

# Liveness for `docker run` / compose. Hits the ops HTTP server on its default
# loopback bind. NOTE: 9090 is hardcoded; a deploy that overrides
# server.ops.addr makes this stale (k8s probes read the port from the
# chart's values, so they stay correct).
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -q -O- http://127.0.0.1:9090/healthz || exit 1

ENTRYPOINT ["/opt/gmountie/gmountie", "serve"]
